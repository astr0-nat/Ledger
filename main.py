import atexit
import base64
import json
import logging.config
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Union, Optional

import requests
import sys
import time
from datetime import datetime, timedelta

import pytz
import redis
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Form, Request
from fastapi.responses import JSONResponse
from quickbooks.client import QuickBooks
from quickbooks.exceptions import QuickbooksException
from quickbooks.objects.vendor import Vendor
from quickbooks.objects.department import Department
from quickbooks.objects.account import Account
from quickbooks.objects.bill import Bill
from quickbooks.objects.detailline import DetailLine, AccountBasedExpenseLineDetail, AccountBasedExpenseLine
from sqlalchemy.orm import Session

from app.database.connection import get_db
from app.database.db_operations import get_cost_items_to_sync, get_utility_fees_to_sync
from logging_config import LOGGING_CONFIG

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

######################################
# LOGGING SETUP
######################################
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

######################################
# ENV & CONSTANTS
######################################
load_dotenv()
california_tz = pytz.timezone('America/Los_Angeles')
redis_client = redis.Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))

TOKEN_URL = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = 'https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl'

REALM_IDS = {
    'month2month_texas': os.getenv('REALM_ID_TEXAS'),
    'month2month_portland': os.getenv('REALM_ID_PORTLAND'),
    'month2month_georgia': os.getenv('REALM_ID_GEORGIA')
}

######################################
# APP & SCHEDULER
######################################
app = FastAPI()
scheduler = BackgroundScheduler()


######################################
# Error Reporting Service
######################################

class ErrorNotificationService:
    """
    Service to handle error tracking and email notifications for QuickBooks sync jobs.
    """

    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.recipient = "qbsyncerrors@month2month.com"
        self.smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_password = os.getenv("SMTP_PASSWORD")

        # In-memory storage for new errors during this job run
        self.new_errors_by_account: Dict[str, List[Dict]] = {}

    def _generate_error_key(self, error_type: str, error_id: Union[int, str], error_message: str) -> str:
        """
        Generate a unique key for storing error information in Redis.

        Args:
            error_type: Either 'cost_item' or 'utility_fee'
            error_id: The ID of the failed entity
            error_message: The error message

        Returns:
            str: A unique Redis key
        """
        # Create a deterministic key that will be the same for identical errors
        return f"qb_sync_error:{error_type}:{error_id}:{hash(error_message)}"

    def check_and_collect_error(self,
                                error_type: str,
                                qb_account: str,
                                entity_id: Union[int, str],
                                error_message: str,
                                context: Optional[Dict] = None) -> None:
        """
        Check if error encountered in last 48 hours, if not collect it for reporting.

        Args:
            error_type: Either 'cost_item' or 'utility_fee'
            qb_account: The QuickBooks account where the error occurred
            entity_id: The ID of the failed entity
            error_message: The error message
            context: Additional error context
        """
        try:
            error_key = self._generate_error_key(error_type, entity_id, error_message)

            if self.redis_client.exists(error_key):
                logger.debug(f"Error already reported in last 48 hours: {error_key}")
                return

            error_data = {
                'error_type': error_type,
                'qb_account': qb_account,
                'entity_id': entity_id,
                'error_message': error_message,
                'context': context or {},
                'timestamp': datetime.utcnow().isoformat(),
                'redis_key': error_key
            }

            # Add to in-memory collection
            if qb_account not in self.new_errors_by_account:
                self.new_errors_by_account[qb_account] = []

            self.new_errors_by_account[qb_account].append(error_data)
            logger.debug(f"Collected new error for reporting: {error_key}")

        except Exception as e:
            raise ErrorNotificationServiceError(f"Failed to check/collect error: {str(e)}") from e

    def _store_errors_as_reported(self) -> None:
        """
        Store all collected errors in Redis with 48-hour expiration.
        """
        try:
            for account_errors in self.new_errors_by_account.values():
                for error in account_errors:
                    # Store error with 48-hour expiration
                    self.redis_client.setex(
                        error['redis_key'],
                        timedelta(hours=48),
                        json.dumps(error)
                    )

            # Clear in-memory collection after storing in Redis
            self.new_errors_by_account = {}

        except Exception as e:
            raise ErrorNotificationServiceError(f"Failed to store errors in Redis: {str(e)}") from e

    def _format_email_body(self) -> str:
        """
        Format the error information into an HTML email body.

        Returns:
            str: Formatted HTML email body
        """
        email_body = ["<html><body>", "<h2>QuickBooks Sync Error Report</h2>",
                      "<p>New errors encountered in the last sync job:</p>"]

        for qb_account, errors in self.new_errors_by_account.items():
            email_body.append(f"<h3>Account: {qb_account}</h3>")

            # Group by error type
            cost_errors = [e for e in errors if e['error_type'] == 'cost_item']
            utility_errors = [e for e in errors if e['error_type'] == 'utility_fee']

            if cost_errors:
                email_body.append("<h4>Cost Item Errors:</h4>")
                email_body.append("<ul>")
                for error in cost_errors:
                    email_body.append(
                        f"<li>Cost Item #{error['entity_id']}: {error['error_message']}"
                        f"<br>Context: {json.dumps(error['context'], indent=2)}</li>"
                    )
                email_body.append("</ul>")

            if utility_errors:
                email_body.append("<h4>Utility Fee Errors:</h4>")
                email_body.append("<ul>")
                for error in utility_errors:
                    email_body.append(
                        f"<li>Utility Fee #{error['entity_id']}: {error['error_message']}"
                        f"<br>Context: {json.dumps(error['context'], indent=2)}</li>"
                    )
                email_body.append("</ul>")

        email_body.append("</body></html>")
        return "\n".join(email_body)

    def send_error_digest(self) -> None:
        """
        Send an email digest of all new errors and store them in Redis as reported.
        """
        try:
            if not any(self.new_errors_by_account.values()):
                logger.info("No new errors to report")
                return

            # Create email
            msg = MIMEMultipart('alternative')
            msg['Subject'] = 'QuickBooks Sync Error Report'
            msg['From'] = self.smtp_username
            msg['To'] = self.recipient

            html_body = self._format_email_body()
            msg.attach(MIMEText(html_body, 'html'))

            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)

            logger.info("Error digest email sent successfully")

        except Exception as e:
            raise ErrorNotificationServiceError(f"Failed to create and send error digest: {str(e)}.") from e

        try:
            # After successful email send, store errors in Redis
            self._store_errors_as_reported()

        except Exception as e:
            raise ErrorNotificationServiceError(f"Failed to store reported errors: {str(e)}. New Errors: {json.dumps(self.new_errors_by_account, indent=2)}") from e


######################################
# Custom Exceptions
######################################

class ErrorNotificationServiceError(Exception):
    """Raised when error reporting operations fail."""

    def __init__(self, message, context=None):
        self.context = context or {}
        super().__init__(message)


class QuickBooksServiceError(Exception):
    """Base exception for QuickBooks service errors."""

    def __init__(self, message, context=None):
        self.context = context or {}
        super().__init__(message)


class QuickBooksAuthError(QuickBooksServiceError):
    """Raised when authentication or token-related operations fail."""
    pass


class QuickBooksClientError(QuickBooksServiceError):
    """Raised when client creation or initialization fails."""
    pass


class QuickBooksEntityError(QuickBooksServiceError):
    """Raised when operations on QuickBooks entities (vendor, department, account) fail."""
    pass


class QuickBooksSyncError(QuickBooksServiceError):
    """Raised when bill synchronization fails."""
    pass


class UtilityFeeError(Exception):
    """Base exception for utility fee related errors."""

    def __init__(self, message, utility_fee_id=None, context=None):
        self.utility_fee_id = utility_fee_id
        self.context = context or {}
        super().__init__(message)


class UtilityFeeBuildError(UtilityFeeError):
    """Raised when building utility fee data fails."""
    pass


class UtilityFeeConversionError(UtilityFeeError):
    """Raised when converting utility fee to bill fails."""
    pass


class UtilityFeeSyncError(UtilityFeeError):
    """Raised when syncing utility fees fails."""
    pass


class CostServiceError(Exception):
    """Base exception for Cost service errors."""

    def __init__(self, message, cost_item_id=None, po_id=None, context=None):
        self.cost_item_id = cost_item_id
        self.po_id = po_id
        self.context = context or {}
        super().__init__(message)


class CostConversionError(CostServiceError):
    """Raised when converting cost items to bills fails."""
    pass


class DocumentNumberError(CostServiceError):
    """Raised when generating document numbers fails."""
    pass


######################################
# AUTH CLIENT / QUICKBOOKS SERVICE
######################################
class AuthClient:
    """
    Handles all aspects of token management for a single QuickBooks realm.
    """

    def __init__(self, client_id, client_secret, environment, redis_client, qb_account):
        self.client_id = client_id
        self.client_secret = client_secret
        self.environment = environment
        self.redis_client = redis_client
        self.qb_account = qb_account
        self._access_token = None
        self._refresh_token = None
        self._token_expiry = 0
        self._load_tokens()

    def _load_tokens(self):
        try:
            self._access_token = self.redis_client.get(f'access_token:{self.qb_account}')
            self._refresh_token = self.redis_client.get(f'refresh_token:{self.qb_account}')
            expiry = self.redis_client.get(f'token_expiry:{self.qb_account}')
            self._token_expiry = int(expiry) if expiry else 0

            if self._access_token:
                self._access_token = self._access_token.decode()
            if self._refresh_token:
                self._refresh_token = self._refresh_token.decode()
        except Exception as e:
            logger.error(f"Failed to load tokens for {self.qb_account}: {str(e)}")

    @property
    def access_token(self):
        if time.time() > self._token_expiry:
            self.refresh()
        return self._access_token

    @property
    def refresh_token(self):
        return self._refresh_token

    def refresh(self):
        if time.time() <= self._token_expiry:
            logger.debug(f"Token for {self.qb_account} is still valid. Skipping refresh.")
            return

        logger.debug(f"Refreshing token for {self.qb_account}")
        try:
            basic_auth = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
            headers = {
                'Authorization': f'Basic {basic_auth}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'grant_type': 'refresh_token',
                'refresh_token': self._refresh_token
            }

            response = requests.post(TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()
            tokens = response.json()

            # Update tokens and expiry
            self._access_token = tokens['access_token']
            self._refresh_token = tokens.get('refresh_token', self._refresh_token)
            self._token_expiry = time.time() + tokens['expires_in'] - 300  # Subtract 5 minutes

            # Store new tokens in Redis
            self.redis_client.set(f'access_token:{self.qb_account}', self._access_token)
            self.redis_client.set(f'refresh_token:{self.qb_account}', self._refresh_token)
            self.redis_client.set(f'token_expiry:{self.qb_account}', int(self._token_expiry))

            logger.info(f"Token refreshed for {self.qb_account}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error refreshing token: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response content: {e.response.text}")
        except Exception as e:
            logger.error(f"Unexpected error during token refresh: {str(e)}")


class QuickBooksService:
    """
    Service to handle QuickBooks operations, such as retrieving a client,
    creating or fetching vendors, departments, accounts, and building Bills.
    """

    @staticmethod
    def get_quickbooks_client(qb_account):
        logger.debug(f"Creating QuickBooks client for {qb_account}")
        try:
            company_id = REALM_IDS[qb_account]
            auth_client = AuthClient(
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                environment='production',
                redis_client=redis_client,
                qb_account=qb_account
            )
            qb_client = QuickBooks(
                auth_client=auth_client,
                company_id=company_id
            )
            return qb_client
        except requests.exceptions.RequestException as e:
            raise QuickBooksClientError(
                f"Error creating QuickBooks client: {getattr(e.response, 'text', None)}",
                {"qb_account": qb_account, 'error_type': type(e).__name__}
            ) from e
        except KeyError as e:
            raise QuickBooksClientError(
                f"Invalid QuickBooks account: {str(e)}",
                {"qb_account": qb_account, 'error_type': type(e).__name__}
            ) from e
        except Exception as e:
            raise QuickBooksClientError(
                f"Error creating QuickBooks client: {str(e)}",
                {"qb_account": qb_account, 'error_type': type(e).__name__}
            ) from e

    @staticmethod
    def exchange_code_for_tokens(auth_code, qb_account):
        """
        Exchange auth code from QuickBooks for tokens and store them in Redis.
        """
        logger.debug(f"Exchanging code for tokens for {qb_account}")
        try:
            basic_auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
            headers = {
                'Authorization': f'Basic {basic_auth}',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'grant_type': 'authorization_code',
                'code': auth_code,
                'redirect_uri': REDIRECT_URI
            }
            response = requests.post(TOKEN_URL, headers=headers, data=data)
            response.raise_for_status()
            tokens = response.json()

            # Store tokens in Redis
            redis_client.set(f'access_token:{qb_account}', tokens['access_token'])
            redis_client.set(f'refresh_token:{qb_account}', tokens['refresh_token'])

            # Calculate and store token expiry time
            expires_in = tokens.get('expires_in', 3600)
            expiry_time = int(time.time()) + expires_in - 300
            redis_client.set(f'token_expiry:{qb_account}', expiry_time)

            return tokens
        except requests.exceptions.RequestException as e:
            raise QuickBooksAuthError(
                f"Failed to exchange authorization code for tokens: {getattr(e.response, 'text', None)}. ",
                {'error_type': type(e).__name__}
            ) from e
        except Exception as e:
            raise QuickBooksAuthError(
                f"Unexpected error during token exchange: {str(e)} ",
                {'error_type': type(e).__name__}
            ) from e

    @staticmethod
    def get_or_create_vendor(vendor_name, qb_client):
        """
        Fetch vendor from QBO or create if not found.
        """
        logger.debug(f"Getting or creating vendor: {vendor_name}")
        try:
            vendors = Vendor.filter(DisplayName=vendor_name, qb=qb_client)
            if vendors:
                vendor = vendors[0]
                logger.debug(f"Vendor found: {vendor_name} with ID {vendor.Id}")
                return vendor
            else:
                new_vendor = Vendor()
                new_vendor.DisplayName = vendor_name
                new_vendor.Active = True
                new_vendor.save(qb=qb_client)
                logger.info(f"Vendor created: {vendor_name} with ID {new_vendor.Id}")
                return new_vendor

        except requests.exceptions.RequestException as e:
            raise QuickBooksEntityError(
                f"Failed to get/create vendor: {getattr(e.response, 'text', None)}",
                {"vendor_name": vendor_name, 'error_type': type(e).__name__}
            ) from e
        except QuickbooksException as e:
            raise QuickBooksEntityError(
                f"Failed to get/create vendor: {str(e)}. ",
                {"vendor_name": vendor_name, 'error_type': type(e).__name__}
            ) from e
        except Exception as e:
            raise QuickBooksEntityError(
                f"Unexpected error while getting/creating vendor: {str(e)}",
                {"vendor_name": vendor_name, 'error_type': type(e).__name__}
            ) from e

    @staticmethod
    def get_or_create_department_ref(house, qb_client):
        """
        QBO 'Department' is akin to 'Class' in QB. We'll use the House to name that class.
        """
        department_name = house.qb_name
        logger.debug(f"Attempting to find/create department '{department_name}' for house {house.id}")
        try:
            departments = Department.filter(Name=department_name, qb=qb_client)
            if departments:
                department = departments[0]
                logger.debug(f"Class (DepartmentRef) found: ID = {department.Id}, Name = {department.Name}")
                return department.to_ref()
            else:
                new_department = Department()
                new_department.Name = department_name
                new_department.Active = True
                created_department = new_department.save(qb=qb_client)
                logger.debug(
                    f"Class (DepartmentRef) created: ID = {created_department.Id}, Name = {created_department.Name}")
                return created_department.to_ref()

        except requests.exceptions.RequestException as e:
            raise QuickBooksEntityError(
                f"Failed to get/create department '{department_name}",
                {"error_response": getattr(e.response, 'text', None)}
            ) from e

        except QuickbooksException as e:
            raise QuickBooksEntityError(
                f"Failed to get/create department '{department_name}'",
                {"department_name": department_name}
            ) from e
        except Exception as e:
            raise QuickBooksEntityError(
                f"Unexpected error while getting/creating department '{department_name}'",
                {"department_name": department_name}
            ) from e

    @staticmethod
    def get_account_ref(account_name, qb_client):
        """
        Retrieve Account from QBO by name.
        """
        logger.debug(f"Getting account ref: {account_name}")
        try:
            accounts = Account.filter(Name=account_name, qb=qb_client)
            if accounts:
                account = accounts[0]
                logger.debug(f"Account found: ID = {account.Id}, Name = {account.Name}")
                return account.to_ref()
            else:
                raise QuickBooksEntityError(
                    f"Account does not exist in QBO. ",
                    {"account_name": account_name}
                )
        except requests.exceptions.RequestException as e:
            raise QuickBooksEntityError(
                f"Error retrieving Account: {getattr(e.response, 'text', None)} ",
                {"account_name": account_name, 'error_type': type(e).__name__}
            ) from e

        except QuickbooksException as e:
            raise QuickBooksEntityError(
                f"Error retrieving Account: {str(e)} ",
                {"account_name": account_name, 'error_type': type(e).__name__}
            ) from e
        except Exception as e:
            raise QuickBooksEntityError(
                f"Unexpected error retrieving Account: {str(e)} ",
                {"account_name": account_name, 'error_type': type(e).__name__}
            ) from e


######################################
# COST SERVICE
######################################
class CostService:
    """
    Handles all logic related to reading Costs from the database,
    converting them to QuickBooks Bills, and saving them to QBO.
    """

    @staticmethod
    def generate_qb_doc_num(cost_item):
        """
        Build a doc number from the cost_item, limit length if needed.
        """
        try:
            parent_cost = cost_item.cost
            house_id = cost_item.house.id
            full_qb_doc_num = f"{house_id}C#{parent_cost.id}#{cost_item.id}"
            short_qb_doc_num = f"C#{parent_cost.id}#{cost_item.id}"
            max_len = int(os.getenv("MAX_QB_DOC_NUM_LEN", 50))
            return short_qb_doc_num if len(full_qb_doc_num) > max_len else full_qb_doc_num
        except AttributeError as e:
            raise DocumentNumberError(
                f"Failed to generate document number - missing required attributes",
                cost_item_id=getattr(cost_item, 'id', None),
                po_id=getattr(getattr(cost_item, 'cost', None), 'id', None)
            ) from e
        except Exception as e:
            raise DocumentNumberError(
                f"Unexpected error generating document number",
                cost_item_id=getattr(cost_item, 'id', None),
                po_id=getattr(getattr(cost_item, 'cost', None), 'id', None)
            ) from e

    @staticmethod
    def convert_cost_item_to_qb_bill(cost_item, qb_client):
        """
        Convert a single cost_item into a QBO Bill object, using the QuickBooksService for lookups.
        Raise exceptions if anything fails, so the error is clear and we don't continue.
        """
        try:
            po = cost_item.cost

            # 1) Doc Number
            qb_doc_num = CostService.generate_qb_doc_num(cost_item)

            # 2) Vendor
            vendor_name = po.cleaner.full_name
            vendor = QuickBooksService.get_or_create_vendor(vendor_name, qb_client)

            # 3) Bill base info

            bill = Bill()
            bill.VendorRef = vendor.to_ref()
            bill.PrivateNote = "Responsibility Party: Holidale"
            bill.DueDate = po.due_date.strftime('%Y-%m-%d') if po.due_date else None
            bill.TxnDate = po.bill_date.strftime('%Y-%m-%d') if po.bill_date else None
            bill.DocNumber = qb_doc_num

            # 4) Department/class
            department_ref = QuickBooksService.get_or_create_department_ref(cost_item.house, qb_client)
            bill.DepartmentRef = department_ref

            # 5) Account
            financial_account = cost_item.financial_account
            if not financial_account:
                raise CostConversionError(
                    "No financial account assigned to cost item",
                    cost_item_id=cost_item.id,
                    po_id=po.id
                )
            account_ref = QuickBooksService.get_account_ref(financial_account.name, qb_client)

            # Create main line for the cost_item
            line = DetailLine()
            line.DetailType = "AccountBasedExpenseLineDetail"
            line.Amount = float(cost_item.amount)
            line.Description = cost_item.description
            line.AccountBasedExpenseLineDetail = AccountBasedExpenseLineDetail()
            line.AccountBasedExpenseLineDetail.AccountRef = account_ref

            line_items = [line]

            # Create subline items if they exist
            if cost_item.cost_sub_items:
                for csi in cost_item.cost_sub_items:
                    sub_line = DetailLine()
                    sub_line.DetailType = "AccountBasedExpenseLineDetail"
                    sub_line.Amount = float(csi.amount)
                    sub_line.Description = csi.description
                    sub_line.AccountBasedExpenseLineDetail = AccountBasedExpenseLineDetail()
                    sub_line.AccountBasedExpenseLineDetail.AccountRef = account_ref
                    line_items.append(sub_line)

            bill.Line = line_items

            logger.debug(f"Successfully converted cost item {cost_item.id} to QuickBooks bill")
            return bill

        except (DocumentNumberError, QuickBooksEntityError) as e:
            error_context = {
                **getattr(e, 'context', {})
            }
            raise CostConversionError(
                f"Failed to convert cost item to bill: {str(e)}",
                cost_item_id=cost_item.id,
                po_id=po.id,
                context=error_context
            ) from e
        except Exception as e:
            error_context = {
                'cost_item_id': cost_item.id,
                'po_id': po.id,
                'error_type': type(e).__name__,
                'original_error': str(e),
                **getattr(e, 'context', {})
            }
            raise CostConversionError(
                f"Unexpected error converting cost item to bill: {str(e)}",
                cost_item_id=cost_item.id,
                po_id=po.id,
                context=error_context
            ) from e

    @staticmethod
    def sync_all_quickbooks_costs(error_notification_service):
        """
        High-level function to read all new cost_items from DB, group them by qb_account,
        and sync to QuickBooks as Bills.
        """
        db: Session = None
        try:
            db = next(get_db())
            costs_by_account = get_cost_items_to_sync(db)
            results = {}
            failed_items_by_account = {}

            for qb_account, items in costs_by_account.items():
                if not items:
                    logger.debug(f"No cost items found for account {qb_account}")
                    results[qb_account] = None
                    continue

                try:
                    qb_client = QuickBooksService.get_quickbooks_client(qb_account)
                    if not qb_client:
                        raise QuickBooksClientError(f"QB Client is empty")

                    account_success = True
                    failed_items = []

                    for cost_item in items:
                        try:
                            bill = CostService.convert_cost_item_to_qb_bill(cost_item, qb_client)
                            result = bill.save(qb=qb_client)

                            cost_item.qbid = result.Id
                            db.commit()

                            logger.info(
                                f"[CostItem Synced] cost_item_id={cost_item.id}, PO={cost_item.cost_id}, qb_id={result.Id}"
                            )

                        except (CostConversionError, QuickbooksException) as e:
                            account_success = False
                            error_details = {
                                'cost_item_id': cost_item.id,
                                'po_id': cost_item.cost_id,
                                'qb_account': qb_account,
                                'error': str(e),
                                'context': {
                                    **getattr(e, 'context', {}),
                                }
                            }
                            failed_items.append(error_details)

                            error_message = (f"Failed to sync cost item. "
                                             f"Error details: {error_details}"
                                             )

                            error_notification_service.check_and_collect_error(
                                error_type='cost_item',
                                qb_account=qb_account,
                                entity_id=cost_item.id,
                                error_message=str(error_message),
                            )

                            logger.error(error_message)
                            continue

                    results[qb_account] = account_success
                    if failed_items:
                        failed_items_by_account[qb_account] = failed_items
                except QuickBooksClientError as e:
                    error_details = {
                        'error_message': str(e),
                        'qb_account': qb_account,
                        'context': {
                            **getattr(e, 'context', {})
                        }
                    }

                    error_message = (f"Unable to establish QBO client connection."
                                     f"Error details: {error_details}")

                    error_notification_service.check_and_collect_error(
                        error_type='cost_item',
                        qb_account=qb_account,
                        entity_id=cost_item.id,
                        error_message=str(error_message)
                    )

                    logger.error(error_message)

            # Send error digest after processing all accounts
            # This will also store any new errors in Redis
            try:
                error_notification_service.send_error_digest()
            except Exception as e:
                logger.error(f"Failed to send cost item error digest for {qb_account}: {str(e)}")

            # Summaries
            if all(result is True for result in results.values() if result is not None):
                logger.info("QuickBooks cost item sync successful for all accounts!")
                return True
            elif any(result is True for result in results.values() if result is not None):
                logger.warning(
                    "QuickBooks cost sync partially successful",
                    extra={'failed_accounts': failed_items_by_account}
                )
                return False
            else:
                logger.error(
                    f"QuickBooks cost sync failed for all accounts: {json.dumps(failed_items_by_account, indent=2)}")
                return False

        except Exception as e:
            logger.error(
                f"Unexpected error in sync_all_quickbooks_costs: {str(e)} ")
            return False
        finally:
            if db:
                db.close()


######################################
# UTILITY FEE SERVICE
######################################
class UtilityFeeService:
    """
    Handles logic related to reading UtilityFees from DB, building QBO Bill objects,
    and syncing them to QuickBooks.
    """

    @staticmethod
    def build_utility_fee_data(utility_fee):
        """
        Gather QBO-friendly data from a UtilityFee object.
        """
        logger.debug(f"Building utility fee data for UtilityFee#{utility_fee.id}")

        try:
            if not utility_fee.doc_number:
                raise UtilityFeeBuildError(
                    "Missing document number",
                    utility_fee_id=utility_fee.id
                )

            if not utility_fee.qb_vendor:
                raise UtilityFeeBuildError(
                    "Missing vendor information",
                    utility_fee_id=utility_fee.id
                )

            if not utility_fee.account_name:
                raise UtilityFeeBuildError(
                    "Missing account name",
                    utility_fee_id=utility_fee.id
                )

            if not utility_fee.amount:
                raise UtilityFeeBuildError(
                    "Missing amount",
                    utility_fee_id=utility_fee.id
                )

            if not utility_fee.house.qb_name:
                raise UtilityFeeBuildError(
                    "Missing location",
                    utility_fee_id=utility_fee.id
                )

            data = {
                'bill_no': utility_fee.doc_number,
                'vendor': utility_fee.qb_vendor,
                'bill_date': utility_fee.qb_bill_datetime,
                'due_date': utility_fee.qb_bill_datetime,
                'description': utility_fee.description,
                'memo': utility_fee.description,
                'account_name': utility_fee.account_name,
                'amount': utility_fee.qb_amount,
                'location': utility_fee.house.qb_name,
            }
            return data
        except AttributeError as e:
            raise UtilityFeeBuildError(
                f"Missing required utility fee attribute: {str(e)}.",
                utility_fee_id=getattr(utility_fee, 'id', None)
            ) from e
        except Exception as e:
            raise UtilityFeeBuildError(
                f"Unexpected error building utility fee data: {str(e)}.",
                utility_fee_id=getattr(utility_fee, 'id', None)
            ) from e

    @staticmethod
    def convert_utility_fee_to_bill(utility_fee):
        """
        Convert a single UtilityFee to a QBO Bill using QuickBooksService for lookups.
        """
        try:
            qb_account = utility_fee.house.qb_account
            qb_client = QuickBooksService.get_quickbooks_client(qb_account)

            data = UtilityFeeService.build_utility_fee_data(utility_fee)

            # Get or create vendor
            vendor = QuickBooksService.get_or_create_vendor(
                data['vendor'],
                qb_client
            )

            # Get account reference
            # Some UtilityFees store a different qb_account name in 'qb_company_account'; if so, retrieve it:
            # fallback to the normal qb_account on the House
            account_ref = QuickBooksService.get_account_ref(
                data['account_name'],
                qb_client
            )

            # Build the Bill
            bill = Bill()
            bill.VendorRef = vendor.to_ref()
            bill.DocNumber = data['bill_no']
            bill.TxnDate = data['bill_date']
            bill.DueDate = data['due_date']
            bill.PrivateNote = data['memo']

            # Get department reference
            department_ref = QuickBooksService.get_or_create_department_ref(
                utility_fee.house,
                qb_client
            )
            if department_ref:
                bill.DepartmentRef = department_ref

            # Create line item
            line = AccountBasedExpenseLine()
            line.Amount = float(data['amount'])
            line.DetailType = "AccountBasedExpenseLineDetail"
            line.Description = data['description']
            line.AccountBasedExpenseLineDetail = AccountBasedExpenseLineDetail()
            line.AccountBasedExpenseLineDetail.AccountRef = account_ref

            bill.Line = [line]

            logger.debug(f"Successfully built QBO Bill for UtilityFee#{utility_fee.id}")
            return bill

        except UtilityFeeBuildError as e:
            raise UtilityFeeConversionError(
                f"Failed to build utility fee data: {str(e)}",
                utility_fee_id=utility_fee.id,
                context=e.context
            ) from e

        except QuickBooksClientError as e:
            raise UtilityFeeConversionError(
                f"Failed to initialize QuickBooks client: {str(e)}",
                utility_fee_id=utility_fee.id,
                context=e.context
            ) from e
        except QuickBooksEntityError as e:
            raise UtilityFeeConversionError(
                f"Failed to get/create QuickBooks entity: {str(e)}",
                utility_fee_id=utility_fee.id,
                context=e.context
            ) from e
        except Exception as e:
            raise UtilityFeeConversionError(
                f"Unexpected error converting utility fee to bill",
                utility_fee_id=utility_fee.id
            ) from e

    @staticmethod
    def sync_all_utility_fees(error_notification_service):
        """
        High-level function: read all new UtilityFees from DB, group them by qb_account,
        and push them to QuickBooks.
        """
        db: Session = None

        try:
            db = next(get_db())
            utility_fees_by_account = get_utility_fees_to_sync(db)
            results = {}
            failed_items_by_account = {}

            for qb_account, utility_fees in utility_fees_by_account.items():
                if not utility_fees:
                    logger.debug(f"No utility fees found for account {qb_account}")
                    results[qb_account] = None
                    continue

                try:
                    qb_client = QuickBooksService.get_quickbooks_client(qb_account)
                    if not qb_client:
                        raise QuickBooksClientError(f"Could not create QuickBooks client for account {qb_account}")

                    account_success = True
                    failed_items = []

                    for fee in utility_fees:
                        try:
                            bill = UtilityFeeService.convert_utility_fee_to_bill(fee)
                            result = bill.save(qb=qb_client)

                            fee.qb_id = result.Id
                            db.commit()

                            logger.info(
                                f"[Utility Fee Synced] UtilityFee#{fee.id}. "
                                f"QB ID: {result.Id}, DocNumber: {result.DocNumber}, QB Account: {qb_account}."
                            )

                        except (UtilityFeeConversionError, QuickbooksException) as e:
                            account_success = False
                            error_details = {
                                'utility_fee_id': fee.id,
                                'qb_account': qb_account,
                                'context': {
                                    **getattr(e, 'context', {}),
                                }
                            }

                            error_message = (f"Failed to sync utility fee: {str(e)}.  "
                                             f"Error Details: {error_details}.",
                                             )

                            failed_items.append(error_message)

                            error_notification_service.check_and_collect_error(
                                error_type='utility_fee',
                                qb_account=qb_account,
                                entity_id=fee.id,
                                error_message=str(error_message)
                            )

                            logger.error(error_message)
                            continue

                    results[qb_account] = account_success
                    if failed_items:
                        failed_items_by_account[qb_account] = failed_items
                except QuickBooksClientError as e:
                    results[qb_account] = False
                    error_details = {
                        'utility_fee_id': fee.id,
                        'qb_account': qb_account,
                        'context': {
                            **getattr(e, 'context', {}),
                        }
                    }
                    error_message = (
                        f"Failed to initialize QuickBooks client for account {qb_account}: {str(e)}. "
                        f"Error Details: {error_details}"
                    )
                    error_notification_service.check_and_collect_error(
                        error_type='utility_fee',
                        qb_account=qb_account,
                        entity_id=fee.id,
                        error_message=str(error_message)
                    )
                    logger.error(error_message)

            # Send error digest after processing all accounts
            # This will also store any new errors in Redis
            try:
                error_notification_service.send_error_digest()
            except Exception as e:
                logger.error(f"Failed to send utility fee error digest: {str(e)}")
            # Summaries
            if all(result is True for result in results.values() if result is not None):
                logger.info("QuickBooks utility sync successful for all accounts!")
                return True
            elif any(result is True for result in results.values() if result is not None):
                logger.warning(
                    "QuickBooks utility fee sync partially successful",
                    extra={'failed_accounts': failed_items_by_account}
                )
                return False
            else:
                logger.error(
                    f"QuickBooks utility fee sync failed for all accounts: {json.dumps(failed_items_by_account, indent=2)}")
                return False

        except Exception as e:
            error_message = f"Unexpected error in sync_all_utility_fees: {str(e)}"
            logger.error(error_message)

            # # TODO refactor to allow this system failure messages
            # error_notification_service.check_and_collect_error(
            #     error_type='system',
            #     error_message=str(error_message)
            # )

            return False
        finally:
            if db:
                db.close()


######################################
# SCHEDULED JOBS
######################################
def refresh_tokens_job():
    logger.debug("Running refresh_tokens_job")
    for qb_account in REALM_IDS.keys():
        logger.info(f"Refreshing tokens for account {qb_account}")
        try:
            auth_client = AuthClient(
                client_id=CLIENT_ID,
                client_secret=CLIENT_SECRET,
                environment='production',
                redis_client=redis_client,
                qb_account=qb_account
            )
            auth_client.refresh()
            logger.info(f"Successfully refreshed tokens for {qb_account}")
        except Exception as e:
            logger.error(f"Failed to refresh tokens for {qb_account}: {str(e)}")


def scheduled_sync_costs():
    logger.info("Running scheduled_sync_costs job")
    error_notification_service = ErrorNotificationService(redis_client)
    CostService.sync_all_quickbooks_costs(error_notification_service)


def scheduled_sync_utility_fees():
    logger.info("Running scheduled_sync_utility_fees job")
    error_notification_service = ErrorNotificationService(redis_client)
    UtilityFeeService.sync_all_utility_fees(error_notification_service)


######################################
# ROUTES / ENDPOINTS
######################################
@app.post('/exchange-token')
async def exchange_token_endpoint(auth_code: str = Form(...), qb_account: str = Form(...)):
    logger.debug(f"Received request - auth_code: {auth_code}, qb_account: {qb_account}")
    try:
        tokens = QuickBooksService.exchange_code_for_tokens(auth_code, qb_account)
        logger.debug(f"Tokens exchanged successfully: {tokens}")
        return JSONResponse(
            content={"message": f"Tokens exchanged and stored successfully for {qb_account}.", "tokens": tokens}
        )
    except requests.HTTPError as e:
        error_message = f"HTTP error occurred: {str(e)}"
        logger.error(error_message)
        raise HTTPException(status_code=e.response.status_code, detail=error_message) from e
    except QuickBooksAuthError as e:
        error_message = f"Quickbooks Auth Error occured: {str(e)}. Error details: {getattr(e, 'context', {})}"
        logger.error(error_message)
        raise HTTPException(status_code=500, detail=error_message) from e
    except Exception as e:
        error_message = f"Unexpected error occurred: {str(e)}"
        logger.error(error_message)
        raise HTTPException(status_code=500, detail=error_message) from e

@app.post('/qb-webhook')
async def qb_webhook(request: Request):
    payload = await request.json()
    logger.info("Received QuickBooks webhook with payload: %s", payload)
    return {"status": "OK"}


######################################
# APP STARTUP & SHUTDOWN
######################################
@app.on_event("startup")
def startup_event():
    scheduler.start()
    logger.info("Scheduler started.")


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
    logger.info("Scheduler shut down.")


# Ensure scheduler shuts down if the application exits
atexit.register(lambda: scheduler.shutdown())

######################################
# SCHEDULER JOBS
######################################
scheduler.add_job(
    scheduled_sync_costs,
    trigger=IntervalTrigger(
        hours=1,
        start_date=california_tz.localize(datetime(2024, 10, 10, 9, 0, 0))
    ),
    id='hourly_PO_quickbooks_sync',
    name='Hourly PO QuickBooks Sync',
    replace_existing=True
)

scheduler.add_job(
    scheduled_sync_utility_fees,
    trigger=CronTrigger(
        hour=7,
        minute=30,
        timezone=pytz.timezone('America/Los_Angeles')
    ),
    id='daily_utility_fee_quickbooks_sync',
    name='Daily Utility Fee QuickBooks Sync at 7:30 AM PT',
    replace_existing=True
)

scheduler.add_job(refresh_tokens_job, 'interval', hours=8)

######################################
# MAIN ENTRYPOINT
######################################
if __name__ == "__main__":
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_config=LOGGING_CONFIG,
            log_level='debug',
            access_log=True,
        )
    except Exception as e:
        logger.error(f"Failed to start the server: {str(e)}")
