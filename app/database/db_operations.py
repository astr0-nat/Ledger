import os
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Set

import pytz
from sqlalchemy import and_
from sqlalchemy.orm import Session, selectinload
from sqlalchemy.orm import joinedload

from app.database.models import Cost, House, CostItem, Location, UtilityFee
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Retrieve QB_ACCOUNTS and split into a list
qb_accounts = os.getenv('QB_ACCOUNTS', '')
qb_accounts = [account.strip() for account in qb_accounts.split(',') if account.strip()]

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_cost_items_to_sync(db: Session) -> Dict[str, Set[CostItem]]:
    """
    Retrieve cost items that need to be synchronized, grouped by QuickBooks accounts.

    Args:
        db (Session): SQLAlchemy database session.

    Returns:
        Dict[str, Set[CostItem]]: Dictionary mapping qb_account to set of CostItem objects.
    """
    cost_items_to_sync: Dict[str, Set[CostItem]] = defaultdict(set)

    try:

        two_weeks_ago = datetime.utcnow() - timedelta(weeks=2)

        # Set the launch date to October 10, 2024, at 9 AM PDT
        california_tz = pytz.timezone('America/Los_Angeles')
        launch_date = california_tz.localize(datetime(2024, 10, 10, 9, 0, 0)).astimezone(pytz.UTC)

        cost_items = (
            db.query(CostItem)
            .join(CostItem.cost)
            .join(CostItem.house)
            .options(
                joinedload(CostItem.cost).joinedload(Cost.cleaner),  # Eager load Cost and Cleaner
                joinedload(CostItem.house),  # Eager load House
                joinedload(CostItem.financial_account),  # Eager load FinancialAccount
                joinedload(CostItem.house).joinedload(House.location).joinedload(Location.translations), # Eager load House -> Location -> Translations,
                selectinload(CostItem.cost_sub_items)  # Eager load CostSubItems (if it's a collection)
        )
            .filter(
                and_(
                    Cost.status.in_(['approved', 'paid']),
                    Cost.category == 'purchase_order',
                    House.qb_account.in_(qb_accounts),
                    CostItem.qbid.is_(None),  # qbid is not yet set
                    Cost.updated_at >= two_weeks_ago,  # Updated in last two weeks
                    Cost.created_at >= launch_date  # Created on or after launch date

                )
            )
            .distinct()
            .all()
        )

        logger.info(f"Retrieved {len(cost_items)} CostItems to sync.")

        for cost_item in cost_items:
            qb_account = cost_item.house.qb_account
            cost_items_to_sync[qb_account].add(cost_item)

    except Exception as e:
        logger.error(f"Error fetching costs to sync: {e}")

    return cost_items_to_sync


def get_utility_fees_to_sync(db: Session):
    """
    Retrieve UtilityFee records that have:
    - An associated house with a QuickBooks account (`qb_account` is present).
    - 'qb_payment_method' in the `extra` field is present and not 'Homeowner pay'.
    - Have not been synced yet (i.e., `qbid` is null) or need re-sync based on `last_sync_time`.
    """
    utility_fees_to_sync: Dict[str, Set[UtilityFee]] = defaultdict(set)

    try:
        # Set the launch date to October 23, 2024, at 6 PM PDT
        california_tz = pytz.timezone('America/Los_Angeles')
        launch_date = california_tz.localize(datetime(2024, 10, 23, 18, 0, 0)).astimezone(pytz.UTC)
        two_weeks_ago = datetime.utcnow() - timedelta(weeks=2)

        child_company_utility_fees = (
            db.query(UtilityFee)
            .join(House)
            .filter(
                and_(
                    House.qb_account.in_(qb_accounts)),
                    UtilityFee.created_at >= launch_date,
                    UtilityFee.updated_at >= two_weeks_ago
            )
            .all()
        )

        qb_syncable_utility_fees = [
            fee for fee in child_company_utility_fees
            if (fee.qb_bill_datetime is not None and
                fee.qb_payment_method != 'Homeowner pay' and
                fee.qb_id is None)
        ]

        for fee in qb_syncable_utility_fees:
            qb_account = fee.house.qb_account
            utility_fees_to_sync[qb_account].add(fee)

        logger.info(f"Retrieved {len(qb_syncable_utility_fees)} Utility Fees to sync.")

    except Exception as e:
        logger.error(f"Error fetching costs to sync: {e}")

    return utility_fees_to_sync
