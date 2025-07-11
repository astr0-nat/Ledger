from decimal import Decimal
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Date, ForeignKey, Text, Numeric
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from app.utils import parse_date, yaml_load, yaml_dump_with_ruby_tags
import logging.config
from logging_config import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)
Base = declarative_base()
QB_ACCOUNT_MAPPING = {
    'internet': "Cable and Internet",
    'water': "Water",
    'gas': "Gas",
    'electric': "Electricity",
    'trash': "Waste Management",
}


class UtilityFee(Base):
    __tablename__ = 'utility_fees'

    id = Column(Integer, primary_key=True)
    house_id = Column(Integer, ForeignKey('houses.id'), nullable=False)
    utility_account_id = Column(Integer)
    utility_type = Column(String)
    amount = Column(Numeric(10, 2))
    start_date = Column(Date)
    end_date = Column(Date)
    note = Column(String)
    refund_file = Column(String)
    extra = Column(Text)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    batch_number = Column(String)
    google_drive_file_name = Column(String)

    house = relationship("House", back_populates="utility_fees")

    @property
    def qb_company_account(self):
        return self.house.qb_account

    @property
    def extra_data(self):
        if not hasattr(self, '_extra_data'):
            if self.extra:
                try:
                    self._extra_data = yaml_load(self.extra) or {}
                except Exception as e:
                    logger.error(f"Error loading YAML: {e}", exc_info=True)
                    self._extra_data = {}
            else:
                self._extra_data = {}
        return self._extra_data

    @extra_data.setter
    def extra_data(self, value):
        self._extra_data = value
        self.extra = yaml_dump_with_ruby_tags(value)

    @property
    def qb_vendor(self):
        return self.extra_data.get('qb_vendor')

    @property
    def account_name(self):
        return QB_ACCOUNT_MAPPING.get(self.utility_type, "Utilities Cost")

    @property
    def description(self):
        return "{vendor}, {start_date} - {end_date}, {house_qb_name} {qb_account_name}. Bill Date: {qb_bill_date}".format(
            vendor=self.qb_vendor,
            start_date=self.start_datetime,
            end_date=self.end_datetime,
            house_qb_name=self.house.qb_name,
            qb_account_name=self.account_name,
            qb_bill_date=self.qb_bill_datetime
        )

    @property
    def doc_number(self):
        return f"UtilityFee#{self.id}"

    @property
    def qb_payment_method(self):
        return self.extra_data.get('qb_payment_method')

    @property
    def qb_amount(self):
        return Decimal(self.extra_data.get('qb_amount')) or self.amount

    @property
    def qb_bill_datetime(self):
        date_str = self.extra_data.get('qb_bill_date')
        if not date_str:
            return None
        return parse_date(self.extra_data.get('qb_bill_date')).strftime('%Y-%m-%d')

    @property
    def start_datetime(self):
        return self.start_date.strftime('%Y-%m-%d')

    @property
    def end_datetime(self):
        return self.end_date.strftime('%Y-%m-%d')

    @property
    def bill(self):
        if 'bill' not in self.extra_data:
            self.extra_data['bill'] = {}
        if not isinstance(self.extra_data['bill'], dict):
            self.extra_data['bill'] = {}
        return self.extra_data['bill']

    @bill.setter
    def bill(self, value):
        if not isinstance(value, dict):
            value = {}
        self.extra_data['bill'] = value
        self.extra = yaml_dump_with_ruby_tags(self.extra_data)

    @property
    def qb_id(self):
        return self.bill.get('qb_id')

    @qb_id.setter
    def qb_id(self, value):
        if not isinstance(self.extra_data.get('bill'), dict):
            self.extra_data['bill'] = {}
        self.extra_data['bill']['qb_id'] = value
        self.extra = yaml_dump_with_ruby_tags(self.extra_data)
        logger.debug(f"Updated 'extra' field for UtilityFee#{self.id}: {self.extra}")


class House(Base):
    __tablename__ = 'houses'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    qb_account = Column(String)
    qbid = Column(String)

    cost_items = relationship("CostItem", back_populates="house")
    location = relationship(
        "Location",
        primaryjoin="and_(foreign(House.id) == Location.locationable_id, "
                    "Location.locationable_type == 'House')",
        back_populates="house",
        # uselist=False
    )

    def get_city(self, locale='en'):
        if self.location and self.location.translations:
            if isinstance(self.location.translations, list):
                translation = next((t for t in self.location.translations if t.locale == locale), None)
            else:
                translation = self.location.translations if self.location.translations.locale == locale else None
            return translation.city if translation else None
        return None

    @property
    def qb_name(self):
        location = self.location
        if location:
            return f"H#{self.id}, {location.address2}, {self.city.title()}, {location.state.upper()} {location.zip}"
        else:
            return None

    @property
    def city(self):
        return self.get_city()

    utility_fees = relationship(
        "UtilityFee",
        back_populates="house",
    )


class LocationTranslation(Base):
    __tablename__ = 'location_translations'

    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, nullable=False)
    locale = Column(String, nullable=False)
    city = Column(String)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)

    location = relationship(
        "Location",
        back_populates="translations",
        primaryjoin="LocationTranslation.location_id == foreign(Location.id)"
    )


class Location(Base):
    __tablename__ = 'locations'

    id = Column(Integer, primary_key=True)
    address = Column(String)
    address2 = Column(String)
    state = Column(String)
    country = Column(String)
    zip = Column(String)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    locationable_id = Column(Integer)
    locationable_type = Column(String)
    house_number = Column(String)
    street_number = Column(String)

    translations = relationship(
        "LocationTranslation",
        back_populates="location",
        primaryjoin="foreign(Location.id) == LocationTranslation.location_id",
    )

    house = relationship(
        "House",
        primaryjoin="and_(Location.locationable_id == foreign(House.id), "
                    "Location.locationable_type == 'House')",
        back_populates="location",
        uselist=False
    )

    def get_city(self, locale='en'):
        translation = next((t for t in self.translations if t.locale == locale), None)
        return translation.city if translation else None


class Cost(Base):
    __tablename__ = 'costs'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    holidale_expense = Column(Numeric(10, 2))
    landlord_expense = Column(Numeric(10, 2))
    bill_date = Column(DateTime, nullable=False)
    landlord_statement_id = Column(Integer)  # ForeignKey to landlord_statements, but we're not modeling it
    due_date = Column(DateTime)
    payment_paid_date = Column(DateTime)
    category = Column(String)
    status = Column(String)
    cost_id = Column(Integer)
    payment_method = Column(String)
    cleaner_id = Column(Integer, ForeignKey('cleaners.id'))
    invoice_number = Column(String)
    business_entity = Column(String, default='holidale')
    account_category = Column(String)
    batch_number = Column(String)
    approver_id = Column(Integer)
    approved_date = Column(DateTime)
    qbid = Column(String)
    last_sync_time = Column(DateTime)
    holidale_cost_id = Column(Integer)
    external_invoice_id = Column(String(40))
    reimbursement = Column(Boolean, default=False)

    cleaner = relationship("Cleaner", back_populates="costs")
    cost_items = relationship("CostItem", back_populates="cost")
    cost_sub_items = relationship("CostSubItem", back_populates="cost")


class CostItem(Base):
    __tablename__ = 'cost_items'

    id = Column(Integer, primary_key=True)
    cost_id = Column(Integer, ForeignKey('costs.id'))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    amount = Column(Numeric(10, 2))
    description = Column(Text)
    house_id = Column(Integer, ForeignKey('houses.id'))
    booking_id = Column(Integer)  # ForeignKey to bookings, but we're not modeling it
    service_schedule_id = Column(Integer)  # ForeignKey to service_schedules, but we're not modeling it
    customer = Column(String)
    cost_item_id = Column(Integer)
    financial_account_id = Column(Integer, ForeignKey('financial_accounts.id'))  # New ForeignKey
    holidale_cost_item_id = Column(Integer)
    qbid = Column(Integer, index=True)
    service_date = Column(Date)

    cost = relationship("Cost", back_populates="cost_items")
    house = relationship("House", back_populates="cost_items")
    cost_sub_items = relationship("CostSubItem", back_populates="cost_item")
    financial_account = relationship("FinancialAccount", back_populates="cost_items")


class FinancialAccount(Base):
    __tablename__ = 'financial_accounts'

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    detail = Column(String(255))
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    qbid = Column(String(255))
    active = Column(Boolean, default=True, nullable=False)
    category = Column(Integer, default=0)

    # Relationship to CostItem
    cost_items = relationship("CostItem", back_populates="financial_account")


class CostSubItem(Base):
    __tablename__ = 'cost_sub_items'

    id = Column(Integer, primary_key=True)
    cost_id = Column(Integer, ForeignKey('costs.id'))
    cost_item_id = Column(Integer, ForeignKey('cost_items.id'))
    amount = Column(Numeric(10, 2), default=0.0)
    description = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    cost_sub_item_id = Column(Integer)

    cost = relationship("Cost", back_populates="cost_sub_items")
    cost_item = relationship("CostItem", back_populates="cost_sub_items")


class Cleaner(Base):
    __tablename__ = 'cleaners'

    id = Column(Integer, primary_key=True)
    rate_setting_group_id = Column(Integer)
    profession = Column(String, nullable=False)
    full_name = Column(String)
    phone = Column(String)
    email = Column(String)
    note = Column(Text)
    receive_message = Column(Boolean, default=False, nullable=False)
    receive_email = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    status = Column(Boolean, default=False)
    qbid = Column(String)
    last_sync_time = Column(DateTime)
    greenland_qb_id = Column(String)
    review_size = Column(Integer, default=0)
    workorder_size = Column(Integer, default=0)
    average_review = Column(Numeric(10, 2), default=0.0)
    average_price = Column(Numeric(10, 2), default=0.0)
    label_names = Column(String(5120))
    extra = Column(Text)
    preferred_by_owner = Column(Boolean)
    enable_access_code_visibility = Column(Boolean, default=False)

    # Relationship with Cost
    costs = relationship("Cost", back_populates="cleaner")
