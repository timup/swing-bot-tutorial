import enum

from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    Enum as SAEnum,
    ForeignKey,
    TIMESTAMP,
    func,
    UniqueConstraint,
)

# In SQLAlchemy 2.0, declarative_base is in sqlalchemy.orm
from sqlalchemy.orm import relationship, declarative_base

# Define Base using declarative_base()
Base = declarative_base()


class OptionType(enum.Enum):
    call = "call"
    put = "put"


class OptionContract(Base):
    __tablename__ = "options_contracts"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    # OCC Option Symbol Format: Underlying + Expiration Date (YYMMDD) + Option Type (C/P) + Strike Price (#######.### -> total 8 digits)
    # Example: SPXW240719P05000000 (SPX Weekly expiring 2024-07-19, Put, Strike 5000.000)
    # Example: AAPL251219C00150000 (AAPL expiring 2025-12-19, Call, Strike 150.000)
    # Length can vary slightly. Let's use String without fixed length for flexibility, but ensure uniqueness.
    contract_symbol = Column(String, unique=True, index=True, nullable=False)
    underlying_ticker = Column(String, index=True, nullable=False)
    option_type = Column(
        SAEnum(OptionType, name="optiontype_enum"), nullable=False
    )  # Added name for potential enum type creation in DB
    expiration_date = Column(Date, nullable=False, index=True)
    strike_price = Column(
        Numeric(10, 2), nullable=False
    )  # Precision 10, 2 decimal places

    # Additional static fields (can be added later if needed via migrations)
    # contract_size = Column(Integer, default=100, nullable=False)
    # currency = Column(String(3), default="USD", nullable=False)
    # primary_exchange = Column(String, nullable=True)

    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Relationship to quotes (one-to-many)
    quotes = relationship(
        "OptionQuote", back_populates="contract", cascade="all, delete-orphan"
    )

    __table_args__ = (
        # Unique constraint on the core contract details
        UniqueConstraint(
            "underlying_ticker",
            "expiration_date",
            "strike_price",
            "option_type",
            name="uq_option_contract_details",
        ),
        # Indexing for common lookup patterns
        # Index('ix_options_contracts_lookup', 'underlying_ticker', 'expiration_date', 'option_type'), # Covered by unique constraint
    )

    def __repr__(self):
        return f"<OptionContract(symbol='{self.contract_symbol}')>"


class OptionQuote(Base):
    __tablename__ = "options_quotes"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    contract_id = Column(
        Integer,
        ForeignKey("options_contracts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    quote_date = Column(Date, nullable=False, index=True)

    # Core EOD quote data
    open = Column(Numeric(12, 4), nullable=True)  # Increased precision
    high = Column(Numeric(12, 4), nullable=True)
    low = Column(Numeric(12, 4), nullable=True)
    close = Column(
        Numeric(12, 4), nullable=False
    )  # Assuming 'close' or 'last' is primary price
    volume = Column(Integer, nullable=True)
    open_interest = Column(Integer, nullable=True)
    implied_volatility = Column(
        Numeric(9, 6), nullable=True
    )  # Precision for IV (e.g., 0.123456)

    # Optional quote data
    bid = Column(Numeric(12, 4), nullable=True)
    ask = Column(Numeric(12, 4), nullable=True)

    # Greeks (optional, nullable)
    delta = Column(Numeric(9, 6), nullable=True)
    gamma = Column(Numeric(9, 6), nullable=True)
    theta = Column(Numeric(9, 6), nullable=True)
    vega = Column(Numeric(9, 6), nullable=True)
    rho = Column(Numeric(9, 6), nullable=True)

    # Metadata
    data_source = Column(
        String, nullable=True, index=True
    )  # E.g., "yfinance_live", "openbb_cboe_eod"
    fetched_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    # Relationship to contract (many-to-one)
    contract = relationship("OptionContract", back_populates="quotes")

    __table_args__ = (
        # Unique constraint per contract per day per source
        UniqueConstraint(
            "contract_id",
            "quote_date",
            "data_source",
            name="uq_option_quote_source_date",
        ),
        # Index for querying quotes by date
        # Index('ix_options_quotes_date', 'quote_date'), # Covered by index=True on column
    )

    def __repr__(self):
        return f"<OptionQuote(contract_id={self.contract_id}, date={self.quote_date}, close={self.close})>"


# Example of how to use Base.metadata with Alembic (will be done in env.py)
# target_metadata = Base.metadata
