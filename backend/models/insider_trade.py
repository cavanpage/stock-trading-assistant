import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.db.session import Base


class InsiderTrade(Base):
    __tablename__ = "insider_trades"

    __table_args__ = (
        UniqueConstraint(
            "accession_number", "sequence_number",
            name="uq_insider_trade",
        ),
        Index("ix_insider_trade_ticker",           "ticker"),
        Index("ix_insider_trade_transaction_date", "transaction_date"),
        Index("ix_insider_trade_owner_name",       "owner_name"),
        Index("ix_insider_trade_owner_cik",        "owner_cik"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    # Filing metadata
    accession_number: Mapped[str]  = mapped_column(String, nullable=False)
    sequence_number:  Mapped[int]  = mapped_column(Integer, default=0)   # row within a filing
    filing_date:      Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    filing_url:       Mapped[str | None] = mapped_column(String, nullable=True)

    # Issuer (the company)
    ticker:       Mapped[str] = mapped_column(String, nullable=False)
    issuer_name:  Mapped[str] = mapped_column(String, nullable=False)
    issuer_cik:   Mapped[str] = mapped_column(String, nullable=False)

    # Reporting owner (the insider)
    owner_name:  Mapped[str]       = mapped_column(String, nullable=False)
    owner_cik:   Mapped[str]       = mapped_column(String, nullable=False)
    owner_title: Mapped[str | None] = mapped_column(String, nullable=True)   # "Chief Executive Officer"
    is_director: Mapped[bool]      = mapped_column(default=False)
    is_officer:  Mapped[bool]      = mapped_column(default=False)
    is_ten_pct:  Mapped[bool]      = mapped_column(default=False)            # 10%+ holder

    # Transaction
    security_title:    Mapped[str]        = mapped_column(String, nullable=False)  # "Common Stock"
    transaction_date:  Mapped[datetime]   = mapped_column(DateTime(timezone=True), nullable=False)
    transaction_code:  Mapped[str]        = mapped_column(String, nullable=False)
    # A=open-market buy, D=open-market sell, S=sale, M=option exercise,
    # G=gift, F=tax withholding, P=purchase, X=option exercise

    shares:            Mapped[float | None] = mapped_column(Float, nullable=True)
    price_per_share:   Mapped[float | None] = mapped_column(Float, nullable=True)
    total_value:       Mapped[float | None] = mapped_column(Float, nullable=True)
    # computed = shares * price_per_share, null if either missing

    acquired_disposed: Mapped[str] = mapped_column(String, nullable=False)   # "A" or "D"
    shares_after:      Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<InsiderTrade({self.ticker} {self.acquired_disposed} "
            f"{self.shares} @ {self.price_per_share} by {self.owner_name!r})>"
        )
