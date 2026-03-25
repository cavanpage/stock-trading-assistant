import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.db.session import Base


class CongressTrade(Base):
    __tablename__ = "congress_trades"

    __table_args__ = (
        UniqueConstraint(
            "member_name", "ticker", "transaction_date", "transaction_type",
            name="uq_congress_trade",
        ),
        Index("ix_congress_member_name", "member_name"),
        Index("ix_congress_ticker", "ticker"),
        Index("ix_congress_transaction_date", "transaction_date"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    member_name: Mapped[str] = mapped_column(String, nullable=False)
    party: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    chamber: Mapped[str] = mapped_column(String, nullable=False)  # "House" / "Senate"
    ticker: Mapped[str] = mapped_column(String, nullable=False)
    asset_type: Mapped[str] = mapped_column(String, nullable=False)
    transaction_type: Mapped[str] = mapped_column(String, nullable=False)  # "Purchase" / "Sale"
    amount_range: Mapped[str] = mapped_column(String, nullable=False)
    transaction_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    disclosure_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    notes: Mapped[str | None] = mapped_column(String, nullable=True)
    source_url: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<CongressTrade(member={self.member_name!r}, ticker={self.ticker!r}, "
            f"type={self.transaction_type!r})>"
        )
