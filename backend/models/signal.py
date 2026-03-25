import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.db.session import Base


class Signal(Base):
    __tablename__ = "signals"

    __table_args__ = (
        Index("ix_signal_symbol", "symbol"),
        Index("ix_signal_generated_at", "generated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    signal_type: Mapped[str] = mapped_column(String, nullable=False)  # "BUY" / "SELL" / "HOLD"
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    quant_score: Mapped[float] = mapped_column(Float, nullable=False)
    sentiment_score: Mapped[float] = mapped_column(Float, nullable=False)
    congress_modifier: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, default="1D", nullable=False)
    reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    def __repr__(self) -> str:
        return f"<Signal(symbol={self.symbol!r}, signal_type={self.signal_type!r}, confidence={self.confidence:.2f})>"
