import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from backend.db.session import Base


class EarningsTranscript(Base):
    __tablename__ = "earnings_transcripts"

    __table_args__ = (
        UniqueConstraint("ticker", "fiscal_quarter", "fiscal_year", name="uq_earnings_transcript"),
        Index("ix_transcript_ticker",       "ticker"),
        Index("ix_transcript_call_date",    "call_date"),
        Index("ix_transcript_fiscal_year",  "fiscal_year"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)

    ticker:        Mapped[str] = mapped_column(String, nullable=False)
    company_name:  Mapped[str] = mapped_column(String, nullable=False)
    fiscal_quarter: Mapped[int] = mapped_column(nullable=False)   # 1-4
    fiscal_year:   Mapped[int] = mapped_column(nullable=False)
    call_date:     Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    source:     Mapped[str]       = mapped_column(String, nullable=False)   # "motley_fool" | "sec_8k"
    source_url: Mapped[str | None] = mapped_column(String, nullable=True)

    full_text:     Mapped[str]        = mapped_column(Text, nullable=False)
    word_count:    Mapped[int]        = mapped_column(default=0)

    # FinBERT aggregate sentiment across the whole transcript
    sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)  # [-1, 1]

    # Optional: CEO/CFO section sentiment if we can extract their speaking turns
    exec_sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    def __repr__(self) -> str:
        return (
            f"<EarningsTranscript({self.ticker} Q{self.fiscal_quarter} {self.fiscal_year}"
            f" — {self.word_count} words)>"
        )
