import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from backend.db.session import Base


class SocialPost(Base):
    __tablename__ = "social_posts"

    __table_args__ = (Index("ix_social_posted_at", "posted_at"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    platform: Mapped[str] = mapped_column(String, nullable=False)  # "reddit" / "twitter"
    external_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    body: Mapped[str] = mapped_column(Text, nullable=False, default="")
    author: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    subreddit: Mapped[str | None] = mapped_column(String, nullable=True)
    upvotes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    symbols: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    sentiment_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    embedding_id: Mapped[str | None] = mapped_column(String, nullable=True)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    def __repr__(self) -> str:
        return f"<SocialPost(platform={self.platform!r}, external_id={self.external_id!r})>"
