import hashlib


def content_hash(text: str) -> str:
    """Generate a SHA-256 hash for deduplication."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
