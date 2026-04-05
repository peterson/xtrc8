"""Shared utilities."""

import re


def sanitize_handle(handle: str) -> str:
    """Strip anything that isn't alphanumeric or underscore from a social handle."""
    return re.sub(r'[^a-zA-Z0-9_]', '', handle) or "unknown"


def slugify(text: str, max_len: int = 60) -> str:
    text = text.lower()
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s]+', '-', text.strip())
    text = re.sub(r'-+', '-', text).strip('-')
    return text[:max_len].rstrip('-')
