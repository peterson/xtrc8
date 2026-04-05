"""Shared utilities."""

import re


def slugify(text: str, max_len: int = 60) -> str:
    text = text.lower()
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s]+', '-', text.strip())
    text = re.sub(r'-+', '-', text).strip('-')
    return text[:max_len].rstrip('-')
