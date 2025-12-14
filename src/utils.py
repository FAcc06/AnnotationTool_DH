"""Utility functions for the qualitative coding application."""

import hashlib
import re
from typing import List


def sha1(text: str) -> str:
    """Generate SHA1 hash of text."""
    return hashlib.sha1(text.encode('utf-8')).hexdigest()


def normalize_tags(input_text: str, base_tags: List[str]) -> List[str]:
    """
    Normalize tag input by splitting on common delimiters,
    lowercasing, and deduplicating.
    """
    if not input_text:
        return []
    
    # Split on comma, semicolon, or multiple spaces
    tags = re.split(r'[,;]\s*|\s{2,}', input_text.strip())
    
    # Clean and normalize
    normalized = []
    for tag in tags:
        tag = tag.strip().lower()
        if tag and tag not in [t.lower() for t in normalized]:
            # Find matching base tag (case-insensitive)
            matched = False
            for base_tag in base_tags:
                if tag == base_tag.lower():
                    normalized.append(base_tag)
                    matched = True
                    break
            if not matched:
                # Capitalize first letter for new tags
                normalized.append(tag.capitalize())
    
    return normalized


def clean_text(text: str) -> str:
    """Clean and normalize text content."""
    if not text:
        return ""
    
    # Replace common HTML entities
    replacements = {
        '&nbsp;': ' ',
        '&#8217;': "'",
        '&#8216;': "'",
        '&#8220;': '"',
        '&#8221;': '"',
        '&#8212;': '—',
        '&#8211;': '–',
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
    }
    
    for entity, replacement in replacements.items():
        text = text.replace(entity, replacement)
    
    return text.strip()

