"""Data models for the qualitative coding application."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class PoemMeta:
    """Metadata extracted from a poem page."""
    url: str
    canonical_url: Optional[str] = None
    poem_uuid: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    author_href: Optional[str] = None
    date_published: Optional[str] = None
    date_modified: Optional[str] = None
    themes: list[str] = None
    about: Optional[str] = None
    public_domain: bool = False
    
    def __post_init__(self):
        if self.themes is None:
            self.themes = []


@dataclass
class PoemText:
    """Poem text content."""
    raw_html: str
    text: str


@dataclass
class CodingRecord:
    """A coding record for a poem."""
    timestamp_iso: str
    coder_id: str
    url: str
    poem_uuid: Optional[str]
    title: Optional[str]
    author: Optional[str]
    tags: list[str]
    moods: list[str]  # anger, anticipation, disgust, fear, joy, sadness, surprise, trust
    sentiment_x: float  # -10 to 10 horizontal axis
    sentiment_y: float  # -10 to 10 vertical axis
    notes: str
    is_complete: bool
    html_sha1: str
    extraction_ok: bool
    year: Optional[str] = None  # Publication year from CSV
    group: Optional[str] = None  # Group classification from CSV
    author_url: Optional[str] = None  # Author URL from CSV
    error: Optional[str] = None
    # Keep sentiment for backward compatibility
    sentiment: Optional[str] = None
    # Timing data
    time_spent_seconds: Optional[float] = None  # Time spent on this poem in seconds
    # Staged timing data
    stage_timings: Optional[dict] = None  # Dictionary with stage timings: {"poem": 10.5, "themes": 5.2, "mood": 3.1, "chart": 2.8, "notes": 4.0}
    # Explanation fields
    tags_explanation: Optional[str] = None  # Explanation for tag selections
    moods_explanation: Optional[str] = None  # Explanation for mood selections
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.moods is None:
            self.moods = []
