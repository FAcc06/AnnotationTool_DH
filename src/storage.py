"""Local storage functionality for coding records."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import pandas as pd

from models import CodingRecord


def _dict_to_coding_record(record_dict: dict) -> CodingRecord:
    """
    Convert a dictionary to a CodingRecord object.
    
    Args:
        record_dict: Dictionary containing record data
        
    Returns:
        CodingRecord object
    """
    return CodingRecord(
        timestamp_iso=record_dict['timestamp_iso'],
        coder_id=record_dict['coder_id'],
        url=record_dict['url'],
        poem_uuid=record_dict.get('poem_uuid'),
        title=record_dict.get('title'),
        author=record_dict.get('author'),
        year=record_dict.get('year'),
        group=record_dict.get('group'),
        author_url=record_dict.get('author_url'),
        tags=record_dict.get('tags', []),
        moods=record_dict.get('moods', []),
        sentiment_x=record_dict.get('sentiment_x', 0.0),
        sentiment_y=record_dict.get('sentiment_y', 0.0),
        notes=record_dict.get('notes', ''),
        is_complete=record_dict.get('is_complete', False),
        html_sha1=record_dict.get('html_sha1', ''),
        extraction_ok=record_dict.get('extraction_ok', True),
        error=record_dict.get('error'),
        sentiment=record_dict.get('sentiment'),  # Keep for backward compatibility
        time_spent_seconds=record_dict.get('time_spent_seconds'),
        stage_timings=record_dict.get('stage_timings'),
    )


def _read_jsonl_records(jsonl_path: Path, url_filter: str = None, coder_filter: str = None):
    """
    Generator that yields records from JSONL file with optional filtering.
    
    Args:
        jsonl_path: Path to JSONL file
        url_filter: Optional URL to filter by
        coder_filter: Optional coder ID to filter by
        
    Yields:
        Dictionary records that match the filters
    """
    if not jsonl_path.exists():
        return
        
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    record_dict = json.loads(line)
                    # Apply filters
                    if url_filter and record_dict.get('url') != url_filter:
                        continue
                    if coder_filter and record_dict.get('coder_id') != coder_filter.strip():
                        continue
                    yield record_dict
                except json.JSONDecodeError:
                    continue


def ensure_coding_dir() -> Path:
    """Ensure coding_records directory exists and return path."""
    coding_dir = Path("coding_records")
    coding_dir.mkdir(exist_ok=True)
    return coding_dir


def save_record(record: CodingRecord) -> None:
    """
    Save a coding record to both JSONL log and CSV snapshot.
    
    Args:
        record: The coding record to save
    """
    coding_dir = ensure_coding_dir()
    
    # Append to JSONL log
    jsonl_path = coding_dir / "codings.jsonl"
    record_dict = {
        'timestamp_iso': record.timestamp_iso,
        'coder_id': record.coder_id,
        'url': record.url,
        'poem_uuid': record.poem_uuid,
        'title': record.title,
        'author': record.author,
        'year': getattr(record, 'year', None),
        'group': getattr(record, 'group', None),
        'author_url': getattr(record, 'author_url', None),
        'tags': record.tags,
        'moods': record.moods,
        'sentiment_x': record.sentiment_x,
        'sentiment_y': record.sentiment_y,
        'notes': record.notes,
        'is_complete': record.is_complete,
        'html_sha1': record.html_sha1,
        'extraction_ok': record.extraction_ok,
        'error': record.error,
        # Keep sentiment for backward compatibility
        'sentiment': getattr(record, 'sentiment', None),
        'time_spent_seconds': getattr(record, 'time_spent_seconds', None),
        'stage_timings': getattr(record, 'stage_timings', None),
    }
    
    with open(jsonl_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record_dict, ensure_ascii=False) + '\n')
    
    # Regenerate CSV snapshot from all JSONL records
    update_csv_snapshot()


def update_csv_snapshot() -> None:
    """Update the CSV snapshot from all JSONL records."""
    coding_dir = ensure_coding_dir()
    jsonl_path = coding_dir / "codings.jsonl"
    csv_path = coding_dir / "codings.csv"
    
    if not jsonl_path.exists():
        return
    
    # Read all JSONL records
    records = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    record = json.loads(line)
                    # Join tags and moods for CSV
                    record['tags_joined'] = '; '.join(record.get('tags', []))
                    record['moods_joined'] = '; '.join(record.get('moods', []))
                    records.append(record)
                except json.JSONDecodeError:
                    continue
    
    if records:
        # Create DataFrame and save as CSV
        df = pd.DataFrame(records)
        # Ensure proper column order
        columns = [
            'timestamp_iso', 'coder_id', 'url', 'poem_uuid', 'title', 'author', 
            'year', 'group', 'author_url',
            'tags_joined', 'moods_joined', 'sentiment', 'sentiment_x', 'sentiment_y', 'notes', 
            'is_complete', 'html_sha1', 'extraction_ok', 'error', 'time_spent_seconds', 'stage_timings'
        ]
        # Only include columns that exist in the data
        available_columns = [col for col in columns if col in df.columns]
        df = df[available_columns]
        
        df.to_csv(csv_path, index=False, encoding='utf-8')


def latest_record_for_coder(url: str, coder_id: str) -> Optional[CodingRecord]:
    """
    Get the latest coding record for a specific URL and coder.
    
    Args:
        url: The URL to search for
        coder_id: The coder ID to filter by
        
    Returns:
        Latest CodingRecord for the URL and coder, or None if not found
    """
    if not coder_id.strip():
        return None
        
    coding_dir = ensure_coding_dir()
    jsonl_path = coding_dir / "codings.jsonl"
    
    latest_record = None
    for record_dict in _read_jsonl_records(jsonl_path, url_filter=url, coder_filter=coder_id):
        latest_record = _dict_to_coding_record(record_dict)
    
    return latest_record


def latest_record_for(url: str) -> Optional[CodingRecord]:
    """
    Get the latest coding record for a specific URL.
    
    Args:
        url: The URL to search for
        
    Returns:
        Latest CodingRecord for the URL, or None if not found
    """
    coding_dir = ensure_coding_dir()
    jsonl_path = coding_dir / "codings.jsonl"
    
    latest_record = None
    for record_dict in _read_jsonl_records(jsonl_path, url_filter=url):
        latest_record = _dict_to_coding_record(record_dict)
    
    return latest_record


def get_coding_stats() -> dict:
    """Get statistics about coding progress."""
    coding_dir = ensure_coding_dir()
    jsonl_path = coding_dir / "codings.jsonl"
    
    if not jsonl_path.exists():
        return {'total_records': 0, 'completed_records': 0, 'unique_urls': 0}
    
    urls_seen = set()
    completed_urls = set()
    total_records = 0
    
    for record in _read_jsonl_records(jsonl_path):
        url = record.get('url')
        if url:
            urls_seen.add(url)
            if record.get('is_complete', False):
                completed_urls.add(url)
            total_records += 1
    
    return {
        'total_records': total_records,
        'completed_records': len(completed_urls),
        'unique_urls': len(urls_seen),
    }
