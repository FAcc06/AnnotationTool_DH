"""HTML fetching and poem parsing functionality."""

import json
import os
import time
from pathlib import Path
from typing import Tuple, Optional
import requests
from bs4 import BeautifulSoup
from slugify import slugify

from models import PoemMeta, PoemText
from utils import clean_text


def fetch_html(url: str, use_cache: bool = True) -> str:
    """
    Fetch HTML from URL with caching and retry logic.
    
    Args:
        url: The URL to fetch
        use_cache: Whether to use cached version if available
        
    Returns:
        HTML content as string
        
    Raises:
        requests.RequestException: If fetch fails and no cache available
    """
    # Create cache directory
    cache_dir = Path("html_cache")
    cache_dir.mkdir(exist_ok=True)
    
    # Generate cache filename
    safe_filename = slugify(url) + ".html"
    cache_path = cache_dir / safe_filename
    
    # Try to use cache first if requested
    if use_cache and cache_path.exists():
        try:
            return cache_path.read_text(encoding='utf-8')
        except Exception:
            pass  # Fall through to fetch
    
    # Fetch from web with retries
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                html = response.text
                # Cache the result
                try:
                    cache_path.write_text(html, encoding='utf-8')
                except Exception:
                    pass  # Don't fail if we can't cache
                return html
            elif response.status_code in [429, 403, 503]:
                # Rate limited or forbidden - wait and retry
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + 1  # Exponential backoff
                    time.sleep(wait_time)
                    continue
            else:
                response.raise_for_status()
                
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                # Last attempt failed, try cache
                if cache_path.exists():
                    try:
                        return cache_path.read_text(encoding='utf-8')
                    except Exception:
                        pass
                raise e
            else:
                # Wait before retry
                time.sleep(1)
    
    raise requests.RequestException(f"Failed to fetch {url} after {max_retries} attempts")


def parse_poem(html: str, url: str) -> Tuple[PoemMeta, PoemText]:
    """
    Parse poem metadata and text from HTML.
    
    Args:
        html: Raw HTML content
        url: Original URL
        
    Returns:
        Tuple of (PoemMeta, PoemText)
    """
    soup = BeautifulSoup(html, 'lxml')
    
    # Initialize metadata
    meta = PoemMeta(url=url)
    
    # Canonical URL
    canonical_link = soup.find('link', rel='canonical')
    if canonical_link and canonical_link.get('href'):
        meta.canonical_url = canonical_link['href']
    
    # Find main poem article
    poem_article = soup.find('article', class_=lambda x: x and 'card--poem-full' in x)
    
    if poem_article:
        # Poem UUID
        meta.poem_uuid = poem_article.get('data-poem-uuid')
        
        # Title
        title_elem = poem_article.find('h1')
        if title_elem:
            meta.title = clean_text(title_elem.get_text())
        
        # Author
        author_field = poem_article.find(class_=lambda x: x and 'field--field_author' in x)
        if author_field:
            # Prefer data-byline-author-name link
            author_link = author_field.find('a', attrs={'data-byline-author-name': True})
            if not author_link:
                # Fallback to any link in author field
                author_link = author_field.find('a')
            
            if author_link:
                meta.author = clean_text(author_link.get_text())
                meta.author_href = author_link.get('href')
        
        # Themes
        themes_field = poem_article.find(class_=lambda x: x and 'field--field_poem_themes' in x)
        if themes_field:
            theme_links = themes_field.find_all('a')
            meta.themes = [clean_text(link.get_text()) for link in theme_links]
        
        # About this poem
        about_field = poem_article.find(class_=lambda x: x and 'field--field_about_this_poem' in x)
        if about_field:
            meta.about = clean_text(about_field.get_text())
        
        # Public domain check
        credit_field = poem_article.find(class_=lambda x: x and 'field--field_credit' in x)
        if credit_field:
            credit_text = credit_field.get_text().lower()
            meta.public_domain = 'public domain' in credit_text
    
    # Parse JSON-LD for additional metadata
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, dict) and '@graph' in data:
                for item in data['@graph']:
                    if item.get('@type') == 'Article':
                        if not meta.title and 'headline' in item:
                            meta.title = clean_text(item['headline'])
                        if 'datePublished' in item:
                            meta.date_published = item['datePublished']
                        if 'dateModified' in item:
                            meta.date_modified = item['dateModified']
                        break
        except (json.JSONDecodeError, KeyError, TypeError):
            continue
    
    # Extract poem text
    poem_text = ""
    raw_html = ""
    
    if poem_article:
        # Primary: Look for .field--body within poem article
        body_field = poem_article.find(class_=lambda x: x and 'field--body' in x)
        if body_field:
            raw_html = str(body_field)
            poem_text = extract_poem_text_from_body(body_field)
    
    # Fallback: Use JSON-LD description if no poem text found
    if not poem_text.strip():
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and '@graph' in data:
                    for item in data['@graph']:
                        if item.get('@type') == 'Article' and 'description' in item:
                            poem_text = clean_text(item['description'])
                            # Normalize line breaks
                            poem_text = poem_text.replace('\\n', '\n')
                            break
            except (json.JSONDecodeError, KeyError, TypeError):
                continue
    
    return meta, PoemText(raw_html=raw_html, text=poem_text)


def extract_poem_text_from_body(body_element) -> str:
    """
    Extract poem text from body field, preserving line breaks and stanza structure.
    
    Args:
        body_element: BeautifulSoup element containing poem body
        
    Returns:
        Formatted poem text with preserved line breaks
    """
    if not body_element:
        return ""
    
    # Process each paragraph separately
    paragraphs = body_element.find_all(['p', 'div'], recursive=True)
    if not paragraphs:
        # If no paragraphs, treat entire body as one block
        paragraphs = [body_element]
    
    stanzas = []
    for para in paragraphs:
        # Convert <br> tags to newlines
        for br in para.find_all('br'):
            br.replace_with('\n')
        
        # Handle long-line spans specially
        for span in para.find_all('span', class_='long-line'):
            # Keep long-line content as-is, just clean trailing NBSPs
            span_text = span.get_text()
            # Remove trailing non-breaking spaces
            span_text = span_text.rstrip('\u00a0 ')
            span.string = span_text
        
        # Get text and clean it
        para_text = para.get_text()
        para_text = clean_text(para_text)
        
        # Remove excessive whitespace but preserve intentional line breaks
        lines = para_text.split('\n')
        cleaned_lines = [line.rstrip() for line in lines]
        para_text = '\n'.join(cleaned_lines)
        
        if para_text.strip():
            stanzas.append(para_text.strip())
    
    # Join stanzas with blank lines
    return '\n\n'.join(stanzas)

