"""Streamlit app for qualitative coding of poems from Poets.org."""

import os
import time
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import requests
import plotly.graph_objects as go
import numpy as np

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from models import CodingRecord
from scraper import fetch_html, parse_poem
from storage import save_record, latest_record_for_coder, get_coding_stats
from utils import sha1, normalize_tags
from admin_page import admin_page
from firebase_storage import download_csv
from annotator_workset_manager import get_annotator_manager
import hashlib
import zipfile
import io


# Page configuration
st.set_page_config(
    page_title="Poetry Annotation System",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="expanded"
)

# FIREBASE AUTHENTICATION FUNCTIONS
def load_firebase_users():
    """Load users from Firebase admin/users.csv file."""
    try:
        users_df = download_csv("admin/users.csv")
        if users_df is not None:
            # Convert DataFrame to dictionary for easier lookup
            users_dict = {}
            for _, row in users_df.iterrows():
                # Handle different column names that might exist
                username = row.get('annotator_id') or row.get('username') or row.get('user_id')
                password = row.get('password')
                
                if username and password:
                    # Determine role based on username or explicit role column
                    role = row.get('role', 'annotator')  # Default to annotator
                    if str(username).lower() == 'admin':
                        role = 'admin'
                    
                    # Determine display name
                    name = row.get('name') or row.get('display_name') or str(username)
                    
                    users_dict[str(username)] = {
                        "password": str(password),
                        "role": role,
                        "name": name
                    }
            
            return users_dict
        else:
            st.error("Failed to load users from Firebase")
            return None
    except Exception as e:
        st.error(f"Error loading Firebase users: {str(e)}")
        return None

# FALLBACK LOGIN ACCOUNTS (used if Firebase fails)
FALLBACK_LOGIN_ACCOUNTS = {
    "admin": {
        "password": "admin",
        "role": "admin",
        "name": "Administrator"
    }
}

def hash_password(password: str) -> str:
    """Hash password using SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_login(username: str, password: str) -> dict:
    """Verify login credentials using Firebase users.csv with fallback support."""
    # First try to load users from Firebase
    firebase_users = load_firebase_users()
    
    if firebase_users:
        # Try Firebase users first
        if username in firebase_users:
            stored_password = firebase_users[username]["password"]
            if password == stored_password:
                return firebase_users[username]
        
        # If not found in Firebase, try fallback accounts (especially for admin)
        if username in FALLBACK_LOGIN_ACCOUNTS:
            stored_password = FALLBACK_LOGIN_ACCOUNTS[username]["password"]
            if password == stored_password:
                return FALLBACK_LOGIN_ACCOUNTS[username]
    else:
        # Firebase unavailable, use fallback accounts only
        st.warning("Using fallback authentication (Firebase unavailable)")
        if username in FALLBACK_LOGIN_ACCOUNTS:
            stored_password = FALLBACK_LOGIN_ACCOUNTS[username]["password"]
            if password == stored_password:
                return FALLBACK_LOGIN_ACCOUNTS[username]
    
    return None
# GLOBAL CONFIGURATION - UI CUSTOMIZATION SETTINGS

ENABLE_UI_CUSTOMIZATION = False  # Set to False to disable all UI customization
ENABLE_TAG_COLUMNS_SETTING = False  # Set to False to disable tag columns setting
ENABLE_TAG_FONT_SIZE_SETTING = False  # Set to False to disable font size setting
ENABLE_TIMING_METHOD_SETTING = True  # Set to False to disable timing method selection
ENABLE_GRID_DENSITY_SETTING = False  # Set to False to disable grid density setting

DEFAULT_TAG_COLUMNS = 4  # Default number of columns for tag display
DEFAULT_TAG_FONT_SIZE = "medium"  # Default font size: "small", "medium", "large"
DEFAULT_TIMING_METHOD = "once"  # Default timing method: "invisible" or "staged"
DEFAULT_GRID_DENSITY = "0.5"  # Default grid density: "1.0", "0.5", "0.1"

# Font size mappings
FONT_SIZE_MAPPING = {
    "small": "0.8rem",
    "medium": "1rem", 
    "large": "1.2rem"
}

# Grid density mappings (number of points per axis)
GRID_DENSITY_MAPPING = {
    "1.0": 21,      # 21x21 = 441 points, precision to 1.0
    "0.5": 41,      # 41x41 = 1681 points, precision to 0.5
    "0.1": 201      # 201x201 = 40401 points, precision to 0.1
}

# Global CSS for tag styling
st.markdown("""
<style>
/* Global checkbox label styling */
.stCheckbox label {
    font-size: 1rem !important;
}

.stCheckbox label div {
    font-size: 1rem !important;
}

.stCheckbox label div p {
    font-size: 1rem !important;
    margin: 0.2rem 0 !important;
}

.stCheckbox {
    margin: 0.1rem 0 !important;
}

/* Additional targeting for different Streamlit versions */
div[data-testid="stCheckbox"] label {
    font-size: 1rem !important;
}

div[data-testid="stCheckbox"] label div {
    font-size: 1rem !important;
}

div[data-testid="stCheckbox"] label div p {
    font-size: 1rem !important;
}
</style>
""", unsafe_allow_html=True)

TOP_20_TAGS = [
    'nature', 'body', 'death', 'love', 'existential', 'identity', 'self',
    'beauty', 'america', 'loss', 'animals', 'history', 'memories', 'family',
    'writing', 'ancestry', 'thought', 'landscapes', 'war', 'time'
]

TOP_50_TAGS = TOP_20_TAGS + [
    'religion', 'grief', 'violence', 'aging', 'childhood', 'desire', 'night', 'mothers',
    'language', 'birds', 'social justice', 'music', 'flowers', 'politics',
    'hope', 'heartache', 'fathers', 'gender', 'environment', 'spirituality',
    'loneliness', 'oceans', 'dreams', 'survival', 'cities', 'earth', 'despair',
    'anxiety', 'weather', 'illness'
]

ALL_CORPUS_TAGS = TOP_50_TAGS + [
    'past', 'myth', 'travel', 'sadness', 'lgbtq', 'mourning', 'work', 'future', 
    'plants', 'afterlife', 'happiness', 'romance', 'sex', 'eating', 'love, contemporary', 
    'beginning', 'creation', 'turmoil', 'friendship', 'parenting', 'pastoral',
    'lust', 'immigration', 'daughters', 'anger', 'nostalgia', 'ambition',
    'migration', 'space', 'carpe diem', 'ghosts', 'marriage', 'reading',
    'popular culture', 'economy', 'tragedy', 'drinking', 'clothing', 'sons',
    'gun violence', 'americana', 'buildings', 'money', 'silence', 'gardens',
    'rebellion', 'new york city', 'heroes', 'science', 'gratitude',
    'storms', 'deception', 'technology', 'slavery', 'cooking', 'apocalypse',
    'humor', 'dance', 'doubt', 'regret', 'flight', 'sports',
    'national parks', 'school', 'oblivion', 'dogs', 'suffrage',
    'old age', 'drugs', 'teaching', 'innocence', 'sisters', 'enemies', 'brothers',
    'covid-19', 'math', 'american revolution', 'incarceration', 'pets', 'underworld',
    'pacifism', 'divorce', 'suburbia', 'theft', 'patience', 'movies', 'civil war',
    'cats', 'moving', 'luck', 'miracles', 'jealousy', 'vanity', 'infidelity', 'high school'
]

DEFAULT_BASE_TAGS = TOP_20_TAGS

MOOD_OPTIONS = ["anger", "anticipation", "disgust", "fear", "joy", "sadness", "surprise", "trust"]
SENTIMENT_OPTIONS = ["positive", "neutral", "negative", "unsure"]


def get_last_completed_index_for_coder(coder_id):
    """Get the index of the last completed poem for a specific coder."""
    if not coder_id.strip():
        return 0
        
    try:
        coding_dir = Path("coding_records")
        if not coding_dir.exists():
            return 0
            
        jsonl_path = coding_dir / "codings.jsonl"
        if not jsonl_path.exists():
            return 0
        
        completed_urls = set()
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        if (record.get('coder_id') == coder_id.strip() and 
                            record.get('is_complete', False)):
                            completed_urls.add(record.get('url'))
                    except json.JSONDecodeError:
                        continue
        
        return len(completed_urls)
    except Exception:
        return 0


def show_login_page():
    """Display login page."""
    st.title("🔐 Poetry Annotation System - Login")
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### Please login to continue")
        
        with st.form("login_form"):
            username = st.text_input(
                "Username",
                placeholder="Enter your username",
                help="Enter your username as configured in Firebase admin/users.csv"
            )
            
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password"
            )
            
            submitted = st.form_submit_button("Login", type="primary", use_container_width=True)
            
            if submitted:
                if not username or not password:
                    st.error("Please enter both username and password")
                else:
                    user_info = verify_login(username, password)
                    if user_info:
                        # Store login info in session state
                        st.session_state.logged_in = True
                        st.session_state.username = username
                        st.session_state.user_role = user_info["role"]
                        st.session_state.user_name = user_info["name"]
                        st.session_state.coder_id = username  # Set coder_id for annotators
                        st.success(f"Welcome, {user_info['name']}!")
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
        
        # Show authentication info
        with st.expander("ℹ️ Authentication Information"):
            firebase_users = load_firebase_users()
            if firebase_users:
                st.success("✅ Connected to Firebase authentication")
                st.markdown(f"**Firebase users:** {len(firebase_users)} accounts loaded")
                
                # Show available usernames (without passwords)
                usernames = list(firebase_users.keys())
                if usernames:
                    st.markdown("**Firebase usernames:**")
                    for username in sorted(usernames):
                        role = firebase_users[username].get('role', 'annotator')
                        st.markdown(f"- `{username}` ({role})")
                
                
            else:
                st.warning("⚠️ Firebase authentication unavailable")


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    # Login state
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False
    if 'username' not in st.session_state:
        st.session_state.username = ""
    if 'user_role' not in st.session_state:
        st.session_state.user_role = ""
    if 'user_name' not in st.session_state:
        st.session_state.user_name = ""
    
    # Existing session state variables
    if 'coder_id' not in st.session_state:
        st.session_state.coder_id = ""
    if 'base_tags' not in st.session_state:
        st.session_state.base_tags = DEFAULT_BASE_TAGS.copy()
    if 'current_index' not in st.session_state:
        st.session_state.current_index = 0
    if 'poems_df' not in st.session_state:
        st.session_state.poems_df = None
    if 'current_poem_meta' not in st.session_state:
        st.session_state.current_poem_meta = None
    if 'current_poem_text' not in st.session_state:
        st.session_state.current_poem_text = None
    if 'extraction_error' not in st.session_state:
        st.session_state.extraction_error = None
    if 'sentiment_x' not in st.session_state:
        st.session_state.sentiment_x = 0.0
    if 'sentiment_y' not in st.session_state:
        st.session_state.sentiment_y = 0.0
    if 'tag_set_preference' not in st.session_state:
        st.session_state.tag_set_preference = "top20"
    if 'just_saved_and_reset' not in st.session_state:
        st.session_state.just_saved_and_reset = False
    # Timer functionality
    if 'timer_start_time' not in st.session_state:
        st.session_state.timer_start_time = None
    if 'current_poem_url' not in st.session_state:
        st.session_state.current_poem_url = None
    # Staged timing functionality
    if 'timing_method' not in st.session_state:
        st.session_state.timing_method = DEFAULT_TIMING_METHOD
    if 'current_stage' not in st.session_state:
        st.session_state.current_stage = "poem"  # "poem", "themes", "mood", "chart", "notes"
    if 'stage_start_time' not in st.session_state:
        st.session_state.stage_start_time = None
    if 'stage_timings' not in st.session_state:
        st.session_state.stage_timings = {}
    # UI customization (use global defaults)
    if 'tag_columns' not in st.session_state:
        st.session_state.tag_columns = DEFAULT_TAG_COLUMNS
    if 'tag_font_size' not in st.session_state:
        st.session_state.tag_font_size = DEFAULT_TAG_FONT_SIZE
    if 'grid_density' not in st.session_state:
        st.session_state.grid_density = DEFAULT_GRID_DENSITY


def load_poets_csv(file_path: str) -> Optional[pd.DataFrame]:
    """Load and validate poets CSV file."""
    try:
        if not os.path.exists(file_path):
            st.error(f"File not found: {file_path}")
            return None
        
        df = pd.read_csv(file_path)
        
        required_columns = ['title', 'author', 'url']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            st.error(f"Missing required columns: {', '.join(missing_columns)}")
            st.error(f"Found columns: {', '.join(df.columns.tolist())}")
            return None
        
        df = df.dropna(subset=['url'])
        df = df[df['url'].str.strip() != '']
        df = df.drop_duplicates(subset=['url'])
        df = df.reset_index(drop=True)
        
        if len(df) == 0:
            st.error("No valid URLs found in the CSV file.")
            return None
        
        return df
        
    except Exception as e:
        st.error(f"Error loading CSV: {str(e)}")
        return None


def start_timer():
    """Start the timer for the current poem."""
    st.session_state.timer_start_time = time.time()

def stop_timer():
    """Stop the timer and return the elapsed time in seconds."""
    if st.session_state.timer_start_time is not None:
        elapsed = time.time() - st.session_state.timer_start_time
        st.session_state.timer_start_time = None
        return elapsed
    return 0.0

def get_elapsed_time():
    """Get the current elapsed time without stopping the timer."""
    if st.session_state.timer_start_time is not None:
        return time.time() - st.session_state.timer_start_time
    return 0.0

def start_stage_timer(stage_name):
    """Start timing for a specific stage."""
    st.session_state.stage_start_time = time.time()
    st.session_state.current_stage = stage_name

def stop_stage_timer():
    """Stop the current stage timer and record the time."""
    if st.session_state.stage_start_time is not None and st.session_state.current_stage:
        elapsed = time.time() - st.session_state.stage_start_time
        st.session_state.stage_timings[st.session_state.current_stage] = elapsed
        st.session_state.stage_start_time = None
        return elapsed
    return 0.0

def get_stage_elapsed_time():
    """Get the current stage elapsed time without stopping the timer."""
    if st.session_state.stage_start_time is not None:
        return time.time() - st.session_state.stage_start_time
    return 0.0

def reset_stage_timings():
    """Reset all stage timings."""
    st.session_state.stage_timings = {}
    st.session_state.current_stage = "poem"
    st.session_state.stage_start_time = None

def _clear_annotation_form_state():
    """Clear all annotation form states including tag and mood selections."""
    # Clear all tag and mood checkbox states, and custom inputs
    keys_to_remove = []
    for key in st.session_state.keys():
        if (key.startswith('tag_') or 
            key.startswith('search_tag_') or 
            key.startswith('mood_') or
            key.startswith('staged_mood_') or
            key.startswith('custom_mood_') or
            'custom_mood_input' in key or  # Catches all variations like custom_mood_input, staged_custom_mood_input, etc.
            key == 'annotation_notes'):
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        if key in st.session_state:
            del st.session_state[key]
    
    # Clear staged selections if they exist
    if 'staged_selected_tags' in st.session_state:
        del st.session_state['staged_selected_tags']
    if 'staged_selected_moods' in st.session_state:
        del st.session_state['staged_selected_moods']
    
    # Also clear sentiment coordinates for fresh start
    st.session_state.sentiment_x = 0.0
    st.session_state.sentiment_y = 0.0

def apply_tag_style():
    """Apply custom tag styling based on user preferences."""
    # Use global font size mapping
    font_size = FONT_SIZE_MAPPING.get(st.session_state.tag_font_size, "1rem")
    
    # Apply dynamic CSS for checkbox labels using JavaScript
    st.markdown(f"""
    <script>
    // Function to apply font size to all checkboxes
    function applyCheckboxFontSize() {{
        const checkboxes = document.querySelectorAll('.stCheckbox label, div[data-testid="stCheckbox"] label');
        checkboxes.forEach(checkbox => {{
            checkbox.style.fontSize = '{font_size}';
            const divs = checkbox.querySelectorAll('div');
            divs.forEach(div => {{
                div.style.fontSize = '{font_size}';
                const ps = div.querySelectorAll('p');
                ps.forEach(p => {{
                    p.style.fontSize = '{font_size}';
                }});
            }});
        }});
    }}
    
    // Apply immediately
    applyCheckboxFontSize();
    
    // Apply after a short delay to catch dynamically loaded elements
    setTimeout(applyCheckboxFontSize, 100);
    setTimeout(applyCheckboxFontSize, 500);
    </script>
    """, unsafe_allow_html=True)
    
    # Also apply CSS as backup
    st.markdown(f"""
    <style>
    .stCheckbox label {{
        font-size: {font_size} !important;
    }}
    
    .stCheckbox label div {{
        font-size: {font_size} !important;
    }}
    
    .stCheckbox label div p {{
        font-size: {font_size} !important;
    }}
    
    div[data-testid="stCheckbox"] label {{
        font-size: {font_size} !important;
    }}
    
    div[data-testid="stCheckbox"] label div {{
        font-size: {font_size} !important;
    }}
    
    div[data-testid="stCheckbox"] label div p {{
        font-size: {font_size} !important;
    }}
    </style>
    """, unsafe_allow_html=True)

def fetch_and_parse_current_poem():
    """Fetch and parse the current poem."""
    if st.session_state.poems_df is None or len(st.session_state.poems_df) == 0:
        return
    
    current_url = st.session_state.poems_df.iloc[st.session_state.current_index]['url']
    
    # Start timer if this is a new poem
    if st.session_state.current_poem_url != current_url:
        if st.session_state.timing_method == "once":
            start_timer()
        elif st.session_state.timing_method == "staged":
            reset_stage_timings()
            start_stage_timer("poem")
        st.session_state.current_poem_url = current_url
    
    try:
        with st.spinner("Fetching poem..."):
            html = fetch_html(current_url)
            meta, text = parse_poem(html, current_url)
            
            st.session_state.current_poem_meta = meta
            st.session_state.current_poem_text = text
            st.session_state.extraction_error = None
            
    except Exception as e:
        st.session_state.current_poem_meta = None
        st.session_state.current_poem_text = None
        st.session_state.extraction_error = str(e)


def render_workset_sidebar():
    """Render the sidebar for workset-based annotation."""
    st.sidebar.title("📝 Workset Annotation")
    
    # Show user info
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Logged in as:** {st.session_state.user_name}")
    st.sidebar.markdown(f"**Role:** {st.session_state.user_role.title()}")
    
    # Progress section removed per user request
    
    # UI Customization (only show if enabled)
    if ENABLE_UI_CUSTOMIZATION:
        st.sidebar.subheader("🎨 UI Settings")
        
        if ENABLE_TAG_COLUMNS_SETTING:
            tag_columns = st.sidebar.selectbox(
                "Tag Columns",
                options=[2, 3, 4, 5, 6],
                index=[2, 3, 4, 5, 6].index(st.session_state.get('tag_columns', DEFAULT_TAG_COLUMNS)),
                help="Number of columns for tag display (affects spacing)"
            )
            if tag_columns != st.session_state.get('tag_columns', DEFAULT_TAG_COLUMNS):
                st.session_state.tag_columns = tag_columns
                st.rerun()
        else:
            st.session_state.tag_columns = DEFAULT_TAG_COLUMNS
        
        if ENABLE_TAG_FONT_SIZE_SETTING:
            tag_font_size = st.sidebar.selectbox(
                "Tag Font Size",
                options=["small", "medium", "large"],
                index=["small", "medium", "large"].index(st.session_state.get('tag_font_size', DEFAULT_TAG_FONT_SIZE)),
                help="Font size for tag checkboxes"
            )
            if tag_font_size != st.session_state.get('tag_font_size', DEFAULT_TAG_FONT_SIZE):
                st.session_state.tag_font_size = tag_font_size
                st.rerun()
        else:
            st.session_state.tag_font_size = DEFAULT_TAG_FONT_SIZE
        
        if ENABLE_GRID_DENSITY_SETTING:
            current_density = st.session_state.get('grid_density', DEFAULT_GRID_DENSITY)
            if current_density not in ["1.0", "0.5", "0.1"]:
                st.session_state.grid_density = DEFAULT_GRID_DENSITY
            
            grid_density = st.sidebar.selectbox(
                "Chart Precision",
                options=["1.0", "0.5", "0.1"],
                index=["1.0", "0.5", "0.1"].index(st.session_state.grid_density),
                format_func=lambda x: f"Precision {x} (21×21 grid)" if x == "1.0" 
                                    else f"Precision {x} (41×41 grid)" if x == "0.5"
                                    else f"Precision {x} (201×201 grid)",
                help="Click precision on the sentiment chart. Higher precision = more clickable points but slower performance."
            )
            if grid_density != st.session_state.grid_density:
                st.session_state.grid_density = grid_density
                st.rerun()
        else:
            st.session_state.grid_density = DEFAULT_GRID_DENSITY
    
    if st.sidebar.button("🚪 Logout"):
        # Clear session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

def render_sidebar():
    """Render the sidebar with controls and progress."""
    st.sidebar.title("📝 Poem Coding")
    
    previous_coder_id = st.session_state.coder_id
    st.session_state.coder_id = st.sidebar.text_input(
        "Coder ID", 
        value=st.session_state.coder_id
    )
    
    # Timing method selection (only show if enabled)
    if st.session_state.coder_id.strip():
        if ENABLE_TIMING_METHOD_SETTING:
            st.sidebar.subheader("⏱️ Timing Method")
            timing_method = st.sidebar.radio(
                "Choose timing method:",
                options=["once", "staged"],
                format_func=lambda x: "Count once at start" if x == "once" else "Count on each stage",
                index=0 if st.session_state.timing_method == "once" else 1,
                key="timing_method_radio"
            )
            
            if timing_method != st.session_state.timing_method:
                st.session_state.timing_method = timing_method
                if timing_method == "staged":
                    reset_stage_timings()
                    start_stage_timer("poem")
                st.rerun()
        else:
            # Use default value when setting is disabled
            st.session_state.timing_method = DEFAULT_TIMING_METHOD
    
    # Only load data if coder ID is entered
    if st.session_state.coder_id.strip():
        csv_path = "src/poets.csv"
        if os.path.exists(csv_path):
            if st.session_state.poems_df is None:
                df_to_load = load_poets_csv(csv_path)
                if df_to_load is not None:
                    st.session_state.poems_df = df_to_load
                    st.session_state.current_index = 0
                    fetch_and_parse_current_poem()
        else:
            st.sidebar.error("poets.csv file not found")
        
        if (st.session_state.coder_id != previous_coder_id and 
            st.session_state.coder_id.strip() != ""):
            new_index = get_last_completed_index_for_coder(st.session_state.coder_id)
            if new_index != st.session_state.current_index:
                st.session_state.current_index = new_index
                fetch_and_parse_current_poem()
                st.rerun()
        
        # UI Customization (only show if enabled)
        if ENABLE_UI_CUSTOMIZATION:
            st.sidebar.subheader("🎨 UI Settings")
            
            if ENABLE_TAG_COLUMNS_SETTING:
                tag_columns = st.sidebar.selectbox(
                    "Tag Columns",
                    options=[2, 3, 4, 5, 6],
                    index=[2, 3, 4, 5, 6].index(st.session_state.tag_columns),
                    help="Number of columns for tag display (affects spacing)"
                )
                if tag_columns != st.session_state.tag_columns:
                    st.session_state.tag_columns = tag_columns
                    st.rerun()
            else:
                # Use default value when setting is disabled
                st.session_state.tag_columns = DEFAULT_TAG_COLUMNS
            
            if ENABLE_TAG_FONT_SIZE_SETTING:
                tag_font_size = st.sidebar.selectbox(
                    "Tag Font Size",
                    options=["small", "medium", "large"],
                    index=["small", "medium", "large"].index(st.session_state.tag_font_size),
                    help="Font size for tag checkboxes"
                )
                if tag_font_size != st.session_state.tag_font_size:
                    st.session_state.tag_font_size = tag_font_size
                    st.rerun()
            else:
                # Use default value when setting is disabled
                st.session_state.tag_font_size = DEFAULT_TAG_FONT_SIZE
            
            if ENABLE_GRID_DENSITY_SETTING:
                # Handle migration from old values to new values
                current_density = st.session_state.grid_density
                if current_density not in ["1.0", "0.5", "0.1"]:
                    # Map old values to new values
                    if current_density in ["low", "medium"]:
                        st.session_state.grid_density = "1.0"
                    elif current_density == "high":
                        st.session_state.grid_density = "0.5"
                    else:
                        st.session_state.grid_density = DEFAULT_GRID_DENSITY
                
                grid_density = st.sidebar.selectbox(
                    "Chart Precision",
                    options=["1.0", "0.5", "0.1"],
                    index=["1.0", "0.5", "0.1"].index(st.session_state.grid_density),
                    format_func=lambda x: f"Precision {x} (21×21 grid)" if x == "1.0" 
                                        else f"Precision {x} (41×41 grid)" if x == "0.5"
                                        else f"Precision {x} (201×201 grid)",
                    help="Click precision on the sentiment chart. Higher precision = more clickable points but slower performance."
                )
                if grid_density != st.session_state.grid_density:
                    st.session_state.grid_density = grid_density
                    st.rerun()
            else:
                # Use default value when setting is disabled
                st.session_state.grid_density = DEFAULT_GRID_DENSITY
        
        # Progress display removed per user request


@st.fragment
def render_sentiment_2d():
    """Render interactive 2D coordinate chart using Plotly."""
    st.subheader("Sentiment Coordinates")
    
    current_x = st.session_state.get('sentiment_x', 0.0)
    current_y = st.session_state.get('sentiment_y', 0.0)
    
    st.write("**Double click anywhere on the chart to set coordinates:**")
    
    dpi = 200

    def cm_to_pixels(cm, dpi):
        return int(cm / 2.54 * dpi)

    pixels_5cm = cm_to_pixels(5, dpi)

    def create_chart():
        fig = go.Figure()
        
        # Get grid density from session state
        grid_density = st.session_state.get('grid_density', DEFAULT_GRID_DENSITY)
        grid_size = GRID_DENSITY_MAPPING.get(grid_density, 21)
        
        x_vals = np.linspace(-10, 10, grid_size)
        y_vals = np.linspace(-10, 10, grid_size)
        
        x_grid, y_grid = [], []
        for y in y_vals:
            for x in x_vals:
                x_grid.append(x)
                y_grid.append(y)
        
        fig.add_trace(go.Scatter(
            x=x_grid, y=y_grid,
            mode='markers',
            marker=dict(size=3, color='rgba(0,0,0,0)'),
            showlegend=False,
            hoverinfo='none'
        ))
        
        if current_x is not None and current_y is not None:
            fig.add_trace(go.Scatter(
                x=[current_x], y=[current_y],
                mode='markers',
                marker=dict(size=8, color='red', symbol='x'),
                showlegend=False,
                hoverinfo='none'
            ))
        
        fig.add_annotation(
            x=-8, y=0.5,
            text="Negative",
            showarrow=False,
            font=dict(size=12,color = "red"),
            xanchor="center"
        )
        fig.add_annotation(
            x=8, y=0.5,
            text="Positive",
            showarrow=False,
           font=dict(size=12,color = "red"),
            xanchor="center"
        )
        fig.add_annotation(
            x=0.5, y=-7.3,
            text="Less Intensive",
            showarrow=False,
            font=dict(size=12,color = "red"),
            textangle=90,
            xanchor="center"
        )
        fig.add_annotation(
            x=0.5, y=7.2,
            text="More Intensive",
            showarrow=False,
            font=dict(size=12,color = "red"),
            textangle=90,
            xanchor="center"
        )
        
        fig.update_layout(
            title="",
            
            xaxis=dict(
                range=[-10, 10],
                showgrid=True,
                gridcolor='lightgray',
                zeroline=True,
                zerolinecolor='black',
                dtick=5,
                tickfont=dict(size=8),
                fixedrange=True,
                scaleanchor="y",
                scaleratio=1
            ),
            
            yaxis=dict(
                range=[-10, 10],
                showgrid=True,
                gridcolor='lightgray',
                zeroline=True,
                zerolinecolor='black',
                dtick=5,
                tickfont=dict(size=8),
                fixedrange=True
            ),
            
            width=pixels_5cm,
            height=pixels_5cm,
            margin=dict(l=50, r=50, t=30, b=50),
            
            plot_bgcolor='white',
            paper_bgcolor='white',
            showlegend=False,
            hovermode=False,
            dragmode=False
        )
        
        return fig

    fig = create_chart()

    chart_key = f"sentiment_chart_{st.session_state.current_index}_{current_x}_{current_y}"
    
    clicked_data = st.plotly_chart(
        fig,
        use_container_width=False,
        config={
            'displayModeBar': False,
            'staticPlot': False,
            'displaylogo': False,
            'responsive': False
        },
        on_select="rerun",
        key=chart_key
    )

    if clicked_data and 'selection' in clicked_data:
        if clicked_data['selection']['points']:
            point = clicked_data['selection']['points'][0]
            x_coord = round(point['x'], 1)
            y_coord = round(point['y'], 1)
            st.session_state.sentiment_x = x_coord
            st.session_state.sentiment_y = y_coord
            # No need for st.rerun() here - on_select="rerun" already handles it

    if current_x is not None and current_y is not None:
        st.success(f"**Selected Coordinates: X = {current_x}, Y = {current_y}**")

    st.markdown(f"""
    <style>
        .plotly-graph-div {{
            width: {pixels_5cm}px !important;
            height: {pixels_5cm}px !important;
        }}
        
        .stPlotlyChart > div {{
            width: {pixels_5cm}px !important;
            height: {pixels_5cm}px !important;
            margin: 0 auto !important;
        }}
    </style>
    """, unsafe_allow_html=True)


def render_navigation():
    """Render navigation controls."""
    if st.session_state.poems_df is None:
        return
    
    total_poems = len(st.session_state.poems_df)
    col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 2])
    
    with col1:
        if st.button("⬅️ Prev", disabled=st.session_state.current_index <= 0):
            st.session_state.current_index -= 1
            fetch_and_parse_current_poem()
            st.rerun()
    
    with col2:
        if st.button("➡️ Next", disabled=st.session_state.current_index >= total_poems - 1):
            st.session_state.current_index += 1
            fetch_and_parse_current_poem()
            st.rerun()
    
    with col3:
        if st.button("⏭️ Skip"):
            st.session_state.current_index = min(st.session_state.current_index + 1, total_poems - 1)
            fetch_and_parse_current_poem()
            st.rerun()
    
    with col4:
        if st.button("🔄 Reload"):
            fetch_and_parse_current_poem()
            st.rerun()
    
    with col5:
        if st.session_state.current_poem_meta:
            current_url = st.session_state.current_poem_meta.url
            st.link_button("🔗 Open Source Page", current_url)


def render_poem_display():
    """Render the poem content."""
    if st.session_state.extraction_error:
        st.error(f"Error loading poem: {st.session_state.extraction_error}")
        st.info("You can still navigate to other poems or try reloading this one.")
        return
    
    if not st.session_state.current_poem_meta or not st.session_state.current_poem_text:
        st.info("No poem loaded. Please select a CSV file with poem URLs.")
        return
    
    meta = st.session_state.current_poem_meta
    text = st.session_state.current_poem_text
    
    if meta.title:
        st.title(meta.title)
    else:
        st.title("Untitled Poem")
    
    if meta.author:
        if meta.author_href:
            st.markdown(f"**By:** [{meta.author}]({meta.author_href})")
        else:
            st.markdown(f"**By:** {meta.author}")
    
    current_row = None
    if st.session_state.poems_df is not None:
        current_row = st.session_state.poems_df.iloc[st.session_state.current_index]
        
        # Create info line with year and group
        info_parts = []
        if 'year' in current_row and pd.notna(current_row['year']):
            info_parts.append(f"**Year:** {current_row['year']}")
        if 'group' in current_row and pd.notna(current_row['group']):
            info_parts.append(f"**Group:** {current_row['group']}")
        
        if info_parts:
            st.markdown(" | ".join(info_parts))
    
    # Dates
    date_info = []
    if meta.date_published:
        date_info.append(f"Published: {meta.date_published}")
    if meta.date_modified:
        date_info.append(f"Modified: {meta.date_modified}")
    
    if date_info:
        st.caption(" | ".join(date_info))
    
    # Poem text
    if text.text:
        st.subheader("Poem Text")
        # Use code block to preserve formatting
        st.code(text.text, language=None)
    else:
        st.warning("No poem text could be extracted.")
    
    # Show themes only in staged mode and after poem stage
    if st.session_state.timing_method == "staged" and st.session_state.current_stage in ["themes", "mood", "chart", "notes"]:
        # Metadata
        col1, col2 = st.columns(2)
        
        with col1:
            if meta.themes:
                st.subheader("Themes")
                for theme in meta.themes:
                    st.badge(theme)
            
            if meta.public_domain:
                st.success("✅ Public Domain")
        
        with col2:
            if meta.about:
                st.subheader("About This Poem")
                st.write(meta.about)
    elif st.session_state.timing_method == "once":
        # Show all metadata in invisible mode
        col1, col2 = st.columns(2)
        
        with col1:
            if meta.themes:
                st.subheader("Themes")
                for theme in meta.themes:
                    st.badge(theme)
            
            if meta.public_domain:
                st.success("✅ Public Domain")
        
        with col2:
            if meta.about:
                st.subheader("About This Poem")
                st.write(meta.about)


def render_coding_panel():
    """Render the coding input panel."""
    if not st.session_state.current_poem_meta:
        return
    
    # Show different content based on timing method and stage
    if st.session_state.timing_method == "staged":
        render_staged_coding_panel()
    else:
        render_full_coding_panel()

def render_staged_coding_panel():
    """Render the staged coding panel."""
    st.subheader("🏷️ Coding Panel")
    
    # Show stage progress
    stages = ["poem", "themes", "mood", "chart", "notes"]
    current_stage_idx = stages.index(st.session_state.current_stage)
    
    # Stage progress indicator
    st.write("**Current Stage:**")
    cols = st.columns(len(stages))
    for i, stage in enumerate(stages):
        with cols[i]:
            if i < current_stage_idx:
                st.success(f"✅ {stage.title()}")
            elif i == current_stage_idx:
                st.info(f"🔄 {stage.title()}")
            else:
                st.write(f"⏳ {stage.title()}")
    
    # Show stage-specific content
    if st.session_state.current_stage == "poem":
        render_poem_stage()
    elif st.session_state.current_stage == "themes":
        render_themes_stage()
    elif st.session_state.current_stage == "mood":
        render_mood_stage()
    elif st.session_state.current_stage == "chart":
        render_chart_stage()
    elif st.session_state.current_stage == "notes":
        render_notes_stage()

def render_poem_stage():
    """Render the poem reading stage."""
    st.info("📖 **Stage 1: Read the Poem** - Take your time to read and understand the poem.")
    
    if st.button("✅ Finished Reading - Go to Themes", type="primary"):
        stop_stage_timer()
        start_stage_timer("themes")
        st.rerun()

def render_themes_stage():
    """Render the themes selection stage."""
    st.info("🏷️ **Stage 2: Select Themes** - Choose relevant thematic tags.")
    
    # Apply custom styling
    apply_tag_style()
    
    # Load existing record if available
    current_url = st.session_state.current_poem_meta.url
    existing_record = latest_record_for_coder(current_url, st.session_state.coder_id)
    default_tags = existing_record.tags if existing_record else []
    
    # Tag selection (full functionality for staged mode)
    st.subheader("📝 Theme Selection")
    
    tag_option = st.radio(
        "Choose tag set:",
        options=["top20", "top50"],
        format_func=lambda x: "Top 20 Tags" if x == "top20" else "Top 50 Tags",
        index=0 if st.session_state.tag_set_preference == "top20" else 1,
        horizontal=True,
        key="staged_tag_set_radio"
    )
    
    if tag_option != st.session_state.tag_set_preference:
        st.session_state.tag_set_preference = tag_option
    
    display_tags = TOP_20_TAGS if tag_option == "top20" else TOP_50_TAGS
    
    # Initialize selected_tags from session state
    if 'staged_selected_tags' not in st.session_state:
        st.session_state.staged_selected_tags = []
    
    selected_tags = []
    
    # Display main tags and collect selections
    num_columns = st.session_state.tag_columns
    for row in range(0, len(display_tags), num_columns):
        cols = st.columns(num_columns)
        for col_idx, tag in enumerate(display_tags[row:row+num_columns]):
            with cols[col_idx]:
                is_default_selected = tag in default_tags
                checkbox_key = f"staged_main_tag_{tag}"
                is_checked = st.checkbox(tag, value=is_default_selected, key=checkbox_key)
                if is_checked:
                    selected_tags.append(tag)
    
    # Search & Add More Tags section
    with st.expander("🔍 Search & Add More Tags"):
        search_term = st.text_input(
            "Search for additional tags:",
            placeholder="Type to search through all available tags...",
            key="staged_search_input"
        )
        
        if search_term:
            matching_tags = [tag for tag in ALL_CORPUS_TAGS 
                           if search_term.lower() in tag.lower() and tag not in selected_tags and tag not in display_tags]
            
            if matching_tags:
                st.write(f"Found {len(matching_tags)} additional matching tags:")
                search_columns = min(st.session_state.tag_columns, len(matching_tags))
                cols = st.columns(search_columns)
                for i, tag in enumerate(matching_tags[:12]):  # Limit to 12 results
                    with cols[i % search_columns]:
                        search_checkbox_key = f"staged_search_tag_{tag}"
                        is_checked = st.checkbox(f"{tag}", key=search_checkbox_key)
                        if is_checked:
                            selected_tags.append(tag)
            else:
                st.write("No additional matching tags found.")
        
        custom_tag_input = st.text_input(
            "Add custom tag:",
            placeholder="Enter tags separated by commas (e.g., tag1, tag2, tag3)...",
            help="Use this for tags not found in the standard corpus. Separate multiple tags with commas.",
            key="staged_custom_tag_input"
        )
        
        if custom_tag_input.strip():
            # Split by comma and add each tag
            custom_tags = [tag.strip() for tag in custom_tag_input.split(',') if tag.strip()]
            for custom_tag in custom_tags:
                if custom_tag not in selected_tags:
                    selected_tags.append(custom_tag)
    
    # Tags explanation box
    tags_explanation = st.text_area(
        "📝 Theme Explanation",
        placeholder="Select 1–2 lines, phrases, or words that influenced your choice, and write one short sentence explaining it.",
        height=80,
        key="staged_tags_explanation",
        help="Provide additional context or reasoning for your tag choices"
    )
    
    # Update session state with current selections
    st.session_state.staged_selected_tags = selected_tags
    st.session_state.staged_tags_explanation = tags_explanation
    
    # Display selection summary
    if selected_tags:
        st.info(f"✅ Selected {len(selected_tags)} tags: {', '.join(selected_tags[:5])}{'...' if len(selected_tags) > 5 else ''}")
    else:
        st.info("No tags selected yet")
    
    if st.button("✅ Finished Themes - Go to Mood", type="primary"):
        stop_stage_timer()
        start_stage_timer("mood")
        st.rerun()

def render_mood_stage():
    """Render the mood selection stage."""
    st.info("🎭 **Stage 3: Select Moods** - Choose emotional categories.")
    
    # Apply custom styling
    apply_tag_style()
    
    # Load existing record if available
    current_url = st.session_state.current_poem_meta.url
    existing_record = latest_record_for_coder(current_url, st.session_state.coder_id)
    default_moods = existing_record.moods if existing_record else []
    
    st.subheader("🎭 Mood Tags")
    selected_moods = []
    
    num_columns = st.session_state.tag_columns
    mood_rows = [MOOD_OPTIONS[i:i+num_columns] for i in range(0, len(MOOD_OPTIONS), num_columns)]
    for mood_row in mood_rows:
        cols = st.columns(num_columns)
        for col_idx, mood in enumerate(mood_row):
            with cols[col_idx]:
                is_default_selected = mood in default_moods
                mood_checkbox_key = f"staged_mood_{mood}"
                if st.checkbox(mood.capitalize(), value=is_default_selected, key=mood_checkbox_key):
                    selected_moods.append(mood)
    
    # Custom mood tags section
    with st.expander("🔍 Add Custom Mood Tags"):
        custom_mood_input = st.text_input(
            "Add custom mood:",
            placeholder="Enter emotions separated by commas (e.g., melancholy, euphoria, nostalgia)...",
            help="Use this for mood tags not found in the standard options. Separate multiple moods with commas.",
            key="staged_custom_mood_input"
        )
        
        if custom_mood_input.strip():
            # Split by comma and add each mood
            custom_moods = [mood.strip() for mood in custom_mood_input.split(',') if mood.strip()]
            for custom_mood in custom_moods:
                if custom_mood not in selected_moods:
                    selected_moods.append(custom_mood)
    
    # Moods explanation box
    moods_explanation = st.text_area(
        "📝 Moods Explanation",
        placeholder="Select 1–2 lines, phrases, or words that influenced your choice, and write one short sentence explaining it.",
        height=80,
        key="staged_moods_explanation",
        help="Provide additional context or reasoning for your mood choices"
    )
    
    # Store selected moods in session state
    st.session_state.staged_selected_moods = selected_moods
    
    # Display selection summary
    if selected_moods:
        st.info(f"✅ Selected {len(selected_moods)} moods: {', '.join(selected_moods[:5])}{'...' if len(selected_moods) > 5 else ''}")
    else:
        st.info("No moods selected yet")
    
    # Store moods explanation in session state
    st.session_state.staged_moods_explanation = moods_explanation
    
    if st.button("✅ Finished Moods - Go to Chart", type="primary"):
        stop_stage_timer()
        start_stage_timer("chart")
        st.rerun()

def render_chart_stage():
    """Render the sentiment chart stage."""
    st.info("📊 **Stage 4: Set Sentiment Coordinates** - Click on the chart to set coordinates.")
    
    # Load existing coordinates if available
    current_url = st.session_state.current_poem_meta.url
    existing_record = latest_record_for_coder(current_url, st.session_state.coder_id)
    
    if existing_record and not st.session_state.just_saved_and_reset:
        st.session_state.sentiment_x = getattr(existing_record, 'sentiment_x', 0.0)
        st.session_state.sentiment_y = getattr(existing_record, 'sentiment_y', 0.0)
    
    render_sentiment_2d()
    
    if st.button("✅ Finished Chart - Go to Notes", type="primary"):
        stop_stage_timer()
        start_stage_timer("notes")
        st.rerun()

def render_notes_stage():
    """Render the notes and final submission stage."""
    st.info("📝 **Stage 5: Add Notes & Submit** - Add any additional observations and submit.")
    
    # Load existing record if available
    current_url = st.session_state.current_poem_meta.url
    existing_record = latest_record_for_coder(current_url, st.session_state.coder_id)
    default_notes = existing_record.notes if existing_record else ""
    
    # Notes input (outside form)
    notes = st.text_area(
        "Notes",
        value=default_notes,
        height=100,
        key="staged_notes_input"
    )
    
    # Submit button (outside form)
    if st.button("💾 Save & Complete", type="primary", key="staged_submit"):
        submit_staged_annotation(notes, current_url)


def submit_staged_annotation(notes, current_url):
    """
    Submit staged annotation with validation and data processing.
    
    Args:
        notes: Notes text
        current_url: Current poem URL
    """
    if not st.session_state.coder_id.strip():
        st.error("Please enter a Coder ID first")
        return
    
    all_tags = st.session_state.get('staged_selected_tags', [])
    selected_moods = st.session_state.get('staged_selected_moods', [])
    tags_explanation = st.session_state.get('staged_tags_explanation', '')
    moods_explanation = st.session_state.get('staged_moods_explanation', '')
    
    if not all_tags:
        st.error("❌ Required: at least one theme must be selected")
        return
    if not selected_moods:
        st.error("❌ Required: at least one mood must be selected")
        return
    
    stop_stage_timer()
    
    current_csv_row = st.session_state.poems_df.iloc[st.session_state.current_index]
    html_content = st.session_state.current_poem_text.raw_html if st.session_state.current_poem_text else ""
    total_time = sum(st.session_state.stage_timings.values())
    
    record = CodingRecord(
        timestamp_iso=datetime.now().isoformat(),
        coder_id=st.session_state.coder_id.strip(),
        url=current_url,
        poem_uuid=st.session_state.current_poem_meta.poem_uuid,
        title=st.session_state.current_poem_meta.title,
        author=st.session_state.current_poem_meta.author,
        year=str(current_csv_row.get('year', '')) if pd.notna(current_csv_row.get('year')) else None,
        group=str(current_csv_row.get('group', '')) if pd.notna(current_csv_row.get('group')) else None,
        author_url=str(current_csv_row.get('author_url', '')) if pd.notna(current_csv_row.get('author_url')) else None,
        tags=all_tags,
        moods=selected_moods,
        sentiment_x=st.session_state.sentiment_x,
        sentiment_y=st.session_state.sentiment_y,
        notes=notes.strip(),
        tags_explanation=tags_explanation.strip() if tags_explanation else None,
        moods_explanation=moods_explanation.strip() if moods_explanation else None,
        is_complete=True,
        html_sha1=sha1(html_content),
        extraction_ok=st.session_state.extraction_error is None,
        error=st.session_state.extraction_error,
        time_spent_seconds=total_time,
        stage_timings=st.session_state.stage_timings.copy()
    )
    
    try:
        save_record(record)
        st.success("✅ Saved successfully!")
        
        keys_to_clear = []
        for tag in all_tags:
            keys_to_clear.extend([f"staged_main_tag_{tag}", f"staged_search_tag_{tag}"])
        for mood in selected_moods:
            keys_to_clear.append(f"staged_mood_{mood}")
        
        keys_to_clear.extend([
            'staged_custom_tag_input', 'staged_custom_mood_input', 
            'staged_notes_input', 'staged_tag_set_radio', 'staged_search_input'
        ])
        
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        if 'staged_selected_tags' in st.session_state:
            del st.session_state['staged_selected_tags']
        if 'staged_selected_moods' in st.session_state:
            del st.session_state['staged_selected_moods']
        
        st.session_state.sentiment_x = 0.0
        st.session_state.sentiment_y = 0.0
        
        reset_stage_timings()
        start_stage_timer("poem")
        
        if st.session_state.current_index < len(st.session_state.poems_df) - 1:
            time.sleep(1)
            st.session_state.current_index += 1
            fetch_and_parse_current_poem()
            st.rerun()
        else:
            st.rerun()
            
    except Exception as e:
        st.error(f"Save error: {str(e)}")


def clear_staged_annotation_session_state(selected_tags=None, selected_moods=None):
    """Clear staged annotation session state variables."""
    keys_to_delete = []
    
    # Clear specific tag keys based on selected_tags
    if selected_tags:
        for tag in selected_tags:
            # Staged main tag keys
            main_key = f"staged_main_tag_{tag}"
            if main_key in st.session_state:
                keys_to_delete.append(main_key)
            
            # Staged search tag keys
            search_key = f"staged_search_tag_{tag}"
            if search_key in st.session_state:
                keys_to_delete.append(search_key)
    
    # Clear specific mood keys based on selected_moods
    if selected_moods:
        for mood in selected_moods:
            mood_key = f"staged_mood_{mood}"
            if mood_key in st.session_state:
                keys_to_delete.append(mood_key)
    
    # Clear all remaining staged-specific keys
    all_session_keys = list(st.session_state.keys())
    for key in all_session_keys:
        if (key.startswith('staged_main_tag_') or key.startswith('staged_search_tag_') or 
            key.startswith('staged_mood_') or key == 'staged_custom_tag_input' or
            key == 'staged_custom_mood_input' or key == 'staged_notes_input' or 
            key == 'staged_tag_set_radio' or key == 'staged_search_input'):
            if key not in keys_to_delete:  # Avoid duplicates
                keys_to_delete.append(key)
    
    # Reset values instead of deleting
    for key in keys_to_delete:
        try:
            if key in st.session_state:
                if key.startswith(('staged_main_tag_', 'staged_search_tag_', 'staged_mood_')):
                    # For checkboxes, set to False
                    st.session_state[key] = False
                elif 'input' in key or 'notes' in key:
                    # For text inputs, set to empty string
                    st.session_state[key] = ""
                else:
                    # For other keys, delete them
                    del st.session_state[key]
        except KeyError:
            pass
    
    # Reset for next poem
    reset_stage_timings()
    start_stage_timer("poem")
    
    # Clear staged data
    if 'staged_selected_tags' in st.session_state:
        del st.session_state['staged_selected_tags']
    if 'staged_selected_moods' in st.session_state:
        del st.session_state['staged_selected_moods']
    
    # Reset sentiment coordinates
    st.session_state.sentiment_x = 0.0
    st.session_state.sentiment_y = 0.0
    

def render_full_coding_panel():
    """Render the full coding panel (original functionality)."""
    if not st.session_state.current_poem_meta:
        return
    
    # Apply custom styling
    apply_tag_style()
    
    st.subheader("🏷️ Coding Panel")
    
    current_url = st.session_state.current_poem_meta.url
    
    # Load existing record if available (only for current coder)
    # But skip loading if we just saved and reset to maintain clean state
    existing_record = None
    if not st.session_state.just_saved_and_reset:
        existing_record = latest_record_for_coder(current_url, st.session_state.coder_id)
    
    # Initialize form values
    default_tags = existing_record.tags if existing_record else []
    default_sentiment = existing_record.sentiment if existing_record else "neutral"
    default_notes = existing_record.notes if existing_record else ""
    default_complete = existing_record.is_complete if existing_record else False
    
    # Load existing coordinates if available (only once per poem)
    if existing_record and not st.session_state.just_saved_and_reset and (not hasattr(st.session_state, 'coords_loaded_for_url') or st.session_state.get('coords_loaded_for_url') != current_url):
        st.session_state.sentiment_x = getattr(existing_record, 'sentiment_x', 0.0)
        st.session_state.sentiment_y = getattr(existing_record, 'sentiment_y', 0.0)
        st.session_state.coords_loaded_for_url = current_url
    
    is_fresh_reset = st.session_state.just_saved_and_reset
    
    if st.session_state.just_saved_and_reset:
        st.session_state.just_saved_and_reset = False

    st.subheader("📝 Tag Selection")
    
    tag_option = st.radio(
        "Choose tag set:",
        options=["top20", "top50"],
        format_func=lambda x: "Top 20 Tags" if x == "top20" else "Top 50 Tags",
        index=0 if st.session_state.tag_set_preference == "top20" else 1,
        horizontal=True,
        key="tag_set_radio"
    )
    
    if tag_option != st.session_state.tag_set_preference:
        st.session_state.tag_set_preference = tag_option
    
    display_tags = TOP_20_TAGS if tag_option == "top20" else TOP_50_TAGS
    
    selected_tags = []
    
    # Use a dynamic key suffix that changes after each submission
    if 'form_version' not in st.session_state:
        st.session_state.form_version = 0
    key_suffix = f"_v{st.session_state.form_version}_{st.session_state.current_index}"
    
    num_columns = st.session_state.tag_columns
    for row in range(0, len(display_tags), num_columns):
        cols = st.columns(num_columns)
        for col_idx, tag in enumerate(display_tags[row:row+num_columns]):
            with cols[col_idx]:
                is_default_selected = tag in default_tags
                checkbox_key = f"main_tag_{tag}{key_suffix}"
                if st.checkbox(tag, value=is_default_selected, key=checkbox_key):
                    selected_tags.append(tag)
    
    with st.expander("🔍 Search & Add More Tags"):
        search_term = st.text_input(
            "Search for additional tags:",
            placeholder="Type to search through all available tags..."
        )
        
        if search_term:
            matching_tags = [tag for tag in ALL_CORPUS_TAGS 
                           if search_term.lower() in tag.lower() and tag not in selected_tags and tag not in display_tags]
            
            if matching_tags:
                st.write(f"Found {len(matching_tags)} additional matching tags:")
                search_columns = min(st.session_state.tag_columns, len(matching_tags))
                cols = st.columns(search_columns)
                for i, tag in enumerate(matching_tags[:12]):  # Limit to 12 results
                    with cols[i % search_columns]:
                        search_checkbox_key = f"search_tag_{tag}{key_suffix}"
                        if st.checkbox(f"{tag}", key=search_checkbox_key):
                            selected_tags.append(tag)
            else:
                st.write("No additional matching tags found.")
        
        custom_tag_input = st.text_input(
            "Add custom tag:",
            placeholder="Enter themes separated by commas (e.g., tag1, tag2, tag3)...",
            help="Use this for tags not found in the standard corpus. Separate multiple tags with commas.",
            key=f"custom_tag_input{key_suffix}"
        )
        
        if custom_tag_input.strip():
            # Split by comma and add each tag
            custom_tags = [tag.strip() for tag in custom_tag_input.split(',') if tag.strip()]
            for custom_tag in custom_tags:
                if custom_tag not in selected_tags:
                    selected_tags.append(custom_tag)
    
    # Tags explanation box
    tags_explanation = st.text_area(
        "📝 Theme Explanation",
        placeholder="Select 1–2 lines, phrases, or words that influenced your choice, and write one short sentence explaining it.",
        height=80,
        key=f"tags_explanation{key_suffix}",
        help="Provide additional context or reasoning for your tag choices"
    )
    
    if selected_tags:
        st.info(f"✅ Selected {len(selected_tags)} tags: {', '.join(selected_tags[:5])}{'...' if len(selected_tags) > 5 else ''}")
    else:
        st.info("No tags selected yet")
    
    st.subheader("🎭 Mood Tags")
    selected_moods = []
    
    default_moods = []
    if existing_record and hasattr(existing_record, 'moods') and existing_record.moods:
        default_moods = existing_record.moods
    
    # Use the same dynamic key suffix for moods
    mood_key_suffix = key_suffix
    
    num_columns = st.session_state.tag_columns
    mood_rows = [MOOD_OPTIONS[i:i+num_columns] for i in range(0, len(MOOD_OPTIONS), num_columns)]
    for mood_row in mood_rows:
        cols = st.columns(num_columns)
        for col_idx, mood in enumerate(mood_row):
            with cols[col_idx]:
                is_default_selected = mood in default_moods
                mood_checkbox_key = f"mood_{mood}{mood_key_suffix}"
                if st.checkbox(mood.capitalize(), value=is_default_selected, key=mood_checkbox_key):
                    selected_moods.append(mood)
    
    # Custom mood tags section
    with st.expander("🔍 Add Custom Mood Tags"):
        custom_mood_input = st.text_input(
            "Add custom mood:",
            placeholder="Enter moods separated by commas (e.g., melancholy, euphoria, nostalgia)...",
            help="Use this for mood tags not found in the standard options. Separate multiple moods with commas.",
            key=f"custom_mood_input{key_suffix}"
        )
        
        if custom_mood_input.strip():
            # Split by comma and add each mood
            custom_moods = [mood.strip() for mood in custom_mood_input.split(',') if mood.strip()]
            for custom_mood in custom_moods:
                if custom_mood not in selected_moods:
                    selected_moods.append(custom_mood)
    
    # Moods explanation box
    moods_explanation = st.text_area(
        "📝 Moods Explanation",
        placeholder="Select 1–2 lines, phrases, or words that influenced your choice, and write one short sentence explaining it.",
        height=80,
        key=f"moods_explanation{key_suffix}",
        help="Provide additional context or reasoning for your mood choices"
    )
    
    # Display mood selection summary
    if selected_moods:
        st.info(f"✅ Selected {len(selected_moods)} moods: {', '.join(selected_moods[:5])}{'...' if len(selected_moods) > 5 else ''}")
    else:
        st.info("No moods selected yet")
    
    render_sentiment_2d()
    
    # Notes input outside of form
    notes = st.text_area(
        "Notes",
        value=default_notes,
        height=100,
        key=f"notes_input{key_suffix}"
    )
    
    # Submit button that calls the submission function
    if st.button("💾 Save", type="primary"):
        submit_annotation(selected_tags, selected_moods, notes, tags_explanation, moods_explanation, current_url)


def submit_annotation(selected_tags, selected_moods, notes, tags_explanation, moods_explanation, current_url):
    """
    Submit annotation with validation and data processing.
    
    Args:
        selected_tags: List of selected tags
        selected_moods: List of selected moods  
        notes: Notes text
        tags_explanation: Explanation for tag selections
        moods_explanation: Explanation for mood selections
        current_url: Current poem URL
    """
    if not st.session_state.coder_id.strip():
        st.error("Please enter a Coder ID first")
        return
    
    all_tags = selected_tags.copy()
    custom_tag_keys = [key for key in st.session_state.keys() if 'custom_tag_input' in key]
    for key in custom_tag_keys:
        custom_tag_input = st.session_state.get(key, "")
        if custom_tag_input and custom_tag_input.strip():
            custom_tags = [tag.strip() for tag in custom_tag_input.split(',') if tag.strip()]
            for custom_tag in custom_tags:
                if custom_tag not in all_tags:
                    all_tags.append(custom_tag)
    
    all_moods = selected_moods.copy()
    custom_mood_keys = [key for key in st.session_state.keys() if 'custom_mood_input' in key]
    for key in custom_mood_keys:
        custom_mood_input = st.session_state.get(key, "")
        if custom_mood_input and custom_mood_input.strip():
            custom_moods = [mood.strip() for mood in custom_mood_input.split(',') if mood.strip()]
            for custom_mood in custom_moods:
                if custom_mood not in all_moods:
                    all_moods.append(custom_mood)
    
    if not all_tags:
        st.error("❌ Required: at least one tag must be selected")
        return
    if not all_moods:
        st.error("❌ Required: at least one mood must be selected")
        return
    
    current_csv_row = st.session_state.poems_df.iloc[st.session_state.current_index]
    html_content = st.session_state.current_poem_text.raw_html if st.session_state.current_poem_text else ""
    time_spent = stop_timer()
    
    record = CodingRecord(
        timestamp_iso=datetime.now().isoformat(),
        coder_id=st.session_state.coder_id.strip(),
        url=current_url,
        poem_uuid=st.session_state.current_poem_meta.poem_uuid,
        title=st.session_state.current_poem_meta.title,
        author=st.session_state.current_poem_meta.author,
        year=str(current_csv_row.get('year', '')) if pd.notna(current_csv_row.get('year')) else None,
        group=str(current_csv_row.get('group', '')) if pd.notna(current_csv_row.get('group')) else None,
        author_url=str(current_csv_row.get('author_url', '')) if pd.notna(current_csv_row.get('author_url')) else None,
        tags=all_tags,
        moods=all_moods,
        sentiment_x=st.session_state.sentiment_x,
        sentiment_y=st.session_state.sentiment_y,
        notes=notes.strip(),
        tags_explanation=tags_explanation.strip() if tags_explanation else None,
        moods_explanation=moods_explanation.strip() if moods_explanation else None,
        is_complete=True,
        html_sha1=sha1(html_content),
        extraction_ok=st.session_state.extraction_error is None,
        error=st.session_state.extraction_error,
        time_spent_seconds=time_spent
    )
    
    try:
        save_record(record)
        st.success("✅ Saved successfully!")
        
        st.session_state.form_version += 1
        st.session_state.sentiment_x = 0.0
        st.session_state.sentiment_y = 0.0
        
        if 'coords_loaded_for_url' in st.session_state:
            del st.session_state['coords_loaded_for_url']
        
        st.session_state.just_saved_and_reset = True
        
        if st.session_state.current_index < len(st.session_state.poems_df) - 1:
            time.sleep(1)
            st.session_state.current_index += 1
            fetch_and_parse_current_poem()
            st.rerun()
        else:
            st.rerun()
            
    except Exception as e:
        st.error(f"Save error: {str(e)}")


def download_all_annotations():
    """Download all annotation files as a ZIP archive."""
    from firebase_storage import list_files, download_json
    
    try:
        st.info("🔍 Scanning annotation files...")
        
        # Get all annotation files
        annotation_files = list_files("annotation/")
        
        if not annotation_files:
            st.warning("No annotation files found.")
            return None
        
        st.info(f"Found {len(annotation_files)} annotation files")
        
        # Create ZIP file in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            progress_bar = st.progress(0)
            total_files = len(annotation_files)
            
            for i, file_path in enumerate(annotation_files):
                try:
                    # Download the annotation data
                    annotation_data = download_json(file_path)
                    
                    if annotation_data:
                        # Convert to JSON string
                        json_content = json.dumps(annotation_data, indent=2, ensure_ascii=False)
                        
                        # Add to ZIP with proper path structure
                        zip_file.writestr(f"{file_path}.json", json_content)
                    
                    # Update progress
                    progress_bar.progress((i + 1) / total_files)
                    
                except Exception as e:
                    st.warning(f"Failed to download {file_path}: {str(e)}")
                    continue
        
        zip_buffer.seek(0)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"all_annotations_{timestamp}.zip"
        
        st.success(f"✅ Successfully created ZIP archive with {len(annotation_files)} files")
        
        return zip_buffer.getvalue(), filename
        
    except Exception as e:
        st.error(f"Error creating annotation archive: {str(e)}")
        return None


def show_annotation_interface():
    """Show the annotation interface for annotators."""
    
    # Check if we need to scroll to top after saving - do this FIRST
    if st.session_state.get('scroll_to_top', False):
        # Inject aggressive scroll-to-top script
        st.markdown("""
        <script>
            // Immediate execution
            (function scrollToTop() {
                // Try all possible scroll methods
                try { window.scrollTo(0, 0); } catch(e) {}
                try { window.parent.scrollTo(0, 0); } catch(e) {}
                try { document.documentElement.scrollTop = 0; } catch(e) {}
                try { document.body.scrollTop = 0; } catch(e) {}
                
                // Delayed attempts
                setTimeout(function() {
                    try { window.scrollTo(0, 0); } catch(e) {}
                    try { window.parent.scrollTo(0, 0); } catch(e) {}
                }, 100);
                
                setTimeout(function() {
                    try { window.parent.scrollTo(0, 0); } catch(e) {}
                }, 300);
            })();
        </script>
        """, unsafe_allow_html=True)
        # Clear the flag
        st.session_state.scroll_to_top = False
    
    # Render workset-specific sidebar
    render_workset_sidebar()
    
    st.title("📝 Poetry Annotation System")
    
    # Use username as coder_id for workset management
    username = st.session_state.username
    
    # Initialize workset manager
    if 'workset_manager' not in st.session_state:
        st.session_state.workset_manager = get_annotator_manager(username)
    
    manager = st.session_state.workset_manager
    
    # Get current workset
    if 'current_workset_info' not in st.session_state:
        st.session_state.current_workset_info = manager.get_current_workset()
    
    current_workset = st.session_state.current_workset_info
    
    if not current_workset:
        # Check if user has no record file (new user) or completed all worksets
        if manager._has_no_record_file():
            # New user with no record file - show request interface
            show_completed_worksets_interface(username)
        else:
            # User has record file but no available worksets - show request interface for completed users
            show_completed_worksets_interface(username)
        return
    
    # Get next poem to annotate
    if 'current_poem_info' not in st.session_state:
        st.session_state.current_poem_info = manager.get_next_poem(current_workset['workset'])
        # Record annotation start time for timing tracking
        if st.session_state.current_poem_info:
            st.session_state.annotation_start_time = time.time()
    
    current_poem = st.session_state.current_poem_info
    
    # Ensure annotation timing is initialized for current poem
    if current_poem and 'annotation_start_time' not in st.session_state:
        st.session_state.annotation_start_time = time.time()
    
    if not current_poem:
        # Check if this is a workset completion or all worksets completed
        next_workset = manager.get_current_workset()
        if next_workset:
            # Move to next workset
            st.session_state.current_workset_info = next_workset
            st.session_state.current_poem_info = manager.get_next_poem(next_workset['workset'])
            # Record annotation start time for new workset
            if st.session_state.current_poem_info:
                st.session_state.annotation_start_time = time.time()
        else:
            # All worksets completed - show request new task option
            show_completed_worksets_interface(username)
            return
        st.rerun()
        return
    
    # Workset progress display removed per user request
    st.divider()
    
    # Show current poem info
    st.subheader(f"Poem {current_poem['current_poem_number']}/{current_poem['total_poems']}")
    
    # Create two-column layout: poem on left, annotation panel on right
    col1, col2 = st.columns([2, 1])  # 2:1 ratio - poem takes more space
    
    with col1:
        st.markdown("### 📖 Poem")
        render_workset_poem(current_poem)
    
    with col2:
        st.markdown("### 🏷️ Annotation")
        render_workset_annotation_panel(current_poem, manager)

def render_workset_poem(poem_info: dict):
    """Render poem content from workset."""
    try:
        poem_url = poem_info['poem_url']
        
        # Show poem metadata
        st.markdown(f"**Title:** {poem_info.get('title', 'Unknown')}")
        st.markdown(f"**Author:** {poem_info.get('author', 'Unknown')}")
        # st.markdown(f"**URL:** [View Original]({poem_url})")
        
        # Fetch and parse poem
        with st.spinner("Loading poem..."):
            html = fetch_html(poem_url)
            meta, text = parse_poem(html, poem_url)
            
            if text and text.text:
                st.subheader("Poem Text")
                st.code(text.text, language=None)
                
                # Store parsed content for annotation
                st.session_state.current_poem_meta = meta
                st.session_state.current_poem_text = text
                st.session_state.extraction_error = None
            else:
                st.error("Could not extract poem text")
                st.session_state.extraction_error = "Failed to extract poem text"
                
    except Exception as e:
        st.error(f"Error loading poem: {str(e)}")
        st.session_state.extraction_error = str(e)

def render_workset_annotation_panel(poem_info: dict, manager):
    """Render annotation panel for workset-based annotation."""
    
    # Apply custom styling
    apply_tag_style()
    
    # Use dynamic key suffix for workset
    if 'workset_form_version' not in st.session_state:
        st.session_state.workset_form_version = 0
    workset_key_suffix = f"_v{st.session_state.workset_form_version}"
    
    num_columns = 4
    
    st.subheader("Emotion")
    st.markdown("#### Choose the most dominant emotions this poem convey.")
    selected_moods = []
    
    mood_rows = [MOOD_OPTIONS[i:i+num_columns] for i in range(0, len(MOOD_OPTIONS), num_columns)]
    for mood_row in mood_rows:
        cols = st.columns(num_columns)
        for col_idx, mood in enumerate(mood_row):
            with cols[col_idx]:
                if st.checkbox(mood.capitalize(), key=f"workset_mood_{mood}{workset_key_suffix}"):
                    selected_moods.append(mood)
    
    # Custom mood tags
    with st.expander("🔍 Add Custom Emotion Tags"):
        custom_mood_input = st.text_input(
            "Add custom mood:",
            placeholder="Enter moods separated by commas...",
            key=f"workset_custom_mood_input{workset_key_suffix}"
        )
        
        if custom_mood_input.strip():
            custom_moods = [mood.strip() for mood in custom_mood_input.split(',') if mood.strip()]
            for custom_mood in custom_moods:
                if custom_mood not in selected_moods:
                    selected_moods.append(custom_mood)
    
    # Moods explanation box
    moods_explanation = st.text_area(
        "📝 Moods Explanation (Required)",
        placeholder="Select 1–2 lines, phrases, or words that influenced your choice, and write one short sentence explaining it.",
        height=80,
        key=f"workset_moods_explanation{workset_key_suffix}",
        help="Required: Provide additional context or reasoning for your mood choices"
    )
    
    # Black divider line
    st.markdown("<hr style='border: 1px solid black; margin: 20px 0;'>", unsafe_allow_html=True)
    
    if 'sentiment_x' not in st.session_state:
        st.session_state.sentiment_x = 0.0
    if 'sentiment_y' not in st.session_state:
        st.session_state.sentiment_y = 0.0
    
    render_sentiment_2d()
    
    # Black divider line
    st.markdown("<hr style='border: 1px solid black; margin: 20px 0;'>", unsafe_allow_html=True)
    
    st.subheader("Theme Selection")
    
    tag_option = st.radio(
        "Choose tag set:",
        options=["top50"],
        format_func=lambda x: "Top 20 Tags" if x == "top20" else "Top 50 Tags",
        horizontal=True,
        key="workset_tag_option"
    )
    
    display_tags = TOP_20_TAGS if tag_option == "top20" else TOP_50_TAGS
    selected_tags = []
    
    # Display main tags
    for row in range(0, len(display_tags), num_columns):
        cols = st.columns(num_columns)
        for col_idx, tag in enumerate(display_tags[row:row+num_columns]):
            with cols[col_idx]:
                if st.checkbox(tag, key=f"workset_tag_{tag}{workset_key_suffix}"):
                    selected_tags.append(tag)
    
    # Search & Add More Tags
    with st.expander("🔍 Search & Add More Tags"):
        # search_term = st.text_input(
        #     "Search for additional tags:",
        #     placeholder="Type to search through all available tags...",
        #     key=f"workset_search_term{workset_key_suffix}"
        # )
        
        # if search_term:
        #     matching_tags = [tag for tag in ALL_CORPUS_TAGS 
        #                    if search_term.lower() in tag.lower() and tag not in selected_tags and tag not in display_tags]
            
        #     if matching_tags:
        #         st.write(f"Found {len(matching_tags)} additional matching tags:")
        #         search_columns = min(4, len(matching_tags))
        #         cols = st.columns(search_columns)
        #         for i, tag in enumerate(matching_tags[:12]):
        #             with cols[i % search_columns]:
        #                 if st.checkbox(f"{tag}", key=f"workset_search_tag_{tag}{workset_key_suffix}"):
        #                     selected_tags.append(tag)
        #     else:
        #         st.write("No additional matching tags found.")
        
        custom_tag_input = st.text_input(
            "Add custom tag:",
            placeholder="Enter themes separated by commas (e.g., tag1, tag2, tag3)...",
            help="Use this for tags not found in the standard corpus.",
            key=f"workset_custom_tag_input{workset_key_suffix}"
        )
        
        if custom_tag_input.strip():
            custom_tags = [tag.strip() for tag in custom_tag_input.split(',') if tag.strip()]
            for custom_tag in custom_tags:
                if custom_tag not in selected_tags:
                    selected_tags.append(custom_tag)
    
    # Tags explanation box
    tags_explanation = st.text_area(
        "📝 Theme Explanation (Required)",
        placeholder="Select 1–2 lines, phrases, or words that influenced your choice, and write one short sentence explaining it.",
        height=80,
        key=f"workset_tags_explanation{workset_key_suffix}",
        help="Required: Provide additional context or reasoning for your tag choices"
    )
    
    # Black divider line
    st.markdown("<hr style='border: 1px solid black; margin: 20px 0;'>", unsafe_allow_html=True)
    
    # Notes
    notes = st.text_area(
        "Notes",
        height=100,
        placeholder="Add any additional observations about this poem...",
        key=f"workset_annotation_notes{workset_key_suffix}"
    )
    
    # Submit button (outside form)
    if st.button("Save Annotation", type="primary", key="workset_submit"):
        submit_workset_annotation(selected_tags, selected_moods, notes, tags_explanation, moods_explanation, poem_info, manager)
        


def submit_workset_annotation(selected_tags, selected_moods, notes, tags_explanation, moods_explanation, poem_info, manager):
    """
    Submit workset annotation with validation and data processing.
    
    Args:
        selected_tags: List of selected tags
        selected_moods: List of selected moods
        notes: Notes text
        tags_explanation: Explanation for tag selections
        moods_explanation: Explanation for mood selections
        poem_info: Poem information dictionary
        manager: Workset manager instance
    """
    all_tags = selected_tags.copy()
    all_moods = selected_moods.copy()
    
    custom_tag_input = st.session_state.get("workset_custom_tag_input", "")
    if custom_tag_input and custom_tag_input.strip():
        custom_tags = [tag.strip() for tag in custom_tag_input.split(',') if tag.strip()]
        for custom_tag in custom_tags:
            if custom_tag not in all_tags:
                all_tags.append(custom_tag)
    
    custom_mood_input = st.session_state.get("workset_custom_mood_input", "")
    if custom_mood_input and custom_mood_input.strip():
        custom_moods = [mood.strip() for mood in custom_mood_input.split(',') if mood.strip()]
        for custom_mood in custom_moods:
            if custom_mood not in all_moods:
                all_moods.append(custom_mood)
    
    validation_errors = []
    
    if not all_tags:
        validation_errors.append("❌ Required: at least one theme must be selected")
    
    if not all_moods:
        validation_errors.append("❌ Required: at least one emotion must be selected")
    
    if not tags_explanation or not tags_explanation.strip():
        validation_errors.append("❌ Required: Theme Explanation cannot be empty")
    
    if not moods_explanation or not moods_explanation.strip():
        validation_errors.append("❌ Required: Emotion Explanation cannot be empty")
    
    if validation_errors:
        for error in validation_errors:
            st.error(error)
        return
    
    annotation_end_time = time.time()
    annotation_start_time = st.session_state.get('annotation_start_time', annotation_end_time)
    annotation_duration = annotation_end_time - annotation_start_time
    
    annotation_data = {
        'poem_url': poem_info['poem_url'],
        'title': poem_info.get('title', ''),
        'author': poem_info.get('author', ''),
        'tags': all_tags,
        'moods': all_moods,
        'sentiment_x': float(st.session_state.sentiment_x),
        'sentiment_y': float(st.session_state.sentiment_y),
        'notes': notes.strip(),
        'tags_explanation': tags_explanation.strip() if tags_explanation else None,
        'moods_explanation': moods_explanation.strip() if moods_explanation else None,
        'poem_meta': st.session_state.get('current_poem_meta', {}).__dict__ if hasattr(st.session_state.get('current_poem_meta', {}), '__dict__') else {},
        'extraction_ok': st.session_state.get('extraction_error') is None,
        'error': st.session_state.get('extraction_error'),
        'timing': {
            'start_time': annotation_start_time,
            'end_time': annotation_end_time,
            'duration_seconds': round(annotation_duration, 2),
            'duration_minutes': round(annotation_duration / 60, 2),
            'start_timestamp': datetime.fromtimestamp(annotation_start_time).isoformat(),
            'end_timestamp': datetime.fromtimestamp(annotation_end_time).isoformat()
        }
    }
    
    if manager.save_annotation(poem_info['workset'], poem_info['row_index'], annotation_data):
        st.success("✅ Annotation saved successfully!")
        
        st.session_state.workset_form_version += 1
        st.session_state.sentiment_x = 0.0
        st.session_state.sentiment_y = 0.0
        
        if 'current_poem_meta' in st.session_state:
            del st.session_state['current_poem_meta']
        if 'current_poem_text' in st.session_state:
            del st.session_state['current_poem_text']
        if 'extraction_error' in st.session_state:
            del st.session_state['extraction_error']
        if 'annotation_start_time' in st.session_state:
            del st.session_state['annotation_start_time']
        
        st.session_state.scroll_to_top = True
        st.session_state.current_poem_info = None
        st.rerun()
    else:
        st.error("❌ Failed to save annotation")


def clear_workset_annotation_session_state(selected_tags=None, selected_moods=None):
    """Clear workset annotation session state variables."""
    keys_to_delete = []
    
    # Clear specific tag keys based on selected_tags
    if selected_tags:
        for tag in selected_tags:
            # Workset tag keys
            tag_key = f"workset_tag_{tag}"
            if tag_key in st.session_state:
                keys_to_delete.append(tag_key)
            
            # Workset search tag keys
            search_key = f"workset_search_tag_{tag}"
            if search_key in st.session_state:
                keys_to_delete.append(search_key)
    
    # Clear specific mood keys based on selected_moods
    if selected_moods:
        for mood in selected_moods:
            mood_key = f"workset_mood_{mood}"
            if mood_key in st.session_state:
                keys_to_delete.append(mood_key)
    
    # Clear all remaining workset-specific keys
    all_session_keys = list(st.session_state.keys())
    for key in all_session_keys:
        if (key.startswith('workset_tag_') or key.startswith('workset_search_tag_') or 
            key.startswith('workset_mood_') or key == 'workset_custom_tag_input' or
            key == 'workset_custom_mood_input' or key == 'workset_annotation_notes' or 
            key == 'workset_tag_option' or key == 'workset_search_term'):
            if key not in keys_to_delete:  # Avoid duplicates
                keys_to_delete.append(key)
    
    # Reset values instead of deleting
    for key in keys_to_delete:
        try:
            if key in st.session_state:
                if key.startswith(('workset_tag_', 'workset_search_tag_', 'workset_mood_')):
                    # For checkboxes, set to False
                    st.session_state[key] = False
                elif 'input' in key or 'notes' in key or 'term' in key:
                    # For text inputs, set to empty string
                    st.session_state[key] = ""
                else:
                    # For other keys, delete them
                    del st.session_state[key]
        except KeyError:
            pass
    
    # Reset sentiment coordinates
    st.session_state.sentiment_x = 0.0
    st.session_state.sentiment_y = 0.0
    
    # Reset timing for next poem (will be set when next poem loads)
    if 'annotation_start_time' in st.session_state:
        del st.session_state['annotation_start_time']
    
    # Clear poem content
    if 'current_poem_meta' in st.session_state:
        del st.session_state['current_poem_meta']
    if 'current_poem_text' in st.session_state:
        del st.session_state['current_poem_text']
    if 'extraction_error' in st.session_state:
        del st.session_state['extraction_error']
    

def show_admin_interface():
    """Show the admin interface."""
    # Add logout button in sidebar
    with st.sidebar:
        st.markdown("---")
        st.markdown(f"**Logged in as:** {st.session_state.user_name}")
        st.markdown(f"**Role:** {st.session_state.user_role.title()}")
        
        st.markdown("---")
        st.markdown("**🚀 Quick Actions**")
        
        # Quick download button
        if st.button("📥 Quick Download All", use_container_width=True):
            with st.spinner("Creating ZIP archive..."):
                result = download_all_annotations()
                
                if result:
                    zip_data, filename = result
                    
                    st.download_button(
                        label="💾 Download ZIP",
                        data=zip_data,
                        file_name=filename,
                        mime="application/zip",
                        use_container_width=True
                    )
        
        st.markdown("---")
        if st.button("🚪 Logout"):
            # Clear session state
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    # Call admin page directly
    admin_page()

def main():
    """Main application function with login system."""
    initialize_session_state()
    
    # Check if user is logged in
    if not st.session_state.logged_in:
        show_login_page()
        return
    
    # Route based on user role
    if st.session_state.user_role == "admin":
        show_admin_interface()
    elif st.session_state.user_role == "annotator":
        show_annotation_interface()
    else:
        st.error("Invalid user role. Please contact administrator.")
        if st.button("Logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


def show_completed_worksets_interface(username: str):
    """Show interface when all worksets are completed with option to request new tasks."""
    from workset_auto_assigner import WorksetAutoAssigner, cleanup_expired_locks
    
    # Clean up expired locks on page load
    cleanup_expired_locks()
    
    # Check if this is a new user or completed user
    from annotator_workset_manager import get_annotator_manager
    manager = get_annotator_manager(username)
    
    if manager._has_no_record_file():
        # New user
        st.info("👋 Welcome! You haven't been assigned any worksets yet.")
        
        # Show welcome message
        st.markdown("""
        ### 🚀 Start Poetry Annotation Work
        
        You're ready to start annotating poetry! You can request a workset from the available pool to begin your annotation work.
        """)
    else:
        # Completed user
        st.success("🎊 All your worksets are completed!")
        
        # Show completion message
        st.markdown("""
        ### 🎉 Congratulations!
        You have successfully completed all assigned worksets.
        """)
    
    # Initialize auto assigner
    assigner = WorksetAutoAssigner()
    
    # Show usage summary
    with st.expander("📊 Workset Usage Status", expanded=False):
        usage_summary = assigner.get_usage_summary()
        if usage_summary:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Worksets", usage_summary.get('total_worksets', 0))
            with col2:
                st.metric("Available", usage_summary.get('available_worksets', 0))
            with col3:
                st.metric("Fully Used", usage_summary.get('fully_used_worksets', 0))
            with col4:
                st.metric("Unused", usage_summary.get('unused_worksets', 0))
    
    st.divider()
    
    # Request new task section
    st.markdown("### 🚀 Request New Task")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        **Ready to do more work?**
        
        You can request a new workset from the available pool. Each workset can be assigned to a maximum of 3 annotators to ensure quality and consistency.
        
        Click the button below to automatically get the next available workset.
        """)
    
    with col2:
        if st.button("🎯 Request New Workset", type="primary", use_container_width=True):
            with st.spinner("🔍 Looking for available worksets..."):
                try:
                    new_workset = assigner.request_new_workset(username)
                    
                    if new_workset:
                        # Clear session state to refresh the interface
                        if 'current_workset_info' in st.session_state:
                            del st.session_state['current_workset_info']
                        if 'current_poem_info' in st.session_state:
                            del st.session_state['current_poem_info']
                        
                        st.success(f"🎉 Successfully assigned new workset: **{new_workset}**")
                        st.info("Refreshing interface...")
                        time.sleep(2)
                        st.rerun()
                    else:
                        st.info("No worksets are currently available. All worksets have reached their maximum usage limit (3 times each).")
                        
                except Exception as e:
                    st.error(f"Failed to request new workset: {str(e)}")
    
    st.divider()
    
    # Alternative options
    st.markdown("### 📞 Other Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        **Contact Administrator:**
        - Request specific worksets
        - Report issues or feedback
        - Get additional assignments
        """)
    
    with col2:
        st.markdown("""
        **Your Progress:**
        - All annotations are automatically saved
        - Progress tracked in real-time
        - Thank you for your contribution!
        """)
    
    # Logout option
    st.divider()
    if st.button("🚪 Logout", use_container_width=False):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()


if __name__ == "__main__":
    main()
