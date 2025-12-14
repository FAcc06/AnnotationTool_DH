"""Firebase Storage integration for uploading and downloading files."""

import os
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import firebase_admin
from firebase_admin import credentials, storage
from datetime import datetime
import pandas as pd
import io
import hashlib
import numpy as np

# Try to import streamlit, but make it optional
try:
    import streamlit as st
    HAS_STREAMLIT = True
except ImportError:
    HAS_STREAMLIT = False
    # Create a dummy st object for non-Streamlit environments
    class DummySt:
        def error(self, msg):
            print(f"[ERROR] {msg}")
        def warning(self, msg):
            print(f"[WARNING] {msg}")
        def info(self, msg):
            print(f"[INFO] {msg}")
        def success(self, msg):
            print(f"[SUCCESS] {msg}")
        class session_state:
            @staticmethod
            def get(key, default=None):
                return default
    st = DummySt()


def _json_serializer(obj):
    """Custom JSON serializer for numpy types and other non-serializable objects."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class FirebaseStorage:
    """Firebase Storage handler for file operations."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize Firebase connection.
        
        Args:
            config: Firebase configuration dictionary. If None, will look for environment variables.
        """
        self.initialized = False
        self.bucket = None
        
        try:
            # Check if Firebase Admin is already initialized
            if not firebase_admin._apps:
                if config:
                    # Use provided config
                    self._init_with_config(config)
                else:
                    # Try to initialize from environment or file
                    self._init_from_env()
            else:
                # Already initialized, get the bucket
                self.bucket = storage.bucket()
                self.initialized = True
                
        except Exception as e:
            st.error(f"Failed to initialize Firebase: {str(e)}")
            self.initialized = False
    
    def _init_with_config(self, config: Dict[str, Any]):
        """Initialize Firebase with provided configuration."""
        try:
            # For Firebase Admin SDK
            if 'service_account' in config:
                cred = credentials.Certificate(config['service_account'])
                firebase_admin.initialize_app(cred, {
                    'storageBucket': config.get('storageBucket', '')
                })
            else:
                # Use default credentials
                cred = credentials.Certificate(config)
                firebase_admin.initialize_app(cred, {
                    'storageBucket': config.get('storageBucket', '')
                })
            
            self.bucket = storage.bucket()
            
            
            self.initialized = True
            
        except Exception as e:
            raise Exception(f"Failed to initialize with config: {str(e)}")
    
    def _init_from_env(self):
        """Initialize Firebase from environment variables or config file."""
        # Try to load from firebase_config.json file
        config_file = Path("firebase_config.json")
        if config_file.exists():
            with open(config_file, 'r') as f:
                config = json.load(f)
                self._init_with_config(config)
        else:
            # Try environment variables
            import os
            config = {
                'type': os.getenv('FIREBASE_TYPE', 'service_account'),
                'project_id': os.getenv('FIREBASE_PROJECT_ID'),
                'private_key_id': os.getenv('FIREBASE_PRIVATE_KEY_ID'),
                'private_key': os.getenv('FIREBASE_PRIVATE_KEY', '').replace('\\n', '\n'),
                'client_email': os.getenv('FIREBASE_CLIENT_EMAIL'),
                'client_id': os.getenv('FIREBASE_CLIENT_ID'),
                'auth_uri': os.getenv('FIREBASE_AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
                'token_uri': os.getenv('FIREBASE_TOKEN_URI', 'https://oauth2.googleapis.com/token'),
                'auth_provider_x509_cert_url': os.getenv('FIREBASE_AUTH_PROVIDER_CERT_URL', 'https://www.googleapis.com/oauth2/v1/certs'),
                'client_x509_cert_url': os.getenv('FIREBASE_CLIENT_CERT_URL'),
                'storageBucket': os.getenv('FIREBASE_STORAGE_BUCKET')
            }
            
            if config['project_id']:
                self._init_with_config(config)
            else:
                raise Exception("No Firebase configuration found. Please provide config or set environment variables.")
    
    def upload(self, firebase_path: str, file_content: Union[bytes, str, Dict], 
               content_type: str = None) -> bool:
        """
        Upload a file to Firebase Storage.
        
        Args:
            firebase_path: Path in Firebase Storage (e.g., 'poems/coder1/data.json')
            file_content: File content as bytes, string, or dictionary (will be JSON serialized)
            content_type: MIME type of the file (e.g., 'application/json', 'text/plain')
        
        Returns:
            True if upload successful, False otherwise
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return False
        
        try:
            blob = self.bucket.blob(firebase_path)
            
            # Handle different content types
            if isinstance(file_content, dict):
                # Convert dict to JSON string with custom encoder for numpy types
                try:
                    content = json.dumps(file_content, ensure_ascii=False, indent=2, default=_json_serializer)
                except TypeError as e:
                    st.error(f"JSON serialization error: {str(e)}")
                    st.error(f"Problematic data: {file_content}")
                    return False
                content_type = content_type or 'application/json'
                blob.upload_from_string(content, content_type=content_type)
            elif isinstance(file_content, str):
                # Upload string content
                content_type = content_type or 'text/plain'
                blob.upload_from_string(file_content, content_type=content_type)
            elif isinstance(file_content, bytes):
                # Upload bytes
                blob.upload_from_string(file_content, content_type=content_type)
            else:
                raise ValueError(f"Unsupported content type: {type(file_content)}")
            
            # Set metadata
            blob.metadata = {
                'uploaded_at': datetime.now().isoformat(),
                'uploaded_by': st.session_state.get('coder_id', 'unknown')
            }
            blob.patch()
            
            return True
            
        except Exception as e:
            st.error(f"Upload failed: {str(e)}")
            return False
    
    def upload_file(self, firebase_path: str, local_file_path: str) -> bool:
        """
        Upload a local file to Firebase Storage.
        
        Args:
            firebase_path: Path in Firebase Storage
            local_file_path: Path to local file
        
        Returns:
            True if upload successful, False otherwise
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return False
        
        try:
            blob = self.bucket.blob(firebase_path)
            blob.upload_from_filename(local_file_path)
            
            # Set metadata
            blob.metadata = {
                'uploaded_at': datetime.now().isoformat(),
                'uploaded_by': st.session_state.get('coder_id', 'unknown'),
                'original_filename': os.path.basename(local_file_path)
            }
            blob.patch()
            
            return True
            
        except Exception as e:
            st.error(f"Upload failed: {str(e)}")
            return False
    
    def download(self, firebase_path: str) -> Optional[bytes]:
        """
        Download a single file from Firebase Storage.
        
        Args:
            firebase_path: Path to file in Firebase Storage
        
        Returns:
            File content as bytes, or None if download failed
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return None
        
        try:
            blob = self.bucket.blob(firebase_path)
            if blob.exists():
                return blob.download_as_bytes()
            else:
                st.warning(f"File not found: {firebase_path}")
                return None
                
        except Exception as e:
            st.error(f"Download failed: {str(e)}")
            return None
    
    def download_as_string(self, firebase_path: str) -> Optional[str]:
        """
        Download a file from Firebase Storage as string.
        
        Args:
            firebase_path: Path to file in Firebase Storage
        
        Returns:
            File content as string, or None if download failed
        """
        content = self.download(firebase_path)
        if content:
            return content.decode('utf-8')
        return None
    
    def download_as_json(self, firebase_path: str) -> Optional[Dict]:
        """
        Download a JSON file from Firebase Storage.
        
        Args:
            firebase_path: Path to JSON file in Firebase Storage
        
        Returns:
            Parsed JSON as dictionary, or None if download failed
        """
        content = self.download_as_string(firebase_path)
        if content:
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                st.error(f"Failed to parse JSON: {str(e)}")
                return None
        return None
    
    def download_folder(self, folder_path: str, local_dir: str = None) -> List[Dict[str, Any]]:
        """
        Download all files from a folder in Firebase Storage.
        
        Args:
            folder_path: Path to folder in Firebase Storage (e.g., 'poems/coder1/')
            local_dir: Optional local directory to save files. If None, returns file data in memory.
        
        Returns:
            List of dictionaries containing file info and content
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return []
        
        try:
            # Ensure folder path ends with /
            if not folder_path.endswith('/'):
                folder_path += '/'
            
            # List all blobs in the folder
            blobs = self.bucket.list_blobs(prefix=folder_path)
            
            downloaded_files = []
            
            for blob in blobs:
                # Skip folder markers
                if blob.name.endswith('/'):
                    continue
                
                file_info = {
                    'path': blob.name,
                    'name': os.path.basename(blob.name),
                    'size': blob.size,
                    'content_type': blob.content_type,
                    'created': blob.time_created,
                    'updated': blob.updated,
                    'metadata': blob.metadata
                }
                
                # Download content
                content = blob.download_as_bytes()
                
                if local_dir:
                    # Save to local directory
                    local_path = os.path.join(local_dir, os.path.basename(blob.name))
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)
                    with open(local_path, 'wb') as f:
                        f.write(content)
                    file_info['local_path'] = local_path
                else:
                    # Keep content in memory
                    file_info['content'] = content
                    # Try to decode as string if it's text
                    try:
                        file_info['content_string'] = content.decode('utf-8')
                    except:
                        pass
                
                downloaded_files.append(file_info)
            
            return downloaded_files
            
        except Exception as e:
            st.error(f"Download folder failed: {str(e)}")
            return []
    
    def list_files(self, folder_path: str = "") -> List[str]:
        """
        List all files in a folder.
        
        Args:
            folder_path: Path to folder in Firebase Storage
        
        Returns:
            List of file paths
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return []
        
        try:
            # Ensure folder path ends with / if not empty
            if folder_path and not folder_path.endswith('/'):
                folder_path += '/'
            
            blobs = self.bucket.list_blobs(prefix=folder_path)
            return [blob.name for blob in blobs if not blob.name.endswith('/')]
            
        except Exception as e:
            st.error(f"List files failed: {str(e)}")
            return []
    
    def delete(self, firebase_path: str) -> bool:
        """
        Delete a file from Firebase Storage.
        
        Args:
            firebase_path: Path to file in Firebase Storage
        
        Returns:
            True if deletion successful, False otherwise
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return False
        
        try:
            blob = self.bucket.blob(firebase_path)
            blob.delete()
            return True
            
        except Exception as e:
            st.error(f"Delete failed: {str(e)}")
            return False
    
    def get_download_url(self, firebase_path: str, expiration_hours: int = 1) -> Optional[str]:
        """
        Get a temporary download URL for a file.
        
        Args:
            firebase_path: Path to file in Firebase Storage
            expiration_hours: Hours until URL expires
        
        Returns:
            Download URL or None if failed
        """
        if not self.initialized:
            st.error("Firebase not initialized")
            return None
        
        try:
            blob = self.bucket.blob(firebase_path)
            if blob.exists():
                from datetime import timedelta
                url = blob.generate_signed_url(
                    version="v4",
                    expiration=timedelta(hours=expiration_hours),
                    method="GET"
                )
                return url
            else:
                st.warning(f"File not found: {firebase_path}")
                return None
                
        except Exception as e:
            st.error(f"Failed to generate URL: {str(e)}")
            return None


# Convenience functions for direct use
_firebase_instance = None

def get_firebase_storage(config: Dict[str, Any] = None) -> FirebaseStorage:
    """Get or create Firebase Storage instance."""
    global _firebase_instance
    if _firebase_instance is None:
        _firebase_instance = FirebaseStorage(config)
    return _firebase_instance

def upload(path: str, content: Union[bytes, str, Dict], content_type: str = None) -> bool:
    """
    Upload content to Firebase Storage.
    
    Args:
        path: Path in Firebase Storage
        content: Content to upload (bytes, string, or dictionary)
        content_type: MIME type of the content
    
    Returns:
        True if successful, False otherwise
    """
    fb = get_firebase_storage()
    return fb.upload(path, content, content_type)

def upload_file(path: str, local_file: str) -> bool:
    """
    Upload a local file to Firebase Storage.
    
    Args:
        path: Path in Firebase Storage
        local_file: Path to local file
    
    Returns:
        True if successful, False otherwise
    """
    fb = get_firebase_storage()
    return fb.upload_file(path, local_file)

def download(path: str) -> Optional[Union[bytes, List[Dict[str, Any]]]]:
    """
    Download a file or folder from Firebase Storage.
    
    Args:
        path: Path to file or folder in Firebase Storage
    
    Returns:
        - If path is a file: file content as bytes
        - If path is a folder (ends with /): list of file info dictionaries
        - None if download failed
    """
    fb = get_firebase_storage()
    
    # Check if it's a folder request
    if path.endswith('/'):
        return fb.download_folder(path)
    else:
        return fb.download(path)

def download_json(path: str) -> Optional[Dict]:
    """
    Download and parse a JSON file from Firebase Storage.
    
    Args:
        path: Path to JSON file in Firebase Storage
    
    Returns:
        Parsed JSON as dictionary, or None if failed
    """
    fb = get_firebase_storage()
    return fb.download_as_json(path)

def list_files(folder: str = "") -> List[str]:
    """
    List all files in a Firebase Storage folder.
    
    Args:
        folder: Folder path in Firebase Storage
    
    Returns:
        List of file paths
    """
    fb = get_firebase_storage()
    return fb.list_files(folder)

def delete_file(path: str) -> bool:
    """
    Delete a file from Firebase Storage.
    
    Args:
        path: Path to file in Firebase Storage
    
    Returns:
        True if successful, False otherwise
    """
    fb = get_firebase_storage()
    return fb.delete(path)

def get_download_url(path: str, hours: int = 1) -> Optional[str]:
    """
    Get a temporary download URL for a file.
    
    Args:
        path: Path to file in Firebase Storage
        hours: Hours until URL expires
    
    Returns:
        Download URL or None if failed
    """
    fb = get_firebase_storage()
    return fb.get_download_url(path, hours)

# CSV Helper Functions for Admin
def download_csv(path: str) -> Optional[pd.DataFrame]:
    """
    Download and parse a CSV file from Firebase Storage.
    
    Args:
        path: Path to CSV file in Firebase Storage
    
    Returns:
        DataFrame or None if failed
    """
    content = download(path)
    if content:
        try:
            return pd.read_csv(io.BytesIO(content))
        except Exception as e:
            if HAS_STREAMLIT:
                st.error(f"Failed to parse CSV: {str(e)}")
            else:
                print(f"Failed to parse CSV: {str(e)}")
            return None
    return None

def upload_csv(path: str, df: pd.DataFrame) -> bool:
    """
    Upload a DataFrame as CSV to Firebase Storage.
    
    Args:
        path: Path in Firebase Storage
        df: DataFrame to upload
    
    Returns:
        True if successful, False otherwise
    """
    try:
        csv_content = df.to_csv(index=False, encoding='utf-8')
        return upload(path, csv_content, 'text/csv')
    except Exception as e:
        if HAS_STREAMLIT:
            st.error(f"Failed to upload CSV: {str(e)}")
        else:
            print(f"Failed to upload CSV: {str(e)}")
        return False

def create_empty_csv(path: str, columns: List[str]) -> bool:
    """
    Create an empty CSV file with specified columns.
    
    Args:
        path: Path in Firebase Storage
        columns: List of column names
    
    Returns:
        True if successful, False otherwise
    """
    df = pd.DataFrame(columns=columns)
    return upload_csv(path, df)

def hash_password(password: str) -> str:
    """
    Hash a password using SHA-256.
    
    Args:
        password: Plain text password
    
    Returns:
        Hashed password
    """
    return hashlib.sha256(password.encode()).hexdigest()
