"""
Annotator Workset Manager
Manages workset progression and poem assignment for annotators.
"""

from firebase_storage import download_csv, upload_csv, upload
import pandas as pd
from typing import Optional, Dict, Tuple
import streamlit as st
import json
from datetime import datetime
import numpy as np

def make_json_serializable(obj):
    """Convert numpy types and other non-serializable types to JSON-serializable types."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: make_json_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [make_json_serializable(item) for item in obj]
    else:
        return obj

class AnnotatorWorksetManager:
    """Manages workset progression for annotators."""
    
    def __init__(self, username: str):
        """Initialize the workset manager for a specific user."""
        self.username = username
        self.record_path = f"annotators/{username}/{username}_record.csv"
        
    def get_current_workset(self) -> Optional[Dict]:
        """Get the current working workset or start a new one."""
        try:
            # Load user record
            record_df = download_csv(self.record_path)
            if record_df is None or record_df.empty:
                # Don't show error here, let the caller handle it
                return None
            
            # Check for existing in_progress workset
            in_progress_worksets = record_df[record_df['status'] == 'in_progress']
            
            if not in_progress_worksets.empty:
                # Return first in_progress workset
                workset_row = in_progress_worksets.iloc[0]
                st.info(f"Continuing workset: {workset_row['workset']}")
                return {
                    'workset': workset_row['workset'],
                    'status': 'in_progress',
                    'row_index': in_progress_worksets.index[0]
                }
            
            # No in_progress workset, find first not_started
            not_started_worksets = record_df[record_df['status'] == 'not_started']
            
            if not not_started_worksets.empty:
                # Start first not_started workset
                workset_row = not_started_worksets.iloc[0]
                row_index = not_started_worksets.index[0]
                
                # Update status to in_progress
                record_df.loc[row_index, 'status'] = 'in_progress'
                
                # Save updated record
                if upload_csv(self.record_path, record_df):
                    st.success(f"Started new workset: {workset_row['workset']}")
                    return {
                        'workset': workset_row['workset'],
                        'status': 'in_progress',
                        'row_index': row_index
                    }
                else:
                    st.error("Failed to update workset status")
                    return None
            
            # No worksets available
            st.info("No worksets available to work on")
            return None
            
        except Exception as e:
            st.error(f"Error getting current workset: {str(e)}")
            return None
    
    def get_next_poem(self, workset: str) -> Optional[Dict]:
        """Get the next poem to annotate from the workset."""
        try:
            # Load workset file
            workset_path = f"coding_result/{self.username}/{workset}.csv"
            workset_df = download_csv(workset_path)
            
            if workset_df is None:
                st.error(f"Workset file not found: {workset}")
                return None
            
            # Find first poem with Progress = 'N' (not started)
            not_started = workset_df[workset_df['Progress'] == 'N']
            
            if not not_started.empty:
                poem_row = not_started.iloc[0]
                row_index = not_started.index[0]
                
                return {
                    'row_index': row_index,
                    'poem_url': poem_row['poem_url'],
                    'title': poem_row.get('title', 'Unknown Title'),
                    'author': poem_row.get('author', 'Unknown Author'),
                    'workset': workset,
                    'total_poems': len(workset_df),
                    'completed_poems': len(workset_df[workset_df['Progress'] == 'Y']),
                    'current_poem_number': row_index + 1
                }
            
            # All poems completed in this workset
            if self._complete_workset(workset):
                # Try to get next workset
                return self._get_next_workset()
            return None
            
        except Exception as e:
            st.error(f"Error getting next poem: {str(e)}")
            return None
    
    def _complete_workset(self, workset: str) -> bool:
        """Mark workset as completed."""
        try:
            # Load and update record
            record_df = download_csv(self.record_path)
            if record_df is not None:
                # Find workset row and mark as completed
                workset_rows = record_df[record_df['workset'] == workset]
                if not workset_rows.empty:
                    row_index = workset_rows.index[0]
                    record_df.loc[row_index, 'status'] = 'completed'
                    
                    if upload_csv(self.record_path, record_df):
                        st.success(f"🎉 Workset {workset} completed!")
                        return True
                    else:
                        st.error("Failed to update workset completion status")
                        return False
            return False
        except Exception as e:
            st.error(f"Error completing workset: {str(e)}")
            return False
    
    def _get_next_workset(self) -> Optional[Dict]:
        """Get the next workset to work on after completing current one."""
        try:
            # Load user record
            record_df = download_csv(self.record_path)
            if record_df is None or record_df.empty:
                return None
            
            # Find first not_started workset
            not_started_worksets = record_df[record_df['status'] == 'not_started']
            
            if not not_started_worksets.empty:
                # Start first not_started workset
                workset_row = not_started_worksets.iloc[0]
                row_index = not_started_worksets.index[0]
                
                # Update status to in_progress
                record_df.loc[row_index, 'status'] = 'in_progress'
                
                # Save updated record
                if upload_csv(self.record_path, record_df):
                    st.success(f"🚀 Starting next workset: {workset_row['workset']}")
                    
                    # Get first poem from new workset
                    next_poem = self.get_next_poem(workset_row['workset'])
                    return next_poem
                else:
                    st.error("Failed to start next workset")
                    return None
            
            # All worksets completed
            st.success("🎊 Congratulations! All worksets completed!")
            st.balloons()
            return None
            
        except Exception as e:
            st.error(f"Error getting next workset: {str(e)}")
            return None
    
    def save_annotation(self, workset: str, row_index: int, annotation_data: Dict) -> bool:
        """Save annotation result to Firebase."""
        try:
            # Create annotation file path
            annotation_path = f"annotation/{self.username}/{workset}_{row_index}.json"
            
            # Add metadata (ensure JSON serializable types)
            annotation_data.update({
                'username': self.username,
                'workset': workset,
                'row_index': int(row_index),  # Convert to native Python int
                'timestamp': datetime.now().isoformat(),
                'completed': True
            })
            
            # Ensure all data is JSON serializable
            serializable_data = make_json_serializable(annotation_data)
            
            # Upload annotation
            if upload(annotation_path, serializable_data):
                # Update progress in workset file
                self._update_progress(workset, row_index)
                return True
            else:
                st.error("Failed to save annotation")
                return False
                
        except Exception as e:
            st.error(f"Error saving annotation: {str(e)}")
            return False
    
    def _update_progress(self, workset: str, row_index: int):
        """Update progress status in workset file."""
        try:
            # Load workset file
            workset_path = f"coding_result/{self.username}/{workset}.csv"
            workset_df = download_csv(workset_path)
            
            if workset_df is not None:
                # Update progress
                workset_df.loc[row_index, 'Progress'] = 'Y'
                
                # Save updated workset
                if upload_csv(workset_path, workset_df):
                    st.success("Progress updated!")
                else:
                    st.warning("Failed to update progress")
                    
        except Exception as e:
            st.error(f"Error updating progress: {str(e)}")
    
    def get_workset_progress(self, workset: str) -> Dict:
        """Get progress statistics for a workset."""
        try:
            workset_path = f"coding_result/{self.username}/{workset}.csv"
            workset_df = download_csv(workset_path)
            
            if workset_df is not None:
                total = len(workset_df)
                completed = len(workset_df[workset_df['Progress'] == 'Y'])
                
                return {
                    'total': total,
                    'completed': completed,
                    'remaining': total - completed,
                    'percentage': (completed / total) * 100 if total > 0 else 0
                }
            
            return {'total': 0, 'completed': 0, 'remaining': 0, 'percentage': 0}
            
        except Exception as e:
            st.error(f"Error getting progress: {str(e)}")
            return {'total': 0, 'completed': 0, 'remaining': 0, 'percentage': 0}
    
    def _has_no_record_file(self) -> bool:
        """Check if user has no record file (new user)."""
        try:
            record_df = download_csv(self.record_path)
            return record_df is None or record_df.empty
        except Exception:
            return True  # If there's an error, assume no record file

def get_annotator_manager(username: str) -> AnnotatorWorksetManager:
    """Get annotator workset manager instance."""
    return AnnotatorWorksetManager(username)
