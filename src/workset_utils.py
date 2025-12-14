"""
Workset utilities for Poetry Annotation System
Manages user worksets and ensures all required workset files are created.
"""

from firebase_storage import download_csv, upload_csv, list_files
import pandas as pd
from typing import List, Dict, Optional
import streamlit as st

def get_user_worksets(username: str) -> Optional[List[str]]:
    """Get worksets for a specific user from their record file."""
    try:
        record_path = f"annotators/{username}/{username}_record.csv"
        record_df = download_csv(record_path)
        
        if record_df is None:
            return None
        
        # Find workset column
        workset_col = None
        for col in record_df.columns:
            if 'workset' in col.lower():
                workset_col = col
                break
        
        if workset_col is None:
            return None
        
        # Get unique worksets
        worksets = record_df[workset_col].dropna().unique()
        return list(worksets)
        
    except Exception as e:
        st.error(f"Error getting user worksets: {str(e)}")
        return None

def check_missing_worksets(username: str, worksets: List[str]) -> List[str]:
    """Check which workset files are missing for a user."""
    try:
        coding_path = f"coding_result/{username}/"
        existing_files = list_files(coding_path)
        
        missing_worksets = []
        for workset in worksets:
            expected_path = f"{coding_path}{workset}.csv"
            if expected_path not in existing_files:
                missing_worksets.append(workset)
        
        return missing_worksets
        
    except Exception as e:
        st.error(f"Error checking workset files: {str(e)}")
        return worksets  # Assume all are missing if error

def create_workset_file(username: str, workset: str) -> bool:
    """Create a single workset file for a user."""
    try:
        # Extract number from workset name (e.g., "workset_001" -> "001")
        workset_number = workset.replace("workset_", "")
        
        # Find the corresponding dataset file
        dataset_path = f"workset/dataset_{workset_number}.csv"
        
        # Download the dataset
        dataset_df = download_csv(dataset_path)
        if dataset_df is None:
            st.error(f"Could not find dataset file: {dataset_path}")
            return False
        
        # Add Progress column with all 'N' values
        dataset_df['Progress'] = 'N'
        
        # Upload to coding_result folder
        output_path = f"coding_result/{username}/{workset}.csv"
        
        if upload_csv(output_path, dataset_df):
            st.success(f"Created workset file: {workset}")
            return True
        else:
            st.error(f"Failed to create workset file: {workset}")
            return False
            
    except Exception as e:
        st.error(f"Error creating workset file {workset}: {str(e)}")
        return False

def ensure_user_worksets(username: str) -> Dict[str, bool]:
    """Ensure all required workset files exist for a user."""
    try:
        # Get user worksets
        worksets = get_user_worksets(username)
        if not worksets:
            return {}
        
        # Check missing worksets
        missing_worksets = check_missing_worksets(username, worksets)
        
        if not missing_worksets:
            return {ws: True for ws in worksets}  # All exist
        
        # Create missing worksets
        results = {}
        for workset in worksets:
            if workset in missing_worksets:
                results[workset] = create_workset_file(username, workset)
            else:
                results[workset] = True  # Already exists
        
        return results
        
    except Exception as e:
        st.error(f"Error ensuring user worksets: {str(e)}")
        return {}

def get_workset_status(username: str) -> Dict[str, str]:
    """Get the status of all worksets for a user."""
    try:
        worksets = get_user_worksets(username)
        if not worksets:
            return {}
        
        missing_worksets = check_missing_worksets(username, worksets)
        
        status = {}
        for workset in worksets:
            if workset in missing_worksets:
                status[workset] = "MISSING"
            else:
                status[workset] = "EXISTS"
        
        return status
        
    except Exception as e:
        st.error(f"Error getting workset status: {str(e)}")
        return {}

def display_workset_management_ui(username: str):
    """Display workset management UI for a user."""
    st.subheader(f"📁 Workset Management for {username}")
    
    # Get workset status
    status = get_workset_status(username)
    
    if not status:
        st.info("No worksets found for this user.")
        return
    
    # Display status
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.write("**Workset Status:**")
        for workset, workset_status in status.items():
            if workset_status == "EXISTS":
                st.success(f"✅ {workset}")
            else:
                st.error(f"❌ {workset}")
    
    with col2:
        missing_count = sum(1 for s in status.values() if s == "MISSING")
        
        if missing_count > 0:
            st.warning(f"{missing_count} worksets missing")
            
            if st.button("🔧 Create Missing Worksets", type="primary"):
                with st.spinner("Creating missing worksets..."):
                    results = ensure_user_worksets(username)
                    
                    success_count = sum(1 for success in results.values() if success)
                    total_count = len(results)
                    
                    if success_count == total_count:
                        st.success(f"✅ All {total_count} worksets created successfully!")
                    else:
                        st.warning(f"⚠️ Created {success_count}/{total_count} worksets")
                    
                    st.rerun()
        else:
            st.success("All worksets exist!")

def auto_ensure_worksets_on_login(username: str) -> bool:
    """Automatically ensure worksets exist when user logs in."""
    try:
        worksets = get_user_worksets(username)
        if not worksets:
            return True  # No worksets needed
        
        missing_worksets = check_missing_worksets(username, worksets)
        
        if missing_worksets:
            st.info(f"Setting up {len(missing_worksets)} missing worksets for {username}...")
            
            results = {}
            for workset in missing_worksets:
                results[workset] = create_workset_file(username, workset)
            
            success_count = sum(1 for success in results.values() if success)
            
            if success_count == len(missing_worksets):
                st.success(f"✅ All worksets ready for {username}")
                return True
            else:
                st.warning(f"⚠️ Some worksets could not be created")
                return False
        
        return True  # All worksets already exist
        
    except Exception as e:
        st.error(f"Error auto-ensuring worksets: {str(e)}")
        return False


