"""Admin Page for managing annotators and worksets."""

import streamlit as st
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import time
import os
import zipfile
import io
import json

# Import Firebase functions
from firebase_storage import (
    download_csv, upload_csv, create_empty_csv, list_files, 
    upload, download
)

# Import workset utilities for auto-creation
from workset_utils import create_workset_file
from firebase_storage import delete_file

def initialize_admin_system():
    """Initialize admin system by creating necessary files and folders."""
    st.info("🔄 Initializing admin system...")
    
    # Initialize progress tracking
    if 'init_progress' not in st.session_state:
        st.session_state.init_progress = []
    
    progress_container = st.empty()
    
    def add_progress(message):
        st.session_state.init_progress.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        with progress_container.container():
            for msg in st.session_state.init_progress[-10:]:  # Show last 10 messages
                st.text(msg)
    
    add_progress("Starting initialization...")
    
    add_progress("Checking admin/users.csv...")
    users_df = download_csv("admin/users.csv")
    if users_df is None:
        add_progress("Creating admin/users.csv...")
        users_df = pd.DataFrame(columns=['annotator_id', 'password', 'created_at'])
        if upload_csv("admin/users.csv", users_df):
            add_progress("✅ Created admin/users.csv")
        else:
            add_progress("❌ Failed to create admin/users.csv")
            return False
    else:
        add_progress("✅ admin/users.csv exists")
    
    add_progress("Checking annotators folder...")
    
    # Get all annotator IDs from users.csv
    if not users_df.empty:
        for _, user in users_df.iterrows():
            annotator_id = user['annotator_id']
            record_path = f"annotators/{annotator_id}/{annotator_id}_record.csv"
            
            add_progress(f"Checking {record_path}...")
            record_df = download_csv(record_path)
            
            if record_df is None:
                add_progress(f"Creating {record_path}...")
                if create_empty_csv(record_path, ['workset', 'status']):
                    add_progress(f"✅ Created {record_path}")
                else:
                    add_progress(f"❌ Failed to create {record_path}")
    
    add_progress("✅ Initialization complete!")
    time.sleep(2)  # Show completion message
    st.session_state.init_progress = []  # Clear progress
    progress_container.empty()
    
    return True

def create_new_annotator():
    """Create a new annotator."""
    st.subheader("➕ Create New Annotator")
    
    # Add download users CSV button
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("📥 Download Users CSV", help="Download all user accounts as CSV"):
            users_df = download_csv("admin/users.csv")
            if users_df is not None and not users_df.empty:
                # Convert to CSV string
                csv_content = users_df.to_csv(index=False)
                
                # Offer download
                st.download_button(
                    label="💾 Save Users CSV",
                    data=csv_content,
                    file_name=f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    help="Download user accounts with password hashes"
                )
                st.success("✅ Users CSV ready for download!")
            else:
                st.warning("No users found to download")
    
    # Show current users table
    users_df = download_csv("admin/users.csv")
    if users_df is not None and not users_df.empty:
        st.write("**Current Users:**")
        st.dataframe(users_df, use_container_width=True)
        
        st.write(f"**Total Users:** {len(users_df)}")
    else:
        st.info("No users created yet.")
    
    st.divider()
    
    with st.form("create_annotator_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            annotator_id = st.text_input(
                "Annotator ID*", 
                placeholder="e.g., coder001",
                help="Unique identifier for the annotator"
            )
        
        with col2:
            password = st.text_input(
                "Password*", 
                type="password",
                help="Login password"
            )
        
        submitted = st.form_submit_button("Create Annotator", type="primary")
        
        if submitted:
            if not annotator_id or not password:
                st.error("Please fill in all required fields")
                return
            
            # Check if annotator already exists
            users_df = download_csv("admin/users.csv")
            if users_df is not None:
                if annotator_id in users_df['annotator_id'].values:
                    st.error(f"Annotator ID '{annotator_id}' already exists")
                    return
            
            
            # Create new user record
            new_user = pd.DataFrame([{
                'annotator_id': annotator_id,
                'password': password,
                'created_at': datetime.now().isoformat()
            }])
            
            # Add to users.csv
            if users_df is not None:
                users_df = pd.concat([users_df, new_user], ignore_index=True)
            else:
                users_df = new_user
            
            # Upload updated users.csv
            if upload_csv("admin/users.csv", users_df):
                # Create empty record file for new annotator
                record_path = f"annotators/{annotator_id}/{annotator_id}_record.csv"
                if create_empty_csv(record_path, ['workset', 'status']):
                    st.success(f"✅ Created annotator '{annotator_id}' successfully!")
                    st.info(f"💡 Password for '{annotator_id}': `{password}` (saved for download)")
                    # Clear cached data to force refresh
                    if 'users_data' in st.session_state:
                        del st.session_state['users_data']
                    if 'selected_annotator' in st.session_state:
                        del st.session_state['selected_annotator']
                    
                    st.rerun()
                else:
                    st.error("Failed to create record file")
            else:
                st.error("Failed to update users.csv")

def get_available_worksets():
    """Get list of available worksets (workset_001 to workset_100)."""
    return [f"workset_{i:03d}" for i in range(1, 101)]

def manage_annotator_tasks(annotator_id: str):
    """Manage tasks for a specific annotator."""
    st.subheader(f"👤 Managing Tasks for: {annotator_id}")
    
    # Load annotator's record
    record_path = f"annotators/{annotator_id}/{annotator_id}_record.csv"
    record_df = download_csv(record_path)
    
    if record_df is None:
        st.error(f"Could not load record file for {annotator_id}")
        return
    
    # Display current tasks
    st.write("**Current Tasks:**")
    if not record_df.empty:
        # Add action buttons for each task
        for idx, row in record_df.iterrows():
            col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
            
            with col1:
                st.write(f"**{row['workset']}**")
            
            with col2:
                status = row['status']
                if status == 'completed':
                    st.success("✅ Completed")
                elif status == 'in_progress':
                    st.warning("🔄 In Progress")
                else:
                    st.info("⏳ Not Started")
            
            with col3:
                # View progress button (only show for in_progress or completed tasks)
                if status in ['in_progress', 'completed']:
                    if st.button(f"View Progress", key=f"progress_{idx}"):
                        view_progress(annotator_id, row['workset'])
                else:
                    st.write("—")  # Show dash for not started tasks
            
            with col4:
                # Remove task button
                if st.button("🗑️", key=f"remove_{idx}", help="Remove task"):
                    workset_to_remove = row['workset']
                    record_df = record_df.drop(idx).reset_index(drop=True)
                    
                    if upload_csv(record_path, record_df):
                        st.success("Task removed!")
                        
                        # Also remove the workset file
                        workset_file_path = f"coding_result/{annotator_id}/{workset_to_remove}.csv"
                        if delete_file(workset_file_path):
                            st.success(f"✅ Workset file deleted: {workset_to_remove}")
                        else:
                            st.warning(f"⚠️ Task removed but workset file deletion failed: {workset_to_remove}")
                        
                        st.rerun()
    else:
        st.info("No tasks assigned yet.")
    
    st.divider()
    
    # Assign new workset
    st.write("**Assign New Workset:**")
    
    # Get all available worksets (workset_001 to workset_100)
    all_worksets = get_available_worksets()
    
    # Filter out already assigned worksets
    assigned_worksets = record_df['workset'].tolist() if not record_df.empty else []
    available_worksets = [ws for ws in all_worksets if ws not in assigned_worksets]
    
    if available_worksets:
        selected_workset = st.selectbox(
            "Select Workset to Assign:",
            available_worksets
        )
        
        if st.button("Assign Workset", type="primary"):
            # Add new task
            new_task = pd.DataFrame([{
                'workset': selected_workset,
                'status': 'not_started'
            }])
            
            record_df = pd.concat([record_df, new_task], ignore_index=True)
            
            if upload_csv(record_path, record_df):
                st.success(f"✅ Assigned workset '{selected_workset}' to {annotator_id}")
                
                # Auto-create workset file
                with st.spinner(f"Creating workset file for {selected_workset}..."):
                    if create_workset_file(annotator_id, selected_workset):
                        st.success(f"✅ Workset file created: {selected_workset}")
                    else:
                        st.warning(f"⚠️ Workset assigned but file creation failed for {selected_workset}")
                
                st.rerun()
            else:
                st.error("Failed to update record file")
    else:
        st.info("All 100 worksets have been assigned to this annotator.")

def view_progress(annotator_id: str, workset_name: str):
    """View annotation results for a specific workset."""
    st.subheader(f"📊 Annotation Results: {annotator_id} - {workset_name}")
    
    # Load annotation results directly
    annotation_results = _load_workset_annotations(annotator_id, workset_name)
    
    if annotation_results:
        # Show summary statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Annotations", len(annotation_results))
        
        with col2:
            # Count unique tags
            all_tags = set()
            for result in annotation_results:
                all_tags.update(result.get('tags', []))
            st.metric("Unique Tags", len(all_tags))
        
        with col3:
            # Count unique moods
            all_moods = set()
            for result in annotation_results:
                all_moods.update(result.get('moods', []))
            st.metric("Unique Moods", len(all_moods))
        
        
        st.divider()
        
        # Show detailed annotations directly (no selector to avoid page refresh)
        _show_detailed_annotations(annotation_results)
        
    else:
        st.info(f"📝 No annotation results found for workset '{workset_name}'")
        st.markdown(f"""
        **Note:** 
        - This workset may not have started annotation yet
        - Or there may be an issue with annotation file paths
        - Annotation files should be located at: `annotation/{annotator_id}/{workset_name}_*.json`
        """)


def _load_workset_annotations(annotator_id: str, workset_name: str):
    """Load all annotation results for a specific workset."""
    from firebase_storage import download_json, list_files
    
    annotation_results = []
    
    try:
        # Get all annotation files for this user and workset
        annotation_files = list_files(f"annotation/{annotator_id}/")
        workset_annotations = [f for f in annotation_files if f.startswith(f"annotation/{annotator_id}/{workset_name}_")]
        
        # Load each annotation
        for annotation_file in workset_annotations:
            try:
                annotation_data = download_json(annotation_file)
                if annotation_data:
                    # Add file info for reference
                    annotation_data['_file_path'] = annotation_file
                    annotation_data['_row_index'] = _extract_row_index_from_filename(annotation_file)
                    annotation_results.append(annotation_data)
                    
            except Exception as e:
                st.warning(f"无法加载标注文件 {annotation_file}: {str(e)}")
                
    except Exception as e:
        st.warning(f"加载标注结果时出错: {str(e)}")
    
    # Sort by row index for consistent display
    annotation_results.sort(key=lambda x: x.get('_row_index', 0))
    
    return annotation_results


def _extract_row_index_from_filename(file_path: str) -> int:
    """Extract row index from annotation filename."""
    try:
        filename = file_path.split('/')[-1]  # Get just filename
        if '_' in filename:
            parts = filename.replace('.json', '').split('_')
            if len(parts) >= 3:
                return int(parts[2])  # workset_001_5 -> 5
    except:
        pass
    return 0


def _show_detailed_annotations(annotation_results):
    """Show simplified detailed view of all annotations."""
    st.subheader("📝 Detailed Annotation Results")
    
    if not annotation_results:
        st.info("No annotations found for this workset.")
        return
    
    for i, annotation in enumerate(annotation_results, 1):
        with st.expander(f"📝 Annotation {i}: {annotation.get('title', 'Unknown Title')}", expanded=False):
            
            col1, col2 = st.columns([3, 2])
            
            with col1:
                # Basic poem information
                st.write(f"**Title:** {annotation.get('title', 'Unknown')}")
                st.write(f"**Author:** {annotation.get('author', 'Unknown')}")
                st.write(f"**URL:** {annotation.get('poem_url', 'Unknown')}")
                
                # Sentiment coordinates
                sentiment_x = annotation.get('sentiment_x', 0)
                sentiment_y = annotation.get('sentiment_y', 0)
                st.write(f"**Sentiment:** ({sentiment_x:.1f}, {sentiment_y:.1f})")
                
                # Time used (if available)
                if 'timing' in annotation:
                    timing = annotation['timing']
                    duration_minutes = timing.get('duration_minutes', 0)
                    duration_seconds = timing.get('duration_seconds', 0)
                    st.write(f"**Time Used:** {duration_minutes:.1f} minutes ({duration_seconds:.1f} seconds)")
                
            
            with col2:
                # Tags
                tags = annotation.get('tags', [])
                if tags:
                    st.write("**Tags:**")
                    valid_tags = []
                    for tag in tags:
                        # Filter out empty or invalid tags
                        if tag and str(tag).strip():
                            valid_tags.append(str(tag).strip())
                    
                    if valid_tags:
                        for tag in valid_tags:
                            st.badge(tag)
                    else:
                        st.write("*No valid tags found*")
                else:
                    st.write("**Tags:** None")
                
                # Moods
                moods = annotation.get('moods', [])
                if moods:
                    st.write("**Moods:**")
                    valid_moods = []
                    for mood in moods:
                        # Filter out empty or invalid moods
                        if mood and str(mood).strip():
                            valid_moods.append(str(mood).strip())
                    
                    if valid_moods:
                        for mood in valid_moods:
                            st.badge(mood)
                    else:
                        st.write("*No valid moods found*")
                else:
                    st.write("**Moods:** None")
    
    # Add export option at the bottom
    st.divider()
    if st.button("📥 Export Annotation Data (JSON)"):
        import json
        json_data = json.dumps(annotation_results, ensure_ascii=False, indent=2)
        
        # Get workset name from function parameter or annotation data
        username = annotation_results[0].get('username', 'unknown') if annotation_results else 'unknown'
        filename = f"{username}_annotations.json"
        
        st.download_button(
            label="Download JSON File",
            data=json_data,
            file_name=filename,
            mime="application/json"
        )



def download_results_page():
    """Page for downloading annotation results."""
    st.header("📥 Download Annotation Results")
    st.markdown("Download all annotation files from the system")
    
    # Show system statistics
    st.subheader("📊 System Statistics")
    
    try:
        # Get annotation files count
        annotation_files = list_files("annotation/")
        total_files = len(annotation_files) if annotation_files else 0
        
        # Get unique annotators
        annotators = set()
        worksets = set()
        
        for file_path in annotation_files:
            parts = file_path.split('/')
            if len(parts) >= 3:
                annotators.add(parts[1])  # annotation/{annotator_id}/{workset}_{index}.json
                filename = parts[2]
                if '_' in filename:
                    workset = filename.split('_')[0]
                    worksets.add(workset)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Annotations", total_files)
        with col2:
            st.metric("Active Annotators", len(annotators))
        with col3:
            st.metric("Worksets", len(worksets))
            
    except Exception as e:
        st.warning(f"Could not load statistics: {str(e)}")
    
    st.divider()
    
    # Download options
    st.subheader("📦 Download Options")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        **Download All Annotations**
        
        This will create a ZIP file containing all annotation results from the system.
        Each annotation is saved as a JSON file with the following structure:
        - `annotation/{annotator_id}/{workset_name}_{row_index}.json`
        
        The ZIP file will preserve the folder structure and include all metadata.
        """)
    
    with col2:
        if st.button("📥 Download All Annotations", type="primary", use_container_width=True):
            with st.spinner("🔄 Creating ZIP archive..."):
                result = download_all_annotations_admin()
                
                if result:
                    zip_data, filename = result
                    
                    st.download_button(
                        label="💾 Download ZIP File",
                        data=zip_data,
                        file_name=filename,
                        mime="application/zip",
                        type="primary",
                        use_container_width=True
                    )
                    
                    st.success("✅ ZIP file ready for download!")
                else:
                    st.error("❌ Failed to create ZIP archive")
    
    # Additional download options
    st.divider()
    st.subheader("🔧 Advanced Options")
    
    with st.expander("🔧 Fix Usage Statistics"):
        st.markdown("""
        **Workset Usage Details**
        
        View detailed information about which users are using each workset.
        """)
        
        if st.button("📊 Show Detailed Usage Table", type="primary"):
            with st.spinner("Loading detailed usage information..."):
                detailed_usage = _get_detailed_workset_usage()
                
                if detailed_usage:
                    # Create DataFrame for display
                    import pandas as pd
                    
                    table_data = []
                    for workset_name, info in sorted(detailed_usage.items()):
                        users_list = ", ".join(info['users']) if info['users'] else "None"
                        table_data.append({
                            'Workset': workset_name,
                            'Usage Count': info['count'],
                            'Status': 'Full' if info['count'] >= 3 else 'Available',
                            'Users': users_list
                        })
                    
                    df = pd.DataFrame(table_data)
                    
                    # Display table
                    st.dataframe(
                        df,
                        use_container_width=True,
                        column_config={
                            "Workset": st.column_config.TextColumn("Workset", width="small"),
                            "Usage Count": st.column_config.NumberColumn("Count", width="small"),
                            "Status": st.column_config.TextColumn("Status", width="small"),
                            "Users": st.column_config.TextColumn("Users", width="large")
                        }
                    )
                    
                    # Summary statistics
                    st.divider()
                    col1, col2, col3, col4 = st.columns(4)
                    
                    total_used = len([d for d in table_data if d['Usage Count'] > 0])
                    available = len([d for d in table_data if d['Status'] == 'Available'])
                    full = len([d for d in table_data if d['Status'] == 'Full'])
                    unused = 100 - total_used
                    
                    with col1:
                        st.metric("Total Used", total_used)
                    with col2:
                        st.metric("Available", available)
                    with col3:
                        st.metric("Full", full)
                    with col4:
                        st.metric("Unused", unused)
                        
                else:
                    st.info("No workset usage found")
        


def _get_detailed_workset_usage():
    """Get detailed workset usage information including which users are using each workset."""
    try:
        from firebase_storage import list_files, download_csv
        
        detailed_usage = {}
        
        # Get all annotator record files
        annotator_files = list_files("annotators/")
        
        for file_path in annotator_files:
            if file_path.endswith("_record.csv"):
                # Extract username from file path: annotators/{username}/{username}_record.csv
                path_parts = file_path.split('/')
                if len(path_parts) >= 3:
                    username = path_parts[1]
                    
                    # Download and process the record file
                    record_df = download_csv(file_path)
                    if record_df is not None and 'workset' in record_df.columns:
                        # Get unique worksets for this user
                        unique_worksets = record_df['workset'].dropna().unique()
                        
                        for workset in unique_worksets:
                            if workset not in detailed_usage:
                                detailed_usage[workset] = {
                                    'count': 0,
                                    'users': []
                                }
                            
                            detailed_usage[workset]['count'] += 1
                            detailed_usage[workset]['users'].append(username)
        
        return detailed_usage
        
    except Exception as e:
        st.error(f"Failed to get detailed usage: {str(e)}")
        return {}


def download_all_annotations_admin():
    """Download all annotation files as a ZIP archive."""
    from firebase_storage import download_json
    
    try:
        # Get all annotation files
        annotation_files = list_files("annotation/")
        
        if not annotation_files:
            st.warning("No annotation files found.")
            return None
        
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


# def workset_management_page():
#     """Workset management page - DISABLED."""
#     # This function has been disabled to remove automatic workset management
#     pass

def admin_page():
    """Main admin page function."""
    st.set_page_config(
        page_title="Admin Dashboard",
        page_icon="👨‍💼",
        layout="wide"
    )
    
    st.title("👨‍💼 Admin Dashboard")
    st.markdown("Manage annotators, worksets, and tasks")
    
    # Initialize system on first run
    if 'admin_initialized' not in st.session_state:
        if initialize_admin_system():
            st.session_state.admin_initialized = True
            st.rerun()
        else:
            st.error("Failed to initialize admin system")
            return
    
    # Sidebar navigation
    with st.sidebar:
        st.header("🧭 Navigation")
        
        page = st.radio(
            "Select Page:",
            ["👥 Annotators", "➕ Create Annotator", "📥 Download Results"]
        )
        
    
    # Main content based on selected page
    if page == "👥 Annotators":
        st.header("👥 Annotator Management")
        
        # Load users with caching
        if 'users_data' not in st.session_state:
            st.session_state.users_data = download_csv("admin/users.csv")
        
        users_df = st.session_state.users_data
        
        if users_df is not None and not users_df.empty:
            st.write("**Select an annotator to manage:**")
            
            # Add refresh button
            if st.button("🔄 Refresh User List"):
                st.session_state.users_data = download_csv("admin/users.csv")
                st.rerun()
            
            # Create annotator buttons
            cols = st.columns(3)
            for idx, (_, user) in enumerate(users_df.iterrows()):
                col = cols[idx % 3]
                
                with col:
                    button_label = f"👤 {user['annotator_id']}"
                    
                    if st.button(button_label, key=f"annotator_{user['annotator_id']}"):
                        st.session_state.selected_annotator = user['annotator_id']
            
            # Show selected annotator's tasks
            if 'selected_annotator' in st.session_state:
                st.divider()
                manage_annotator_tasks(st.session_state.selected_annotator)
        else:
            st.info("No annotators found. Create some annotators first.")
    
    elif page == "➕ Create Annotator":
        create_new_annotator()
    
    elif page == "📥 Download Results":
        download_results_page()

if __name__ == "__main__":
    admin_page()
