"""
Workset Auto Assignment System
Implements file-lock based workset assignment with usage tracking.
"""

import time
import random
from datetime import datetime, timedelta
from firebase_storage import upload, download_json, delete_file, list_files, download_csv, upload_csv
from workset_utils import create_workset_file
from typing import Optional, Dict, List
import streamlit as st
import pandas as pd

class WorksetAutoAssigner:
    """File-lock based workset auto-assigner"""
    
    def __init__(self):
        self.locks_dir = "system/locks"
        self.usage_stats_path = "system/workset_usage_stats.json"
        self.assignment_log_path = "system/workset_assignment_log.csv"
    
    def request_new_workset(self, username: str) -> Optional[str]:
        """User requests a new workset"""
        
        # Check if user has unfinished worksets
        if self._has_pending_worksets(username):
            st.warning("You still have unfinished worksets. Please complete your current tasks first.")
            return None
        
        st.info("🔍 Looking for a new workset for you...")
        
        # Try to assign new workset
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                # Select available workset
                available_workset = self._find_available_workset(username)
                
                if not available_workset:
                    st.info("No worksets available for assignment (all worksets have been used 3 times)")
                    return None
                
                # Try to acquire lock for this workset
                if self._try_acquire_workset_lock(available_workset, username):
                    try:
                        # Double-check workset is still available
                        if self._is_workset_still_available(available_workset):
                            # Complete assignment
                            if self._complete_workset_assignment(username, available_workset):
                                st.success(f"✅ Successfully assigned new workset: {available_workset}")
                                return available_workset
                        else:
                            st.info(f"Workset {available_workset} was taken by another user, retrying...")
                    
                    finally:
                        # Release lock
                        self._release_workset_lock(available_workset, username)
                
                # Wait and retry
                wait_time = (attempt + 1) * 0.5 + random.uniform(0, 0.5)
                time.sleep(wait_time)
                
            except Exception as e:
                st.error(f"Assignment attempt {attempt + 1} failed: {str(e)}")
                time.sleep(1)
        
        st.warning("Unable to assign new workset after multiple attempts. Please try again later.")
        return None
    
    def _has_pending_worksets(self, username: str) -> bool:
        """Check if user has unfinished worksets"""
        try:
            record_path = f"annotators/{username}/{username}_record.csv"
            record_df = download_csv(record_path)
            
            if record_df is not None and not record_df.empty:
                # Check for in_progress or not_started worksets
                pending = record_df[record_df['status'].isin(['in_progress', 'not_started'])]
                return not pending.empty
            
            # If no record file or empty file, this is a new user with no pending worksets
            return False
            
        except Exception as e:
            st.error(f"Failed to check pending worksets: {str(e)}")
            return True  # Conservative handling on error
    
    def _has_completed_workset(self, username: str, workset_name: str) -> bool:
        """Check if user has already completed a specific workset"""
        try:
            record_path = f"annotators/{username}/{username}_record.csv"
            record_df = download_csv(record_path)
            
            if record_df is not None and not record_df.empty:
                # Check if user has completed this specific workset
                completed_worksets = record_df[
                    (record_df['workset'] == workset_name) & 
                    (record_df['status'] == 'completed')
                ]
                return not completed_worksets.empty
            
            return False
            
        except Exception as e:
            st.error(f"Failed to check completed workset: {str(e)}")
            return False  # Conservative handling - allow assignment if unsure
    
    def _find_available_workset(self, username: str) -> Optional[str]:
        """Find available workset (usage count < 3 AND user hasn't completed it)"""
        try:
            # Get current usage statistics
            usage_stats = self._get_usage_statistics()
            
            
            # Show workset counts by category
            total_unused = sum(1 for count in usage_stats.values() if count == 0)
            total_used_once = sum(1 for count in usage_stats.values() if count == 1)
            total_used_twice = sum(1 for count in usage_stats.values() if count == 2)
            total_full = sum(1 for count in usage_stats.values() if count >= 3)
            
            # Strict numerical order strategy: lower-numbered worksets must reach 3 uses before moving to next
            for i in range(1, 101):  # workset_001 to workset_100, check in order
                workset_name = f"workset_{i:03d}"
                usage_count = usage_stats.get(workset_name, 0)
                
                # If this workset hasn't reached 3 uses, check if user can use it
                if usage_count < 3:
                    # CRITICAL CHECK: Skip if user has already completed this workset
                    if self._has_completed_workset(username, workset_name):
                        st.info(f"⏭️  Skipping {workset_name}: User {username} has already completed it")
                        continue
                    
                    st.info(f"🎯 Selected workset: {workset_name} (usage: {usage_count}/3)")
                    st.info(f"📋 Strategy: Using workset in strict numerical order - {workset_name} must reach 3 uses before moving to next")
                    return workset_name
            
            # If all worksets are fully used or user has completed all available ones
            st.warning("🚫 No available worksets found (either fully used or user has completed all available ones)")
            return None
            
        except Exception as e:
            st.error(f"Failed to find available workset: {str(e)}")
            return None
    
    def _get_usage_statistics(self) -> Dict[str, int]:
        """Get workset usage statistics"""
        try:
            usage_data = download_json(self.usage_stats_path)
            if usage_data:
                return usage_data.get('workset_usage', {})
            else:
                # If no statistics file, scan all user records to generate statistics
                return self._generate_usage_statistics()
                
        except Exception as e:
            # If file not found error, this is normal
            if "File not found" in str(e) or "not found" in str(e).lower():
                return self._generate_usage_statistics()
            else:
                st.warning(f"Failed to get usage statistics: {str(e)}")
                return {}
    
    def _generate_usage_statistics(self) -> Dict[str, int]:
        """Generate usage statistics by scanning user records"""
        try:
            usage_count = {}
            scanned_files = 0
            
            # Get all annotator directories
            annotator_files = list_files("annotators/")
            
            if not annotator_files:
                # If no user records found, return empty statistics
                usage_data = {
                    'workset_usage': {},
                    'last_updated': datetime.now().isoformat(),
                    'generated_from_scan': True,
                    'note': 'No existing user records found'
                }
                upload(self.usage_stats_path, usage_data)
                return {}
            
            for file_path in annotator_files:
                if file_path.endswith("_record.csv"):
                    record_df = download_csv(file_path)
                    if record_df is not None and 'workset' in record_df.columns:
                        scanned_files += 1
                        # Get unique worksets used by this user (count each workset only once per user)
                        # Count all assignments (both completed and in_progress count toward usage limit)
                        unique_worksets = record_df['workset'].dropna().unique()
                        for workset in unique_worksets:
                            usage_count[workset] = usage_count.get(workset, 0) + 1
            
            # Save statistics results
            usage_data = {
                'workset_usage': usage_count,
                'last_updated': datetime.now().isoformat(),
                'generated_from_scan': True,
                'scanned_files': scanned_files,
                'total_assignments': sum(usage_count.values())
            }
            
            upload(self.usage_stats_path, usage_data)
            
            return usage_count
            
        except Exception as e:
            st.error(f"Failed to generate usage statistics: {str(e)}")
            return {}
    
    def _try_acquire_workset_lock(self, workset_name: str, username: str) -> bool:
        """Try to acquire workset lock (improved version with competition detection)"""
        try:
            # Generate unique competition ID
            competition_id = f"{username}_{int(time.time() * 1000)}_{random.randint(10000, 99999)}"
            
            competition_path = f"{self.locks_dir}/competition_{workset_name}_{competition_id}.json"
            competition_data = {
                'workset': workset_name,
                'owner': username,
                'competition_id': competition_id,
                'timestamp': datetime.now().isoformat(),
                'status': 'competing'
            }
            
            if not upload(competition_path, competition_data):
                return False
            
            # Wait random time to let all competitors create files
            wait_time = random.uniform(0.5, 1.5)
            time.sleep(wait_time)
            
            if self._win_competition(workset_name, username, competition_id):
                # We won, create formal lock
                lock_path = f"{self.locks_dir}/{workset_name}_lock.json"
                lock_data = {
                    'workset': workset_name,
                    'owner': username,
                    'competition_id': competition_id,
                    'timestamp': datetime.now().isoformat(),
                    'expires_at': (datetime.now() + timedelta(minutes=2)).isoformat(),
                    'status': 'locked'
                }
                
                if upload(lock_path, lock_data):
                    # Clean up competition files
                    self._cleanup_competition_files(workset_name, competition_id)
                    return True
            
            # We failed, clean up competition files
            delete_file(competition_path)
            return False
            
        except Exception as e:
            st.error(f"Failed to acquire lock: {str(e)}")
            return False
    
    def _win_competition(self, workset_name: str, username: str, competition_id: str) -> bool:
        """Check if won the competition"""
        try:
            # Get all competition files
            lock_files = list_files(f"{self.locks_dir}/")
            competitors = []
            
            for file_path in lock_files:
                if f"competition_{workset_name}_" in file_path and file_path.endswith('.json'):
                    comp_data = download_json(file_path)
                    if comp_data and comp_data.get('status') == 'competing':
                        competitors.append({
                            'owner': comp_data.get('owner'),
                            'competition_id': comp_data.get('competition_id'),
                            'timestamp': comp_data.get('timestamp'),
                            'file_path': file_path
                        })
            
            if not competitors:
                return False
            
            # Sort by timestamp, earliest wins
            competitors.sort(key=lambda x: x['timestamp'])
            
            # Check if we are the earliest
            winner = competitors[0]
            is_winner = (winner['owner'] == username and 
                        winner['competition_id'] == competition_id)
            
            if is_winner:
                st.info(f"🏆 Won workset competition: {workset_name}")
            else:
                st.info(f"⏳ Another user requested {workset_name} earlier, retrying...")
            
            return is_winner
            
        except Exception as e:
            st.error(f"Competition detection failed: {str(e)}")
            return False
    
    def _cleanup_competition_files(self, workset_name: str, winner_competition_id: str):
        """Clean up competition files"""
        try:
            lock_files = list_files(f"{self.locks_dir}/")
            
            for file_path in lock_files:
                if f"competition_{workset_name}_" in file_path:
                    delete_file(file_path)
                    
        except Exception as e:
            st.warning(f"Failed to clean up competition files: {str(e)}")
    
    def _release_workset_lock(self, workset_name: str, username: str):
        """Release workset lock"""
        try:
            lock_path = f"{self.locks_dir}/{workset_name}_lock.json"
            
            # Verify lock belongs to us before deleting
            current_lock = download_json(lock_path)
            if current_lock and current_lock.get('owner') == username:
                delete_file(lock_path)
                
        except Exception as e:
            st.warning(f"Failed to release lock: {str(e)}")
    
    def _is_workset_still_available(self, workset_name: str) -> bool:
        """Double-check if workset is still available"""
        try:
            current_usage = self._get_usage_statistics()
            return current_usage.get(workset_name, 0) < 3
            
        except Exception:
            return False
    
    def _complete_workset_assignment(self, username: str, workset_name: str) -> bool:
        """Complete workset assignment (with final verification)"""
        try:
            # CRITICAL CHECK: Prevent duplicate assignment of completed worksets
            if self._has_completed_workset(username, workset_name):
                st.warning(f"❌ Assignment cancelled: User {username} has already completed {workset_name}")
                st.info("Users cannot be assigned worksets they have already completed.")
                return False
            
            # Final verification: check again if workset is really available
            current_usage = self._get_real_time_usage_count(workset_name)
            
            if current_usage >= 3:
                st.warning(f"Workset {workset_name} has reached usage limit, assignment cancelled")
                return False
            
            if not self._update_usage_statistics_safely(workset_name, current_usage):
                return False
            
            if not self._add_workset_to_user_record(username, workset_name):
                self._rollback_usage_statistics(workset_name)
                return False
            
            if not create_workset_file(username, workset_name):
                self._rollback_usage_statistics(workset_name)
                self._remove_workset_from_user_record(username, workset_name)
                return False
            
            self._log_assignment(username, workset_name)
            
            return True
            
        except Exception as e:
            st.error(f"Failed to complete assignment: {str(e)}")
            return False
    
    def _get_real_time_usage_count(self, workset_name: str) -> int:
        """Get real-time usage count of workset (scan all user records)"""
        try:
            usage_count = 0
            
            # Scan all annotator records
            annotator_files = list_files("annotators/")
            
            for file_path in annotator_files:
                if file_path.endswith("_record.csv"):
                    record_df = download_csv(file_path)
                    if record_df is not None and 'workset' in record_df.columns:
                        # Check if this user has used this workset (count max 1 time per user)
                        # Count both completed and in_progress (all assignments count toward the limit)
                        if workset_name in record_df['workset'].values:
                            usage_count += 1
            
            return usage_count
            
        except Exception as e:
            st.error(f"Failed to get real-time usage count: {str(e)}")
            return 999  # Return large number for conservative handling
    
    def _update_usage_statistics_safely(self, workset_name: str, expected_current_count: int) -> bool:
        """Safely update usage statistics (with verification)"""
        try:
            usage_data = download_json(self.usage_stats_path) or {
                'workset_usage': {},
                'last_updated': datetime.now().isoformat()
            }
            
            # Verify current count matches expectation
            current_count_in_stats = usage_data['workset_usage'].get(workset_name, 0)
            
            # If statistics data is clearly wrong, regenerate
            if abs(current_count_in_stats - expected_current_count) > 1:
                st.warning(f"Detected statistics inconsistency, regenerating statistics data")
                usage_data['workset_usage'] = self._generate_usage_statistics()
            
            # Increase usage count
            new_count = usage_data['workset_usage'].get(workset_name, 0) + 1
            
            # Final check: ensure not exceeding 3 times
            if new_count > 3:
                st.error(f"Workset {workset_name} usage count will exceed limit ({new_count})")
                return False
            
            usage_data['workset_usage'][workset_name] = new_count
            usage_data['last_updated'] = datetime.now().isoformat()
            usage_data['verified_at'] = datetime.now().isoformat()
            
            return upload(self.usage_stats_path, usage_data)
            
        except Exception as e:
            st.error(f"Failed to safely update usage statistics: {str(e)}")
            return False
    
    def _update_usage_statistics(self, workset_name: str) -> bool:
        """Update usage statistics"""
        try:
            usage_data = download_json(self.usage_stats_path) or {
                'workset_usage': {},
                'last_updated': datetime.now().isoformat()
            }
            
            # Increase usage count
            usage_data['workset_usage'][workset_name] = usage_data['workset_usage'].get(workset_name, 0) + 1
            usage_data['last_updated'] = datetime.now().isoformat()
            
            return upload(self.usage_stats_path, usage_data)
            
        except Exception as e:
            st.error(f"Failed to update usage statistics: {str(e)}")
            return False
    
    def _rollback_usage_statistics(self, workset_name: str):
        """Rollback usage statistics"""
        try:
            usage_data = download_json(self.usage_stats_path)
            if usage_data and workset_name in usage_data.get('workset_usage', {}):
                usage_data['workset_usage'][workset_name] -= 1
                if usage_data['workset_usage'][workset_name] <= 0:
                    del usage_data['workset_usage'][workset_name]
                
                usage_data['last_updated'] = datetime.now().isoformat()
                upload(self.usage_stats_path, usage_data)
                
        except Exception as e:
            st.warning(f"Failed to rollback usage statistics: {str(e)}")
    
    def _add_workset_to_user_record(self, username: str, workset_name: str) -> bool:
        """Add workset to user record"""
        try:
            record_path = f"annotators/{username}/{username}_record.csv"
            record_df = download_csv(record_path)
            
            if record_df is None:
                record_df = pd.DataFrame(columns=['workset', 'status'])
            
            # Add new workset
            new_task = pd.DataFrame([{
                'workset': workset_name,
                'status': 'not_started',
                'assigned_at': datetime.now().isoformat(),
                'auto_assigned': True,
                'assignment_type': 'user_request'
            }])
            
            record_df = pd.concat([record_df, new_task], ignore_index=True)
            
            return upload_csv(record_path, record_df)
            
        except Exception as e:
            st.error(f"Failed to add to user record: {str(e)}")
            return False
    
    def _remove_workset_from_user_record(self, username: str, workset_name: str):
        """Remove workset from user record (for rollback)"""
        try:
            record_path = f"annotators/{username}/{username}_record.csv"
            record_df = download_csv(record_path)
            
            if record_df is not None:
                # Remove the last added workset
                record_df = record_df[~((record_df['workset'] == workset_name) & 
                                      (record_df.get('auto_assigned', False) == True))]
                upload_csv(record_path, record_df)
                
        except Exception as e:
            st.warning(f"Failed to remove from user record: {str(e)}")
    
    def _log_assignment(self, username: str, workset_name: str):
        """Log assignment"""
        try:
            log_df = download_csv(self.assignment_log_path)
            if log_df is None:
                log_df = pd.DataFrame(columns=[
                    'timestamp', 'username', 'workset', 'assignment_type', 'success'
                ])
            
            new_log = pd.DataFrame([{
                'timestamp': datetime.now().isoformat(),
                'username': username,
                'workset': workset_name,
                'assignment_type': 'user_request',
                'success': True
            }])
            
            log_df = pd.concat([log_df, new_log], ignore_index=True)
            upload_csv(self.assignment_log_path, log_df)
            
        except Exception as e:
            st.warning(f"Failed to log assignment: {str(e)}")
    
    def get_usage_summary(self) -> Dict:
        """Get usage summary"""
        try:
            usage_stats = self._get_usage_statistics()
            
            total_worksets = 100
            used_worksets = len(usage_stats)
            available_worksets = sum(1 for count in usage_stats.values() if count < 3)
            fully_used_worksets = sum(1 for count in usage_stats.values() if count >= 3)
            
            return {
                'total_worksets': total_worksets,
                'used_worksets': used_worksets,
                'available_worksets': available_worksets,
                'fully_used_worksets': fully_used_worksets,
                'unused_worksets': total_worksets - used_worksets
            }
            
        except Exception as e:
            st.error(f"Failed to get usage summary: {str(e)}")
            return {}
    
    def force_regenerate_usage_statistics(self) -> Dict[str, int]:
        """Force regenerate usage statistics (fix incorrect statistics data)"""
        try:
            # Delete old statistics file
            try:
                delete_file(self.usage_stats_path)
            except:
                pass  # File might not exist
            
            # Regenerate statistics
            new_stats = self._generate_usage_statistics()
            return new_stats
            
        except Exception as e:
            st.error(f"Failed to regenerate statistics: {str(e)}")
            return {}

def cleanup_expired_locks():
    """Clean up expired lock files"""
    try:
        lock_files = list_files("system/locks/")
        current_time = datetime.now()
        
        for file_path in lock_files:
            if file_path.endswith('_lock.json'):
                lock_data = download_json(file_path)
                if lock_data and 'expires_at' in lock_data:
                    expires_at = datetime.fromisoformat(lock_data['expires_at'])
                    
                    if current_time > expires_at:
                        delete_file(file_path)
                        
    except Exception as e:
        st.warning(f"Failed to clean up expired locks: {str(e)}")
