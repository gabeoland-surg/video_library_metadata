import streamlit as st
import json
from datetime import datetime, timedelta
import os
import boto3
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse
import pandas as pd
import shutil

# Load environment variables
load_dotenv()

# Configuration from .env
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'insights-prod-media-bucket')
AUTH_URL = os.getenv('AUTH_URL', 'https://api.accounts.surgicalsafety.com/oauth/v1/token')
EXPLORER_API_URL = os.getenv('EXPLORER_API_URL', 'https://api.blackbox.surgicalsafety.com/api/explorer/v2/export')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Import surgeon IDs from separate config file (not tracked in Git)
try:
    from surgeon_config import MY_SURGEON_IDS
except ImportError:
    # Fallback if config file doesn't exist
    MY_SURGEON_IDS = []
    st.warning("⚠️ surgeon_config.py not found. Create this file to enable surgeon filtering.")

def format_date(date_str):
    """Convert YYYY-MM-DD to MM/DD/YYYY"""
    if not date_str or date_str == 'N/A':
        return 'N/A'
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return date_obj.strftime('%m/%d/%Y')
    except:
        return date_str

def get_auth_token():
    """Get authentication token from Surgical Safety API"""
    try:
        response = requests.post(
            url=AUTH_URL,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'accept': 'application/json'
            },
            data={
                'client_id': CLIENT_ID,
                'secret': CLIENT_SECRET,
                'grant_type': 'client_credentials'
            }
        )
        response.raise_for_status()
        return response.json()['accessToken']
    except Exception as e:
        st.error(f"Authentication failed: {e}")
        return None

def fetch_videos_from_explorer(start_date, end_date, token):
    """Fetch video list from Explorer API"""
    try:
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        payload = {
            'startDate': start_date,
            'endDate': end_date
        }
        
        response = requests.post(EXPLORER_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        
        return response.json()
    except Exception as e:
        st.error(f"Error fetching videos: {e}")
        return []

def parse_video_metadata(video_data):
    """Parse video metadata from Explorer API response"""
    videos = []
    
    for case in video_data:
        procedures = case.get('procedures', [])
        specialties = case.get('specialties', [])
        room = case.get('room', 'N/A')
        case_date = case.get('caseDate', 'N/A')
        upload_date = case.get('uploadDate', 'N/A')
        duration = case.get('videoDurationSeconds', 0)
        
        # Extract users/surgeon EMR IDs (list of strings like ["EMRID1", "EMRID2"])
        users = case.get('users', [])
        if isinstance(users, list) and users:
            surgeon_id_str = ', '.join(users)
        else:
            surgeon_id_str = 'N/A'
        
        # Process each media file
        for media in case.get('mediaFiles', []):
            s3_location = media.get('s3Location', '')
            start_time = media.get('startTime', 'N/A')
            end_time = media.get('endTime', 'N/A')
            
            if s3_location:
                parsed_url = urlparse(s3_location)
                s3_key = parsed_url.path.lstrip('/')
                path_segments = s3_key.split('/')
                video_id = path_segments[-2] if len(path_segments) > 1 else 'unknown'
                filename = path_segments[-1] if path_segments else 'unknown.mp4'
            else:
                s3_key = ''
                video_id = 'unknown'
                filename = 'unknown.mp4'
            
            videos.append({
                'filename': filename,
                'video_id': video_id,
                's3_key': s3_key,
                's3_location': s3_location,
                'procedure_name': ', '.join(procedures) if procedures else 'N/A',
                'specialties': ', '.join(specialties) if specialties else 'N/A',
                'room': room,
                'case_date': case_date,
                'upload_date': upload_date,
                'users': users,  # Keep raw users list for filtering
                'start_time': start_time.split('T')[1] if 'T' in start_time else start_time,
                'end_time': end_time.split('T')[1] if 'T' in end_time else end_time,
                'duration_seconds': duration
            })
    
    return videos

def group_related_videos(videos):
    """
    Group videos that are part of the same procedure.
    Videos are grouped if they share: procedure, room, surgeons, case date.
    This handles multiple camera feeds (V1, V2, V3) of the same procedure.
    """
    if not videos:
        return []
    
    # Sort by case date, procedure, and room
    sorted_videos = sorted(videos, key=lambda v: (
        v.get('case_date', ''),
        v.get('procedure_name', ''),
        v.get('room', ''),
        v.get('start_time', '')
    ))
    
    grouped = []
    current_group = [sorted_videos[0]]
    
    for i in range(1, len(sorted_videos)):
        current = sorted_videos[i]
        previous = sorted_videos[i-1]
        
        # Check if videos should be grouped (same procedure instance)
        same_procedure = current.get('procedure_name') == previous.get('procedure_name')
        same_room = current.get('room') == previous.get('room')
        same_case_date = current.get('case_date') == previous.get('case_date')
        same_surgeons = current.get('users') == previous.get('users')
        
        # Check if videos overlap or are sequential within 60 minutes
        # Compare current video's start time to previous video's END time
        time_close = False
        try:
            prev_end = previous.get('end_time', '')
            curr_start = current.get('start_time', '')
            
            if prev_end and curr_start:
                # Handle both full ISO format and time-only format
                if 'T' in prev_end and 'T' in curr_start:
                    from datetime import datetime
                    prev_end_dt = datetime.fromisoformat(prev_end)
                    curr_start_dt = datetime.fromisoformat(curr_start)
                    
                    # Time difference in minutes (can be negative if overlapping/simultaneous)
                    time_diff = (curr_start_dt - prev_end_dt).total_seconds() / 60
                    
                    # Group if current starts within 60 minutes after previous ends
                    # OR if they overlap (negative time_diff means simultaneous/overlapping)
                    time_close = time_diff <= 60
                else:
                    # If time parsing doesn't work, allow grouping
                    time_close = True
        except Exception as e:
            # If time parsing fails, still allow grouping based on other criteria
            time_close = True
        
        # If all criteria match, add to current group
        if same_procedure and same_room and same_case_date and same_surgeons and time_close:
            current_group.append(current)
        else:
            # Finalize current group and start new one
            grouped.append(current_group)
            current_group = [current]
    
    # Add the last group
    grouped.append(current_group)
    
    # Convert groups to combined video entries
    combined_videos = []
    for group in grouped:
        if len(group) == 1:
            # Single video, keep as is
            combined_videos.append(group[0])
        else:
            # Multiple videos (feeds), create combined entry
            combined = group[0].copy()
            
            # Identify feed types from filenames
            feed_names = []
            for v in group:
                filename = v.get('filename', '')
                # Extract feed name (e.g., "49V1.mp4" -> "V1")
                if 'V' in filename:
                    feed = filename.split('.')[0][-2:]  # Get last 2 chars before extension
                    feed_names.append(feed)
                else:
                    feed_names.append('?')
            
            # Combine info
            combined['is_concatenated'] = True
            combined['segment_count'] = len(group)
            combined['segments'] = group  # Store all segments
            combined['feed_names'] = feed_names
            combined['filename'] = f"{group[0]['procedure_name'][:30]}... ({', '.join(feed_names)})"
            
            # Use earliest start and latest end time
            all_start_times = [v.get('start_time', '') for v in group if v.get('start_time')]
            all_end_times = [v.get('end_time', '') for v in group if v.get('end_time')]
            combined['start_time'] = min(all_start_times) if all_start_times else 'N/A'
            combined['end_time'] = max(all_end_times) if all_end_times else 'N/A'
            
            # Sum durations (though they might overlap if simultaneous feeds)
            # Use max duration instead of sum for simultaneous feeds
            durations = [v.get('duration_seconds', 0) for v in group]
            combined['duration_seconds'] = max(durations) if durations else 0
            
            combined_videos.append(combined)
    
    return combined_videos

def download_video_from_s3(s3_key, local_path):
    """Download video from S3 to local path"""
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3_client.download_file(S3_BUCKET_NAME, s3_key, local_path)
        return True
    except Exception as e:
        st.error(f"Error downloading video: {e}")
        return False

def export_videos_to_directory(video_list, destination_dir):
    """Copy downloaded videos to export directory"""
    if not os.path.exists(destination_dir):
        st.error(f"Destination directory does not exist: {destination_dir}")
        return 0, len(video_list)
    
    exported = 0
    failed = 0
    
    for video in video_list:
        try:
            # Handle concatenated videos (multiple segments)
            if video.get('is_concatenated', False):
                for segment in video.get('segments', []):
                    local_path = f"data/temp_videos/{segment['s3_key'].replace('/', '_')}"
                    if os.path.exists(local_path):
                        dest_path = os.path.join(destination_dir, segment['filename'])
                        shutil.copy2(local_path, dest_path)
                        exported += 1
                    else:
                        failed += 1
            else:
                # Single video
                local_path = f"data/temp_videos/{video['s3_key'].replace('/', '_')}"
                if os.path.exists(local_path):
                    dest_path = os.path.join(destination_dir, video['filename'])
                    shutil.copy2(local_path, dest_path)
                    exported += 1
                else:
                    failed += 1
        except Exception as e:
            st.error(f"Error copying {video.get('filename', 'unknown')}: {e}")
            failed += 1
    
    return exported, failed

# Initialize session state
if 'video_list' not in st.session_state:
    st.session_state.video_list = []
if 'auth_token' not in st.session_state:
    st.session_state.auth_token = None

# Page config
st.set_page_config(page_title="Surgical Video Metadata Viewer", layout="wide")

st.title("🏥 Surgical Video Metadata Viewer")

# Sidebar for import
with st.sidebar:
    st.header("🔐 Authentication")
    if st.button("Authenticate", type="primary"):
        with st.spinner("Authenticating..."):
            token = get_auth_token()
            if token:
                st.session_state.auth_token = token
                st.success("✓ Authenticated successfully")
            else:
                st.error("Authentication failed")
    
    st.divider()
    
    st.header("📥 Fetch Videos from Explorer API")
    
    st.subheader("Filters")
    
    # Toggle between "My Surgeons" and "All Surgeons"
    filter_mode = st.radio(
        "Surgeon Filter:",
        options=["My Surgeons Only", "All Surgeons"],
        help="My Surgeons: Show only videos from your configured 6 surgeons"
    )
    
    # Store filter mode in session state so it's accessible in metadata display
    st.session_state.filter_mode = filter_mode

    # Show which surgeons are configured
    if filter_mode == "My Surgeons Only":
        st.info(f"Filtering by {len(MY_SURGEON_IDS)} configured surgeons")
    
    st.divider()
    
    # Date range selection (default: last 7 days)
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=(datetime.now() - timedelta(days=7)).date())
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())
    
    if st.button("Fetch Videos", disabled=not st.session_state.auth_token):
        if not st.session_state.auth_token:
            st.warning("Please authenticate first")
        else:
            with st.spinner("Fetching videos from Explorer API..."):
                start_str = start_date.strftime('%Y-%m-%d')
                end_str = end_date.strftime('%Y-%m-%d')
                
                video_data = fetch_videos_from_explorer(
                    start_str, 
                    end_str, 
                    st.session_state.auth_token
                )
                
                if video_data:
                    all_videos = parse_video_metadata(video_data)
                    
                    # Filter by case date first
                    all_videos = [v for v in all_videos
                                 if start_str <= v.get('case_date', '') <= end_str]
                    
                    # Group related videos (concatenate multi-feed procedures)
                    original_count = len(all_videos)
                    all_videos = group_related_videos(all_videos)
                    grouped_count = len(all_videos)

                    # Apply surgeon filter based on mode
                    if filter_mode == "My Surgeons Only":
                        filtered_videos = []
                        for v in all_videos:
                            video_users = v.get('users', [])
                            if isinstance(video_users, list):
                                if any(surgeon_id in video_users for surgeon_id in MY_SURGEON_IDS):
                                    filtered_videos.append(v)
                        
                        # Count total video files in filtered procedures
                        my_video_count = sum(v.get('segment_count', 1) for v in filtered_videos)
                        all_video_count = sum(v.get('segment_count', 1) for v in all_videos)
                        
                        st.success(f"✓ My Surgeons: {len(filtered_videos)} operations ({my_video_count} videos) | All Surgeons: {len(all_videos)} operations ({all_video_count} videos)")
                    else:
                        filtered_videos = all_videos
                        
                        # Count total video files
                        total_video_files = sum(v.get('segment_count', 1) for v in filtered_videos)
                        
                        st.success(f"✓ Fetched {len(filtered_videos)} operations ({total_video_files} videos) from all surgeons")
                    
                    st.session_state.video_list = filtered_videos
    
    st.divider()
    
    # Export section
    st.header("📤 Export")
    if st.session_state.video_list:
        export_name = st.text_input("Export name:", value="surgical_videos")
        
        # Video export destination (placeholder)
        st.subheader("Export Videos")
        export_video_dir = st.text_input(
            "Video destination directory:",
            value="//placeholder/path/to/destination",
            help="Destination folder for exported video files"
        )
        
        if st.button("📹 Export Videos to Directory"):
            # Check how many videos are downloaded
            downloaded_count = sum(1 for v in st.session_state.video_list 
                                  if os.path.exists(f"data/temp_videos/{v['filename']}"))
            
            if downloaded_count == 0:
                st.warning("⚠️ No videos have been downloaded yet. Preview videos first to download them.")
            elif export_video_dir == "//placeholder/path/to/destination":
                st.warning("⚠️ Please update the destination directory path first")
            else:
                with st.spinner("Copying videos to export directory..."):
                    exported, failed = export_videos_to_directory(st.session_state.video_list, export_video_dir)
                    
                    if exported > 0:
                        st.success(f"✓ Exported {exported} videos to {export_video_dir}")
                    if failed > 0:
                        st.warning(f"⚠️ {failed} videos not downloaded yet or failed to copy")
        
        st.divider()
        
        # JSON export
        st.subheader("Export Metadata")
        if st.button("📄 Export as JSON"):
            export_data = {
                "export_date": datetime.now().isoformat(),
                "video_count": len(st.session_state.video_list),
                "videos": st.session_state.video_list
            }
            
            os.makedirs("data/exports", exist_ok=True)
            filename = f"data/exports/{export_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(export_data, f, indent=2)
            
            st.success(f"✓ Exported to: {filename}")
            
            st.download_button(
                label="⬇️ Download JSON",
                data=json.dumps(export_data, indent=2),
                file_name=f"{export_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )

# Main content area
if st.session_state.video_list:
    st.subheader(f"📹 Video List ({len(st.session_state.video_list)} videos)")
    
    # Select video from list
    video_options = [f"{i+1}. {v['filename']} - {v['procedure_name']}" 
                     for i, v in enumerate(st.session_state.video_list)]
    selected_index = st.selectbox(
        "Select a video to preview:",
        range(len(video_options)),
        format_func=lambda x: video_options[x]
    )
    
    if selected_index is not None:
        selected_video = st.session_state.video_list[selected_index]
        
        # Two column layout: video player (left) + metadata (right)
        col1, col2 = st.columns([1.5, 2.5])
        
        with col1:
            st.subheader("🎥 Video Preview")
            
            # Check if this is a concatenated video (multiple feeds)
            if selected_video.get('is_concatenated', False):
                feed_names = selected_video.get('feed_names', [])
                st.info(f"📹 Multiple Feeds: {', '.join(feed_names)} ({selected_video['segment_count']} videos)")
                
                # Show each feed
                for idx, segment in enumerate(selected_video.get('segments', []), 1):
                    feed_name = feed_names[idx-1] if idx-1 < len(feed_names) else f"Feed {idx}"
                    st.markdown(f"**{feed_name}:** {segment['filename']}")
                    # Use S3 key to create unique local path
                    local_path = f"data/temp_videos/{segment['s3_key'].replace('/', '_')}"
                    
                    if os.path.exists(local_path):
                        st.video(local_path)
                    else:
                        if st.button(f"⬇️ Download Segment {idx}", key=f"download_seg_{selected_index}_{idx}"):
                            with st.spinner(f"Downloading segment {idx}..."):
                                if download_video_from_s3(segment['s3_key'], local_path):
                                    st.success(f"✓ Downloaded segment {idx}")
                                    st.video(local_path)
            else:
                # Single video (not concatenated)
                # Use S3 key to create unique local path
                local_path = f"data/temp_videos/{selected_video['s3_key'].replace('/', '_')}"
                
                # Always show which video should be playing
                st.caption(f"**File:** {selected_video['filename']}")
                
                if os.path.exists(local_path):
                    # Video is downloaded, show player
                    st.video(local_path)
                    st.success("✓ Video loaded")
                else:
                    # Video not downloaded yet
                    st.warning("⚠️ Video not downloaded - click below")
                    
                    if st.button("⬇️ Download & Preview", key=f"download_{selected_index}"):
                        with st.spinner("Downloading video from S3..."):
                            if download_video_from_s3(selected_video['s3_key'], local_path):
                                st.success("✓ Downloaded!")
                                st.rerun()
                                
        with col2:
            st.subheader("📋 Metadata")
            
            # Two column layout: labels on left, values on right
            meta_col_label, meta_col_value = st.columns([1, 2])
            
            # Get distinct specialties only
            specialties = selected_video.get('specialties', 'N/A')
            if specialties != 'N/A':
                specialty_list = [s.strip() for s in specialties.split(',')]
                # Remove duplicates while preserving order
                seen = set()
                distinct_specialties = []
                for s in specialty_list:
                    if s not in seen:
                        seen.add(s)
                        distinct_specialties.append(s)
                specialty_display = ', '.join(distinct_specialties)
            else:
                specialty_display = 'N/A'
            
            # Calculate video length in mm:ss format
            duration_seconds = selected_video.get('duration_seconds', 0)
            if duration_seconds and duration_seconds > 0:
                minutes = int(duration_seconds // 60)
                seconds = int(duration_seconds % 60)
                video_length = f"{minutes}:{seconds:02d}"
            else:
                video_length = 'N/A'

            # Determine what to show for EMR ID based on filter mode
            filter_mode = st.session_state.get('filter_mode', 'All Surgeons')
            video_users = selected_video.get('users', [])
            
            if filter_mode == "My Surgeons Only":
                # Show only the surgeons that match MY_SURGEON_IDS
                if isinstance(video_users, list):
                    matching_surgeons = [user for user in video_users if user in MY_SURGEON_IDS]
                    emr_display = ', '.join(matching_surgeons) if matching_surgeons else 'N/A'
                else:
                    emr_display = 'N/A'
            else:
                # All Surgeons mode - hide identity for privacy
                emr_display = '[Protected]'


            # Create aligned metadata table using HTML (smaller values text)
            metadata_html = f"""
            <table style="width:100%; border-collapse: collapse;">
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top; width: 20%;"><strong>Procedure:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{selected_video.get('procedure_name', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top;"><strong>Case Date:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{format_date(selected_video.get('case_date', 'N/A'))}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top;"><strong>Upload Date:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{format_date(selected_video.get('upload_date', 'N/A'))}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top;"><strong>Video Length:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{video_length}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top;"><strong>Specialty:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{specialty_display}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top;"><strong>Room:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{selected_video.get('room', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 16px 8px 0; vertical-align: top;"><strong>Relevant User:</strong></td>
                    <td style="padding: 8px 0; vertical-align: top; font-size: 0.9em;">{emr_display}</td>
                </tr>
            </table>
            """
            st.markdown(metadata_html, unsafe_allow_html=True)
            
            st.divider()
            
            # Full details in expandable section
            with st.expander("📄 Full Video Details", expanded=False):
                # Show file size if video is downloaded
                local_path = f"data/temp_videos/{selected_video['filename']}"
                if os.path.exists(local_path):
                    file_size_bytes = os.path.getsize(local_path)
                    file_size_gb = file_size_bytes / (1024**3)  # Convert to GB
                    st.markdown(f"**File Size:** {file_size_gb:.2f} GB")
                    st.divider()
                
                st.json(selected_video)
        
    st.divider()
    
    # Table view of all videos
    st.subheader("📊 All Videos in Current Selection")
    df = pd.DataFrame(st.session_state.video_list)
    
    # Format dates in dataframe
    if 'case_date' in df.columns:
        df['case_date'] = df['case_date'].apply(format_date)
    if 'upload_date' in df.columns:
        df['upload_date'] = df['upload_date'].apply(format_date)
    
        # Clean up specialties column to show only distinct values
        if 'specialties' in df.columns:
            def clean_specialties(spec_str):
                if not spec_str or spec_str == 'N/A':
                    return 'N/A'
                specialty_list = [s.strip() for s in spec_str.split(',')]
                # Remove duplicates while preserving order
                seen = set()
                distinct = []
                for s in specialty_list:
                    if s not in seen:
                        seen.add(s)
                        distinct.append(s)
                return ', '.join(distinct)
            
            df['specialties'] = df['specialties'].apply(clean_specialties)
        
        # Display columns from API metadata
        display_columns = ['filename', 'procedure_name', 'case_date', 'start_time','room', 'specialties']
        available_columns = [col for col in display_columns if col in df.columns]
        
        st.dataframe(df[available_columns], width='stretch', hide_index=True)

else:
    st.info("👈 Use the sidebar to authenticate and fetch videos from Explorer API")