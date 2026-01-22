import streamlit as st
import json
from datetime import datetime
import os
import boto3
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse
import pandas as pd

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
        
        # Check if users is a dictionary or list
        if isinstance(users, dict):
            for key, value in users.items():
                if key.startswith('EMRID') and value:
                    surgeon_ids.append(f"{key}: {value}")
        elif isinstance(users, list):
            # If users is a list, just join them
            surgeon_ids = [str(u) for u in users if u]
        
        surgeon_id_str = ', '.join(surgeon_ids) if surgeon_ids else 'N/A'
        
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
                'surgeon_ids': surgeon_id_str,
                'users': users,  # Keep raw users dict for filtering
                'start_time': start_time.split('T')[1] if 'T' in start_time else start_time,
                'end_time': end_time.split('T')[1] if 'T' in end_time else end_time,
                'duration_seconds': duration
            })
    
    return videos

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
    surgeon_ids = st.text_area(
        "Surgeon EMR IDs (one per line):",
        placeholder="12345\n67890\n11223",
        help="Leave empty to fetch all surgeons"
    )
    
    use_case_date = st.checkbox(
        "Filter by case date (not upload date)",
        value=True,
        help="When checked, date range filters by when surgery occurred, not when video was uploaded"
    )
    
    st.divider()
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())
    
    if st.button("Fetch Videos", disabled=not st.session_state.auth_token):
        if not st.session_state.auth_token:
            st.warning("Please authenticate first")
        else:
            with st.spinner("Fetching videos from Explorer API..."):
                start_str = start_date.strftime('%Y-%m-%d')
                end_str = end_date.strftime('%Y-%m-%d')
                
                # Parse surgeon IDs
                surgeon_list = [s.strip() for s in surgeon_ids.split('\n') if s.strip()] if surgeon_ids else []
                
                video_data = fetch_videos_from_explorer(
                    start_str, 
                    end_str, 
                    st.session_state.auth_token
                )
                
                if video_data:
                    all_videos = parse_video_metadata(video_data)
                    
					# Filter by surgeon ID if specified
                    if surgeon_list:
                        filtered_videos = []
                        for v in all_videos:
                            video_users = v.get('users', [])
                            
							# Check if any of the surgeon IDs match any user in the video
                            if isinstance(video_users, list):
                                if any(surgeon_id in video_user for surgeon_id in surgeon_list for video_user in video_users):
                                    filtered_videos.append(v)
                            else:
                                filtered_videos.append(v)

                            # Get user values based on type
                            if isinstance(users, dict):
                                user_values = [val for key, val in users.items() if key.startswith('EMRID')]
                            elif isinstance(users, list):
                                user_values = users
                            else:
                                user_values = []
                            
                            if any(surgeon_id in str(val) for val in user_values for surgeon_id in surgeon_list):
                                filtered_videos.append(v)
                    else:
                        filtered_videos = all_videos
                    
                    # Filter by case date if selected
                    if use_case_date:
                        filtered_videos = [v for v in filtered_videos
                                         if start_str <= v.get('case_date', '') <= end_str]
                    
                    st.session_state.video_list = filtered_videos
                    st.success(f"✓ Fetched {len(filtered_videos)} videos (from {len(all_videos)} total)")
                else:
                    st.info("No videos found for this date range")
    
    st.divider()
    
    # Export section
    st.header("📤 Export")
    if st.session_state.video_list:
        export_name = st.text_input("Export name:", value="surgical_videos")
        
        if st.button("Export as JSON"):
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
        
        # Two column layout: video player (left 1/4) + metadata (right 3/4)
        col1, col2 = st.columns([1, 3])
        
        with col1:
            st.subheader("🎥 Video Preview")
            
            # Video stored in S3 - show location
            st.info("Video stored in S3")
            st.caption(f"{selected_video.get('s3_location', 'N/A')}")
            
            # Video ID (S3 key)
            st.markdown("**Video ID:**")
            st.markdown(f"<small>{selected_video.get('s3_key', 'N/A')}</small>", unsafe_allow_html=True)
            
            # Optional: Add download button for future implementation
            if st.button("⬇️ Download from S3 (Coming soon)", disabled=True):
                st.info("Download feature coming soon")
        
        with col2:
            st.subheader("📋 Metadata")
            
            # Display metadata from API
            col_a, col_b, col_c = st.columns(3)
            
            with col_a:
                st.metric("Procedure", selected_video.get('procedure_name', 'N/A'))
                st.metric("Specialty", selected_video.get('specialties', 'N/A'))
            
            with col_b:
                st.metric("Case Date", format_date(selected_video.get('case_date', 'N/A')))
                st.metric("Room", selected_video.get('room', 'N/A'))
            
            with col_c:
                st.metric("Upload Date", format_date(selected_video.get('upload_date', 'N/A')))
                st.metric("EMR ID", selected_video.get('surgeon_ids', 'N/A'))
            
            st.divider()
            
            # Full details in expandable section
            with st.expander("📄 Full Video Details", expanded=False):
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
        
        # Display columns from API metadata
        display_columns = ['filename', 'procedure_name', 'case_date', 'room', 'specialties']
        available_columns = [col for col in display_columns if col in df.columns]
        
        st.dataframe(df[available_columns], use_container_width=True, hide_index=True)

else:
    st.info("👈 Use the sidebar to authenticate and fetch videos from Explorer API")