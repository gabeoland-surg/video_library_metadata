# %% [markdown]
# # Weekly Surgical Video Export Automation
# 
# This notebook automatically fetches surgical videos from the Explorer API and exports them.
# Can be run manually or scheduled to run weekly via Task Scheduler.

# %% [markdown]
## Setup and Configuration

# %%
import os
import json
import requests
import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from urllib.parse import urlparse

# Load environment variables
load_dotenv()

# %% [markdown]
## Configuration Variables

# %%
# API Configuration
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'insights-prod-media-bucket')
AUTH_URL = os.getenv('AUTH_URL', 'https://api.accounts.surgicalsafety.com/oauth/v1/token')
EXPLORER_API_URL = os.getenv('EXPLORER_API_URL', 'https://api.blackbox.surgicalsafety.com/api/explorer/v2/export')

# Export Configuration
OUTPUT_DIR = 'data/weekly_exports'
VIDEO_EXPORT_DIR = '//placeholder/path/to/destination'  # UPDATE THIS PATH
SURGEON_FILTER = []  # Add EMR IDs here to filter, e.g., ['EMRID1', 'EMRID2'], or leave empty for all

# Date range: Last 7 days by default
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=7)

# Toggle: Filter by case date (True) or upload date (False)
USE_CASE_DATE = True

print(f"Configuration loaded successfully")
print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")

# %% [markdown]
## Helper Functions

# %%
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
        token = response.json()['accessToken']
        print("✓ Authentication successful")
        return token
    except Exception as e:
        print(f"✗ Authentication failed: {e}")
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
        
        data = response.json()
        print(f"✓ Fetched {len(data)} cases from API")
        return data
    except Exception as e:
        print(f"✗ Error fetching videos: {e}")
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
        
        # Extract users/surgeon EMR IDs
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
                'surgeon_ids': surgeon_id_str,
                'users': users,
                'start_time': start_time.split('T')[1] if 'T' in start_time else start_time,
                'end_time': end_time.split('T')[1] if 'T' in end_time else end_time,
                'duration_seconds': duration
            })
    
    print(f"✓ Parsed {len(videos)} video files")
    return videos

def filter_videos(videos, surgeon_filter, use_case_date, start_date_str, end_date_str):
    """Apply filters to video list"""
    filtered = videos
    
    # Filter by surgeon if specified
    if surgeon_filter:
        filtered = []
        for v in videos:
            video_users = v.get('users', [])
            if isinstance(video_users, list):
                if any(surgeon_id in video_users for surgeon_id in surgeon_filter):
                    filtered.append(v)
        print(f"✓ Filtered by surgeon: {len(filtered)} videos match")
    
    # Filter by case date if specified
    if use_case_date:
        filtered = [v for v in filtered
                   if start_date_str <= v.get('case_date', '') <= end_date_str]
        print(f"✓ Filtered by case date: {len(filtered)} videos")
    
    return filtered

def export_metadata_json(videos, output_dir):
    """Export video metadata to JSON"""
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(output_dir, f'video_export_{timestamp}.json')
    
    export_data = {
        'export_date': datetime.now().isoformat(),
        'date_range': {
            'start': START_DATE.strftime('%Y-%m-%d'),
            'end': END_DATE.strftime('%Y-%m-%d')
        },
        'surgeon_filter': SURGEON_FILTER,
        'use_case_date': USE_CASE_DATE,
        'video_count': len(videos),
        'videos': videos
    }
    
    with open(filename, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    print(f"✓ Exported metadata to: {filename}")
    return filename

def download_videos(videos, output_dir):
    """Download videos from S3 to local directory"""
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    
    os.makedirs(output_dir, exist_ok=True)
    
    downloaded = 0
    failed = 0
    
    for i, video in enumerate(videos, 1):
        try:
            s3_key = video['s3_key']
            filename = video['filename']
            local_path = os.path.join(output_dir, filename)
            
            print(f"Downloading {i}/{len(videos)}: {filename}...", end=' ')
            s3_client.download_file(S3_BUCKET_NAME, s3_key, local_path)
            print("✓")
            downloaded += 1
            
        except Exception as e:
            print(f"✗ Failed: {e}")
            failed += 1
    
    print(f"\n✓ Downloaded {downloaded} videos")
    if failed > 0:
        print(f"✗ Failed to download {failed} videos")
    
    return downloaded, failed

# %% [markdown]
## Main Execution

# %%
def main():
    """Main workflow"""
    print("=" * 60)
    print(f"WEEKLY VIDEO EXPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Step 1: Authenticate
    print("\n[1/5] Authenticating...")
    token = get_auth_token()
    if not token:
        print("Exiting due to authentication failure")
        return
    
    # Step 2: Fetch videos from API
    print("\n[2/5] Fetching videos from Explorer API...")
    start_str = START_DATE.strftime('%Y-%m-%d')
    end_str = END_DATE.strftime('%Y-%m-%d')
    video_data = fetch_videos_from_explorer(start_str, end_str, token)
    
    if not video_data:
        print("No videos found. Exiting.")
        return
    
    # Step 3: Parse metadata
    print("\n[3/5] Parsing video metadata...")
    all_videos = parse_video_metadata(video_data)
    
    # Step 4: Apply filters
    print("\n[4/5] Applying filters...")
    filtered_videos = filter_videos(all_videos, SURGEON_FILTER, USE_CASE_DATE, start_str, end_str)
    
    # Step 5: Export
    print("\n[5/5] Exporting...")
    
    # Export metadata JSON
    json_file = export_metadata_json(filtered_videos, OUTPUT_DIR)
    
    # Export videos (optional - can be slow)
    download_choice = input("\nDownload videos to local directory? (y/n): ").lower()
    if download_choice == 'y':
        print(f"Downloading videos to: {VIDEO_EXPORT_DIR}")
        downloaded, failed = download_videos(filtered_videos, VIDEO_EXPORT_DIR)
    else:
        print("Skipping video download")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total videos found: {len(all_videos)}")
    print(f"After filtering: {len(filtered_videos)}")
    print(f"Metadata exported to: {json_file}")
    print("=" * 60)
    
    return filtered_videos

# %% [markdown]
## Run the Export

# %%
# Execute the workflow
results = main()

# %% [markdown]
## View Results (Optional)

# %%
# Display results as DataFrame
if results:
    df = pd.DataFrame(results)
    display_columns = ['filename', 'procedure_name', 'case_date', 'room', 'surgeon_ids']
    print("\nFiltered Videos:")
    print(df[display_columns].to_string(index=False))

# %% [markdown]
## Scheduling Instructions
# 
# ### To run this notebook automatically every week:
# 
# #### Option 1: Windows Task Scheduler
# 
# 1. Convert notebook to Python script:
# ```bash
# jupyter nbconvert --to script weekly_video_export.ipynb
# ```
# 
# 2. Create batch file `run_weekly_export.bat`:
# ```batch
# @echo off
# cd C:\video_library_metadata
# call .venv\Scripts\activate
# python weekly_video_export.py
# pause
# ```
# 
# 3. Schedule in Task Scheduler:
#    - Open Task Scheduler
#    - Create Basic Task
#    - Name: "Weekly Surgical Video Export"
#    - Trigger: Weekly, Sunday 11:00 PM
#    - Action: Start program → `C:\video_library_metadata\run_weekly_export.bat`
# 
# #### Option 2: Using Papermill (Parameterized execution)
# 
# ```bash
# pip install papermill
# 
# # Run with custom parameters
# papermill weekly_video_export.ipynb output.ipynb \
#   -p START_DATE "2025-01-15" \
#   -p END_DATE "2025-01-22"
# ```
# 
# #### Option 3: Cron (Linux/Mac)
# 
# ```bash
# # Edit crontab
# crontab -e
# 
# # Add this line (runs every Sunday at 11 PM)
# 0 23 * * 0 cd /path/to/video_library_metadata && .venv/bin/python weekly_video_export.py
# ```
