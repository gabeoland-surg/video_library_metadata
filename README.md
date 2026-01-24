# Surgical Video Metadata Viewer

A Streamlit-based application for fetching and reviewing surgical video metadata from the Surgical Safety Technologies Black Box Platform API.

## Features

- **API Authentication**: Secure authentication with Black Box Platform API
- **Video Fetching**: Retrieve surgical videos from AWS S3 via Explorer API
- **Metadata Display**: View procedure details including:
  - Procedure name and specialty
  - Case date and upload date
  - Operating room
  - Surgeon EMR IDs
- **Filtering**: Filter videos by:
  - Date range (case date or upload date)
  - Surgeon EMR ID
- **Export**: Export filtered video lists to JSON format

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/video-library-mvp.git
cd video-library-mvp
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. **Request `.env` file from project owner** containing:
   - Black Box Platform API credentials (CLIENT_ID, CLIENT_SECRET)
   - AWS S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

5. Place the `.env` file in the project root directory

## Usage

Run the Streamlit application:
```bash
streamlit run app.py
```

The app will open in your default web browser at `http://localhost:8501`

### Workflow

1. **Authenticate**: Click "Authenticate" in the sidebar to get an API access token
2. **Set Filters**: 
   - Optionally enter Surgeon EMR IDs (one per line) to filter by specific surgeons
   - Choose whether to filter by case date or upload date
3. **Select Date Range**: Choose start and end dates
4. **Fetch Videos**: Click "Fetch Videos" to retrieve video metadata from the API
5. **Review**: Browse videos, view metadata, and verify footage details
6. **Export**: Export filtered video list as JSON

## Data Storage

- **Exports**: JSON files saved to `data/exports/`
- **Temp Videos**: Downloaded videos stored in `data/temp_videos/` (feature placeholder)

## Security & Compliance

⚠️ **PHI Warning**: This application handles Protected Health Information (PHI). Users must:
- Only use on approved systems
- Not share credentials or `.env` files publicly
- Follow all applicable HIPAA and organizational policies
- Never commit the `.env` file to version control (already in `.gitignore`)

## Technology Stack

- **Python 3.8+**
- **Streamlit**: Web interface framework
- **Boto3**: AWS S3 integration
- **Requests**: API communication
- **Pandas**: Data manipulation
- **python-dotenv**: Environment variable management

## API Information

This application connects to:
- **Authentication API**: `https://api.accounts.surgicalsafety.com/oauth/v1/token`
- **Explorer API**: `https://api.blackbox.surgicalsafety.com/api/explorer/v2/export`
- **S3 Bucket**: `insights-prod-media-bucket`

## License

[Specify your license here]

## Support

For API credentials or access issues, contact the project administrator.