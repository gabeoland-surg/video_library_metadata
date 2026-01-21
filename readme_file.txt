# Video Library (Local MVP)

A Streamlit-based video library management application with PHI (Protected Health Information) status tracking and tagging capabilities.

## Features

- **Video Indexing**: Scan folders to automatically index video files (MP4, MOV, M4V, AVI, MKV)
- **Search & Filter**: Search by filename/path and filter by tags or PHI status
- **PHI Tracking**: Mark videos as unknown, suspected, or cleared for PHI content
- **Tagging System**: Organize videos with custom tags
- **Video Playback**: Built-in video player for indexed files
- **Manifest Export**: Export filtered video lists to JSON manifests

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/video-library-mvp.git
cd video-library-mvp
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the Streamlit application:
```bash
streamlit run app.py
```

The app will open in your default web browser at `http://localhost:8501`

### Workflow

1. **Index Videos**: Use the sidebar to specify a folder path and click "Index videos"
2. **Browse**: Search and filter your video library
3. **Tag & Classify**: Select videos to add tags and set PHI status
4. **Export**: Generate manifest files for filtered video sets

## Data Storage

- **Database**: SQLite database stored in `data/videos.db`
- **Manifests**: Exported JSON files saved to `data/manifests/`
- **Thumbnails**: Reserved directory at `data/thumbnails/` (feature placeholder)

## PHI Compliance Warning

⚠️ This application is designed to handle Protected Health Information (PHI). Users must:
- Only use on approved systems
- Not copy, upload, or share videos outside authorized environments
- Follow all applicable HIPAA and organizational policies

## Technology Stack

- **Python 3.x**
- **Streamlit**: Web interface framework
- **SQLite**: Local database
- **Pandas**: Data manipulation

## Project Status

This is a local MVP (Minimum Viable Product) for video library management with PHI tracking capabilities.

## License

[Specify your license here]

## Contributing

[Add contribution guidelines if applicable]
