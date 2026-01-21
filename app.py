import json
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

APP_TITLE = "Video Library (Local MVP)"
DB_PATH = Path("data/videos.db")
MANIFEST_DIR = Path("data/manifests")
THUMB_DIR = Path("data/thumbnails")
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi", ".mkv"}

st.set_page_config(page_title=APP_TITLE, layout="wide")

def get_conn():
	DB_PATH.parent.mkdir(parents=True, exist_ok=True)
	return sqlite3.connect(DB_PATH)

def init_db():
	conn = get_conn()
	cur = conn.cursor()
	cur.execute("""
	CREATE TABLE IF NOT EXISTS videos (
		video_id INTEGER PRIMARY KEY AUTOINCREMENT,
		path TEXT UNIQUE NOT NULL,
		filename TEXT NOT NULL,
		ext TEXT,
		bytes INTEGER,
		mtime REAL,
		phi_status TEXT DEFAULT 'unknown'
	);
	""")
	cur.execute("""
	CREATE TABLE IF NOT EXISTS tags (
		tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT UNIQUE NOT NULL
	);
	""")
	cur.execute("""
	CREATE TABLE IF NOT EXISTS video_tags (
		video_id INTEGER NOT NULL,
		tag_id INTEGER NOT NULL,
		UNIQUE(video_id, tag_id)
	);
	""")

	# --- Migration: add phi_status column if it doesn't exist yet ---
	cur.execute("PRAGMA table_info(videos)")
	cols = [row[1] for row in cur.fetchall()]
	if "phi_status" not in cols:
		cur.execute("ALTER TABLE videos ADD COLUMN phi_status TEXT DEFAULT 'unknown'")
	else:
		cur.execute("UPDATE videos SET phi_status='unknown' WHERE phi_status IS NULL")

	conn.commit()
	conn.close()

def upsert_video(row: dict):
	conn = get_conn()
	cur = conn.cursor()
	cur.execute("""
	INSERT INTO videos (path, filename, ext, bytes, mtime)
	VALUES (:path, :filename, :ext, :bytes, :mtime)
	ON CONFLICT(path) DO UPDATE SET
		filename=excluded.filename,
		ext=excluded.ext,
		bytes=excluded.bytes,
		mtime=excluded.mtime
	;
	""", row)
	conn.commit()
	conn.close()

def list_videos(search: str = "", tag: str | None = None):
	conn = get_conn()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	params = {"q": f"%{search.strip()}%"}
	if tag and tag.strip():
		params["tag"] = tag.strip()
		cur.execute("""
		SELECT v.*
		FROM videos v
		JOIN video_tags vt ON vt.video_id = v.video_id
		JOIN tags t ON t.tag_id = vt.tag_id
		WHERE (v.filename LIKE :q OR v.path LIKE :q)
		  AND t.name = :tag
		ORDER BY v.mtime DESC
		""", params)
	else:
		cur.execute("""
		SELECT v.*
		FROM videos v
		WHERE (v.filename LIKE :q OR v.path LIKE :q)
		ORDER BY v.mtime DESC
		""", params)
	rows = [dict(r) for r in cur.fetchall()]
	conn.close()
	return rows

def get_all_tags():
	conn = get_conn()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	cur.execute("SELECT name FROM tags ORDER BY name ASC")
	tags = [r["name"] for r in cur.fetchall()]
	conn.close()
	return tags

def get_video(video_id: int):
	conn = get_conn()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	cur.execute("SELECT * FROM videos WHERE video_id = ?", (video_id,))
	r = cur.fetchone()
	conn.close()
	return dict(r) if r else None

def get_video_tags(video_id: int):
	conn = get_conn()
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()
	cur.execute("""
	SELECT t.name
	FROM tags t
	JOIN video_tags vt ON vt.tag_id = t.tag_id
	WHERE vt.video_id = ?
	ORDER BY t.name ASC
	""", (video_id,))
	tags = [r["name"] for r in cur.fetchall()]
	conn.close()
	return tags

def set_video_tags(video_id: int, tag_names: list[str]):
	tag_names = sorted({t.strip() for t in tag_names if t.strip()})
	conn = get_conn()
	cur = conn.cursor()

	for t in tag_names:
		cur.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (t,))

	cur.execute("DELETE FROM video_tags WHERE video_id = ?", (video_id,))

	if tag_names:
		cur.execute("SELECT tag_id, name FROM tags WHERE name IN ({})".format(
			",".join(["?"] * len(tag_names))
		), tag_names)
		tag_id_map = {name: tag_id for tag_id, name in cur.fetchall()}
		for name in tag_names:
			cur.execute("INSERT OR IGNORE INTO video_tags (video_id, tag_id) VALUES (?, ?)",
						(video_id, tag_id_map[name]))

	conn.commit()
	conn.close()

def find_videos(root: Path):
	for p in root.rglob("*"):
		if p.is_file() and p.suffix.lower() in VIDEO_EXTS:
			yield p

def fmt_bytes(n):
	if n is None:
		return ""
	for unit in ["B", "KB", "MB", "GB", "TB"]:
		if n < 1024:
			return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
		n /= 1024
	return f"{n:.1f} PB"

def fmt_dt(ts):
	if not ts:
		return ""
	return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

@st.cache_data(ttl=2)
def cached_list_videos(search: str, tag: str | None):
	return list_videos(search=search, tag=tag)


# --- App ---
init_db()
MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
THUMB_DIR.mkdir(parents=True, exist_ok=True)

st.title(APP_TITLE)
st.warning(
	"⚠️ This video library may contain Protected Health Information (PHI). "
	"Do not copy, upload, or share videos outside approved systems."
)


with st.sidebar:
	st.header("Ingest")
	root = st.text_input("Folder to index", value=r"D:/")
	make_thumbs = st.checkbox("Generate thumbnails (slower first time)", value=True)
	run_ingest = st.button("Index videos", use_container_width=True)

	st.divider()
	st.header("Browse")
	q = st.text_input("Search filename/path", value="")

	phi_opts = ["(any)", "unknown", "suspected", "cleared"]
	phi_choice = st.selectbox("Filter by PHI status", options=phi_opts, index=0)

	tags = ["(any)"] + get_all_tags()
	tag_choice = st.selectbox("Filter by tag", options=tags, index=0)

	st.divider()
	st.header("Export")
	export_tag = st.selectbox("Manifest tag filter", options=["(any)"] + get_all_tags(), index=0)
	exclude_suspected = st.checkbox("Exclude suspected PHI (recommended)", value=True)
	preview_btn = st.button("Preview manifest", use_container_width=True)
	export_btn = st.button("Export manifest.json", use_container_width=True)


if run_ingest:
	root_path = Path(root)
	if not root_path.exists():
		st.error("Folder not found.")
	else:
		vids = list(find_videos(root_path))
		st.info(f"Found {len(vids)} videos. Indexing…")
		prog = st.progress(0)
		for i, p in enumerate(vids, start=1):
			try:
				stat = p.stat()
				upsert_video({
					"path": str(p.resolve()),
					"filename": p.name,
					"ext": p.suffix.lower(),
					"bytes": int(stat.st_size),
					"mtime": float(stat.st_mtime),
				})
			except Exception as e:
				st.warning(f"Failed: {p} — {e}")
			prog.progress(i / max(len(vids), 1))
		st.success("Done indexing.")

tag_filter = None if tag_choice == "(any)" else tag_choice
rows = cached_list_videos(search=q, tag=tag_filter)
if phi_choice != "(any)":
	rows = [r for r in rows if (r.get("phi_status") or "unknown") == phi_choice]
df = pd.DataFrame(rows)

left, right = st.columns([1.25, 1])

with left:
	st.subheader("Library")
	if df.empty:
		st.write("No videos indexed yet. Use the sidebar to index a folder.")
		selected_id = None
	else:
		view = df.copy()
		view["size"] = view["bytes"].apply(fmt_bytes)
		view["modified"] = view["mtime"].apply(fmt_dt)

		# Tags column (comma-separated)
		tag_map = {}
		for r in rows:
			tag_map[r["video_id"]] = get_video_tags(r["video_id"])
		view["tags"] = view["video_id"].map(lambda vid: ", ".join(tag_map.get(vid, [])))

		# Ensure phi_status shows up even if missing
		if "phi_status" not in view.columns:
			view["phi_status"] = "unknown"

		view = view[["video_id", "phi_status", "tags", "filename", "ext", "size", "modified", "path"]]

		event = st.dataframe(
			view,
			use_container_width=True,
			hide_index=True,
			on_select="rerun",
			selection_mode="single-row",
		)

		selected_id = int(view["video_id"].iloc[0])

		if event and event.selection and event.selection.get("rows"):
			row_idx = event.selection["rows"][0]
			selected_id = int(view.iloc[row_idx]["video_id"])

		st.caption(f"Selected video_id: {selected_id}")

with right:
	st.subheader("Video Detail")
	if not selected_id:
		st.write("Index videos first.")
	else:
		vid = get_video(int(selected_id))
		if not vid:
			st.write("Pick a valid video ID.")
		else:
			st.write(f"**{vid['filename']}**")
			st.caption(vid["path"])

			p = Path(vid["path"])
			if p.exists():
				if st.button("Load video", use_container_width=True):
					st.video(str(p))
				else:
					st.info("Click **Load video** to open the player (prevents slow auto-load).")
			else:
				st.warning("File not found (moved/deleted?).")
		
	st.write("**PHI status**")
	status_options = ["unknown", "suspected", "cleared"]
	current_status = vid.get("phi_status") or "unknown"
	new_status = st.selectbox("Set PHI status", status_options, index=status_options.index(current_status))

	if st.button("Save PHI status", use_container_width=True):
		conn = get_conn()
		cur = conn.cursor()
		cur.execute("UPDATE videos SET phi_status=? WHERE video_id=?", (new_status, vid["video_id"]))
		conn.commit()
		conn.close()
		st.success("PHI status saved.")

	st.write("**Tags**")
	current = get_video_tags(vid["video_id"])
	all_tags = get_all_tags()
	new_tags = st.multiselect("Select tags", options=all_tags, default=current)

	add = st.text_input("Add a new tag")
	if add.strip():
		new_tags = sorted(set(new_tags + [add.strip()]))

	if st.button("Save tags", use_container_width=True):
		set_video_tags(vid["video_id"], new_tags)
		st.success("Saved.")

if preview_btn or export_btn:
	export_tag_filter = None if export_tag == "(any)" else export_tag
	export_rows = list_videos(search="", tag=export_tag_filter)

	if exclude_suspected:
		export_rows = [r for r in export_rows if (r.get("phi_status") or "unknown") != "suspected"]

	manifest = {
		"created_at": datetime.now().isoformat(),
		"filter_tag": export_tag_filter,
		"exclude_suspected_phi": exclude_suspected,
		"count": len(export_rows),
		"videos": [
			{
				"video_id": r["video_id"],
				"path": r["path"],
				"filename": r["filename"],
				"phi_status": r.get("phi_status", "unknown"),
				"tags": get_video_tags(r["video_id"]),
				"bytes": r.get("bytes"),
				"mtime": r.get("mtime"),
			}
			for r in export_rows
		],
	}

	if preview_btn:
		st.subheader("Manifest preview")
		st.json(manifest)

	if export_btn:
		out = MANIFEST_DIR / f"manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
		out.write_text(json.dumps(manifest, indent=2))
		st.success(f"Exported: {out}")
