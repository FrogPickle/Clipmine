# ClipMine

ClipMine is a local-first tool for building a **searchable database of transcripts** from YouTube videos.  
The goal is to make it easy to collect, index, and search through public figures’ statements, with room to grow into multi-project archives.

---

## Features
- Full-text search (FTS5 in SQLite) across transcript segments.
- Metadata support (title, channel, publish date, duration).
- Simple web interface built with Flask (`app.py` + `templates/`).
- Organized folder layout for data, templates, and experiments.

---

## Folder Structure

clipmine/
├── app.py # Main Flask app (search UI and API endpoints)
├── tscripter.py # Transcript utilities
├── archive/ # Old/experimental scripts (kept for reference)
│ └── ... (init, import, verify, etc.)
├── data/ # Database and transcript storage
│ ├── clipmine.db # Primary SQLite database
│ ├── clipmine_backup.db # Backup copy
│ ├── transcripts/ # Per-video transcript JSONs (<video_id>.json)
│ ├── approved.json # Legacy JSON state (pre-SQLite)
│ ├── pending.json
│ ├── rejected.json
│ ├── failed.json
│ ├── seen_ids.json
│ └── search_index.json
├── templates/ # HTML templates for Flask
│ ├── approved.html
│ ├── index.html
│ ├── results.html
│ └── review.html
├── .gitignore # Ignores apikey.txt and other local-only files
└── apikey.txt # Local API key (not tracked in Git)


## Usage
- Start the app:
  ```bash
  python app.py
Open http://127.0.0.1:5000 to access the search interface.

Transcripts and metadata are managed through the database (data/clipmine.db).

Experimental / one-off scripts live under archive/ and aren’t needed for normal operation.

Roadmap
Channel filter & blocklist.

Date range / duration filters.

Replace button for duplicates.

Multiple projects (per-user archives).

Export / backup options from UI.

Optional public-facing site (community search).