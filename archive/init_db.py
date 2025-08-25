# init_db.py
import os, sqlite3, textwrap

DB_PATH = os.path.join("data", "clipmine.db")
os.makedirs("data", exist_ok=True)

schema = textwrap.dedent("""
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- projects
CREATE TABLE IF NOT EXISTS projects (
  id            INTEGER PRIMARY KEY,
  slug          TEXT UNIQUE NOT NULL,
  name          TEXT NOT NULL,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
  search_before TEXT,
  visible_slots INTEGER DEFAULT 5,
  notes         TEXT
);

-- channels (cache)
CREATE TABLE IF NOT EXISTS channels (
  id           TEXT PRIMARY KEY,  -- YouTube channelId
  title        TEXT,
  handle       TEXT,
  subs         INTEGER,
  last_seen_at TEXT
);

-- blocklists (NULL project_id = global)
CREATE TABLE IF NOT EXISTS blocklists (
  id         INTEGER PRIMARY KEY,
  project_id INTEGER,
  channel_id TEXT NOT NULL,
  reason     TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(project_id, channel_id)
);
CREATE INDEX IF NOT EXISTS idx_blocklists_project ON blocklists(project_id);

-- clusters (near-duplicate sets)
CREATE TABLE IF NOT EXISTS clusters (
  id            INTEGER PRIMARY KEY,
  project_id    INTEGER NOT NULL,
  rep_signature TEXT,
  title_hint    TEXT,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_clusters_project ON clusters(project_id);

-- transcripts (one per video/source)
CREATE TABLE IF NOT EXISTS transcripts (
  id            INTEGER PRIMARY KEY,
  project_id    INTEGER NOT NULL,
  cluster_id    INTEGER,
  source_id     TEXT NOT NULL,     -- YouTube videoId or URL key
  channel_id    TEXT,
  title         TEXT,
  published_at  TEXT,              -- RFC3339
  duration_sec  INTEGER,
  signature     TEXT,
  status        TEXT CHECK(status IN ('visible','throwaway','defective','trash')) DEFAULT 'visible',
  visible_rank  INTEGER,
  path_json     TEXT,              -- disk path to full transcript JSON
  path_txt      TEXT,              -- disk path to plain text
  quality_score INTEGER,           -- 0..100
  quality_flags TEXT,              -- JSON string
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_transcripts_proj_cluster ON transcripts(project_id, cluster_id);
CREATE INDEX IF NOT EXISTS idx_transcripts_status ON transcripts(project_id, cluster_id, status, visible_rank);
CREATE UNIQUE INDEX IF NOT EXISTS unq_transcripts_project_source ON transcripts(project_id, source_id);

-- segments (line-level, with speaker tags)
CREATE TABLE IF NOT EXISTS segments (
  id            INTEGER PRIMARY KEY,
  transcript_id INTEGER NOT NULL,
  start_ms      INTEGER NOT NULL,
  end_ms        INTEGER NOT NULL,
  speaker_role  TEXT CHECK(speaker_role IN ('subject','journalist','host','audience','network_intro','other')) NOT NULL,
  speaker_name  TEXT,
  text          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_segments_transcript ON segments(transcript_id, start_ms);
CREATE INDEX IF NOT EXISTS idx_segments_role ON segments(transcript_id, speaker_role);

-- FTS5 index over segments.text
CREATE VIRTUAL TABLE IF NOT EXISTS fts_segments USING fts5(
  transcript_id UNINDEXED,
  speaker_role  UNINDEXED,
  text,
  content=''
);

-- Triggers to keep FTS in sync
CREATE TRIGGER IF NOT EXISTS seg_ai AFTER INSERT ON segments BEGIN
  INSERT INTO fts_segments(rowid, transcript_id, speaker_role, text)
  VALUES (new.id, new.transcript_id, new.speaker_role, new.text);
END;
CREATE TRIGGER IF NOT EXISTS seg_ad AFTER DELETE ON segments BEGIN
  INSERT INTO fts_segments(fts_segments, rowid, transcript_id, speaker_role, text)
  VALUES('delete', old.id, old.transcript_id, old.speaker_role, old.text);
END;
CREATE TRIGGER IF NOT EXISTS seg_au AFTER UPDATE ON segments BEGIN
  INSERT INTO fts_segments(fts_segments, rowid, transcript_id, speaker_role, text)
  VALUES('delete', old.id, old.transcript_id, old.speaker_role, old.text);
  INSERT INTO fts_segments(rowid, transcript_id, speaker_role, text)
  VALUES (new.id, new.transcript_id, new.speaker_role, new.text);
END;

-- flags & ratings
CREATE TABLE IF NOT EXISTS flags (
  id            INTEGER PRIMARY KEY,
  project_id    INTEGER NOT NULL,
  transcript_id INTEGER,
  segment_id    INTEGER,
  flag_type     TEXT CHECK(flag_type IN ('wrong_speaker','fake_video','mismatch_transcript','prune_requested','other')) NOT NULL,
  note          TEXT,
  actor         TEXT,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_flags_targets ON flags(project_id, transcript_id, segment_id, flag_type);

CREATE TABLE IF NOT EXISTS ratings (
  id            INTEGER PRIMARY KEY,
  project_id    INTEGER NOT NULL,
  transcript_id INTEGER,
  video_quality INTEGER CHECK(video_quality BETWEEN 1 AND 5),
  audio_quality INTEGER CHECK(audio_quality BETWEEN 1 AND 5),
  authenticity  INTEGER CHECK(authenticity  BETWEEN 1 AND 5),
  reliability   INTEGER CHECK(reliability   BETWEEN 1 AND 5),
  pruning       INTEGER CHECK(pruning       BETWEEN 1 AND 5),
  actor         TEXT,
  created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ratings_target ON ratings(project_id, transcript_id);
""")

with sqlite3.connect(DB_PATH) as con:
    con.executescript(schema)

print(f"✓ Created/updated schema at {DB_PATH}")
