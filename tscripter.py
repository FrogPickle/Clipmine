# tscripter.py
# Fetch transcripts for approved videos and build a global search index.
# - Reads:  data/approved.json
# - Writes: data/transcripts/{video_id}.json, data/search_index.json, data/seen_ids.json
# - Auto-removes processed IDs from approved.json (queue-drain, crash-safe)

import os, json, time, random
from typing import List, Dict, Tuple, Any
from datetime import datetime

APPROVED_PATH = "data/approved.json"
TRANSCRIPTS_DIR = "data/transcripts"
SEARCH_INDEX_PATH = "data/search_index.json"
SEEN_PATH = "data/seen_ids.json"
import re

_YT_ID_RE = re.compile(r'(?:(?<=v=)|(?<=/v/)|(?<=/embed/)|(?<=youtu\.be/))([A-Za-z0-9_-]{11})')

def _extract_vid(s: str | None) -> str | None:
    """Return canonical 11-char YouTube ID from an ID or a URL (watch, youtu.be, embed)."""
    if not s:
        return None
    s = str(s).strip()
    # exact 11-char ID
    if re.fullmatch(r'[A-Za-z0-9_-]{11}', s):
        return s
    # try to pull from URL
    m = _YT_ID_RE.search(s)
    return m.group(1) if m else None

def _vid_of(entry) -> str | None:
    """Get the canonical video id from an approved.json entry (string or dict)."""
    if isinstance(entry, str):
        return _extract_vid(entry)
    if isinstance(entry, dict):
        # try common keys
        for k in ("video_id","videoId","id","url","link"):
            if k in entry:
                v = _extract_vid(entry[k])
                if v: return v
    return None

def _remove_id_all(entries: list, video_id: str) -> tuple[list, int]:
    """Remove ALL entries (strings or dicts) that refer to video_id (after normalization)."""
    target = _extract_vid(video_id)
    if not target:
        return entries, 0
    kept, removed = [], 0
    for e in entries:
        vid = _vid_of(e)
        if vid and vid == target:
            removed += 1
        else:
            kept.append(e)
    return kept, removed

def _dedupe_by_id(entries: list) -> list:
    seen, out = set(), []
    for e in entries:
        vid = _vid_of(e)
        if not vid or vid in seen:
            continue
        seen.add(vid)
        out.append(e)
    return out


# ---------- tiny fs helpers ----------
def _read_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return default

def _write_json_atomic(path: str, obj):
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

# ---------- approved queue helpers ----------
def _extract_ids(approved_obj) -> List[str]:
    """Accepts:
       - ["abc123", "def456"]
       - [{"videoId": "abc123"}, {"videoId":"def456"}]
       - [{"id":"abc123"}]  (fallback)
    """
    ids = []
    if isinstance(approved_obj, list):
        for item in approved_obj:
            if isinstance(item, str):
                ids.append(item)
            elif isinstance(item, dict):
                vid = item.get("videoId") or item.get("id") or item.get("video_id")
                if vid:
                    ids.append(vid)
    return ids

def _remove_id(approved_obj, vid: str):
    if isinstance(approved_obj, list):
        return [
            x for x in approved_obj
            if not (x == vid or (isinstance(x, dict) and (x.get("videoId")==vid or x.get("id")==vid or x.get("video_id")==vid)))
        ]
    return approved_obj

def _mark_seen(vid: str):
    seen = _read_json(SEEN_PATH, [])
    if vid not in seen:
        seen.append(vid)
        _write_json_atomic(SEEN_PATH, seen)
        

def _timestamp():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _move_to_failed(video_id: str, reason: str = "unknown error"):
    failed_path = "data/failed.json"
    approved = _read_json(APPROVED_PATH, [])
    failed   = _read_json(failed_path, [])

    # find a sample (to carry title/thumbnail if present)
    sample = next((e for e in approved if _vid_of(e) == _extract_vid(video_id)), None)
    entry = {"video_id": _extract_vid(video_id) or video_id, "title": "(unknown)"}
    if isinstance(sample, dict):
        for k in ("title","thumbnail"):
            if k in sample:
                entry[k] = sample[k]

    # remove ALL occurrences of this id
    approved, n_removed = _remove_id_all(approved, video_id)
    approved = _dedupe_by_id(approved)
    _write_json_atomic(APPROVED_PATH, approved)
    print(f"[CLEANUP] removed {n_removed} occurrence(s) of {entry['video_id']} from approved.json")

    # append to failed (allow duplicates there if you want, or dedupe too)
    failed.append({
        **entry,
        "failed_at": _timestamp(),
        "reason": reason,
    })
    _write_json_atomic(failed_path, failed)
      

# ---------- transcript fetch (force .fetch path) ----------
def fetch_transcript(video_id: str):
    """
    Returns: (segments, meta)
    segments = [{"start": float, "duration": float, "text": str}, ...]
    meta     = {"video_id": str}
    """
    from youtube_transcript_api import YouTubeTranscriptApi
    api = YouTubeTranscriptApi()               # your working path
    raw = api.fetch(video_id)

    def _get_attr_or_key(o, key, default=None):
        if isinstance(o, dict):
            return o.get(key, default)
        # object: try attribute, then __dict__
        v = getattr(o, key, default)
        if v is not None:
            return v
        try:
            return o.__dict__.get(key, default)
        except Exception:
            return default

    segments = []
    for s in raw:
        start = _get_attr_or_key(s, "start", 0.0)
        duration = _get_attr_or_key(s, "duration", 0.0)
        text = _get_attr_or_key(s, "text", "")
        # coerce types & clean
        try:
            start = float(start or 0.0)
        except Exception:
            start = 0.0
        try:
            duration = float(duration or 0.0)
        except Exception:
            duration = 0.0
        text = (text or "").strip()

        segments.append({"start": start, "duration": duration, "text": text})

    meta = {"video_id": video_id}
    return segments, meta


# ---------- output writers ----------
def save_per_video_json(video_id: str, segments: List[Dict[str, Any]], meta: Dict[str, Any]):
    _ensure_dir(TRANSCRIPTS_DIR)
    out_path = os.path.join(TRANSCRIPTS_DIR, f"{video_id}.json")
    payload = {"segments": segments, "meta": meta}
    _write_json_atomic(out_path, payload)

def _hms(seconds: float) -> str:
    s = int(seconds)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"

def update_search_index(video_id: str, segments: List[Dict[str, Any]], meta: Dict[str, Any], index_path=SEARCH_INDEX_PATH):
    """
    Upsert entries for this video in the global index:
    {
      "video_id": "...",
      "start": 12.34,
      "ts": "00:12",
      "text": "...",
      "title": meta.get("title",""),
      "channel": meta.get("channel","")
    }
    """
    index = _read_json(index_path, [])
    # remove old rows for this video_id
    index = [row for row in index if row.get("video_id") != video_id]

    for s in segments:
        start = float(s.get("start", 0.0))
        index.append({
            "video_id": video_id,
            "start": start,
            "ts": _hms(start),
            "text": s.get("text", ""),
            "title": meta.get("title", ""),
            "channel": meta.get("channel", ""),
        })
    _write_json_atomic(index_path, index)

# ---------- pacing/backoff ----------
REQS_PER_MIN = 10
BASE_DELAY = 60.0 / REQS_PER_MIN

def _pace(a=0.1, b=0.6):
    time.sleep(BASE_DELAY + random.uniform(a, b))


# ---------- main runner ----------
def run_from_approved():
    approved_obj = _read_json(APPROVED_PATH, [])
    queue = _extract_ids(approved_obj)
    if not queue:
        print("No approved IDs to process.")
        return {"processed": 0, "moved_to_failed": 0}

    processed = 0
    moved = 0

    print(f"Found {len(queue)} approved video(s).")
    for vid in list(queue):  # iterate snapshot
        try:
            print(f"[PROCESS] {vid}")
            # pace each networked operation a bit to avoid bursts
            _pace()

            segments, meta = fetch_transcript(vid)

            save_per_video_json(vid, segments, meta)
            update_search_index(vid, segments, meta)

            # drain from approved and mark seen
            _mark_seen(vid)
            approved_obj, _ = _remove_id_all(approved_obj, vid)
            approved_obj = _dedupe_by_id(approved_obj)
            _write_json_atomic(APPROVED_PATH, approved_obj)

            print(f"[DONE] {vid} — {len(segments)} segments")
            processed += 1

        except Exception as e:
            print(f"[FAIL] {vid}: {e}")
            _move_to_failed(vid, reason=str(e))
            approved_obj, _ = _remove_id_all(approved_obj, vid)
            approved_obj = _dedupe_by_id(approved_obj)
            _write_json_atomic(APPROVED_PATH, approved_obj)   # optional but safest
            moved += 1

    print(f"[SUMMARY] processed={processed}, moved_to_failed={moved}")
    return {"processed": processed, "moved_to_failed": moved}
                


if __name__ == "__main__":
    # --- Your existing transcript-fetching code ---
    # Example placeholder:
    # approved_videos = load_approved_json()
    # for video_id in approved_videos:
    #     process_video(video_id)

    # After processing, clear the approved list
    run_from_approved()
    
                    

