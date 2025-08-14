# tscripter.py
# Fetch transcripts for approved videos and build a global search index.
# - Reads:  data/approved.json
# - Writes: data/transcripts/{video_id}.json, data/search_index.json, data/seen_ids.json
# - Auto-removes processed IDs from approved.json (queue-drain, crash-safe)

import os, json, time, random
from typing import List, Dict, Tuple, Any

APPROVED_PATH = "data/approved.json"
TRANSCRIPTS_DIR = "data/transcripts"
SEARCH_INDEX_PATH = "data/search_index.json"
SEEN_PATH = "data/seen_ids.json"

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
REQS_PER_MIN = 5
BASE_DELAY = 60.0 / REQS_PER_MIN

def _pace(a=0.1, b=0.6):
    time.sleep(BASE_DELAY + random.uniform(a, b))

def _backoff(attempt: int):
    delay = min(2 ** (attempt - 1), 20)
    time.sleep(delay + random.uniform(0, 0.5))

# ---------- main runner ----------
def run_from_approved():
    approved_obj = _read_json(APPROVED_PATH, [])
    queue = _extract_ids(approved_obj)
    if not queue:
        print("No approved IDs to process.")
        return

    print(f"Found {len(queue)} approved video(s).")
    for vid in list(queue):  # iterate snapshot
        for attempt in range(1, 6):  # up to 5 tries per ID
            try:
                print(f"[PROCESS] {vid} (attempt {attempt})")
                segments, meta = fetch_transcript(vid)

                save_per_video_json(vid, segments, meta)
                update_search_index(vid, segments, meta)

                # Immediately drain queue entry (crash-safe) and mark seen
                _mark_seen(vid)
                approved_obj = _remove_id(approved_obj, vid)
                _write_json_atomic(APPROVED_PATH, approved_obj)

                print(f"[DONE] {vid} — {len(segments)} segments")
                _pace()
                break
            except Exception as e:
                print(f"[ERROR] {vid}: {e}")
                if attempt >= 5:
                    print(f"[GIVE UP] {vid} after {attempt} attempts")
                else:
                    _backoff(attempt)
                    



if __name__ == "__main__":
    # --- Your existing transcript-fetching code ---
    # Example placeholder:
    # approved_videos = load_approved_json()
    # for video_id in approved_videos:
    #     process_video(video_id)

    # After processing, clear the approved list
    run_from_approved()
    
                    

