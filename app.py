from flask import Flask, request, redirect, url_for, jsonify, render_template
import requests
import os, pathlib, re, html
import json
from tscripter import run_from_approved
from pathlib import Path
import time, traceback
from datetime import datetime

app = Flask(__name__)

def load_api_key(path="apikey.txt"):
    p = pathlib.Path(path)
    if not p.exists():
        raise RuntimeError("apikey.txt not found. Create it and add your key.")
    return p.read_text(encoding="utf-8").strip()

# Put your actual YouTube Data API v3 key here:
YOUTUBE_API_KEY = load_api_key()
TRANSCRIPTS_DIR = Path("data/transcripts")
INDEX = {}  # { video_id: {"title": str|None, "segments": [{"start": float, "text": str}, ...]} }
INDEX_BUILT = False

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

MAX_RETRIES = 5
RETRY_BACKOFF_SEC = 5  # simple linear backoff; adjust as you like

_WORD = re.compile(r"\w+")

def _display_snippet(hay: str, m: re.Match, pre_words=12, post_words=12) -> str:
    """
    Build a consistent-length snippet around the match.
    Adds … if we trimmed content on either side.
    """
    a, b = m.start(), m.end()
    before, after = hay[:a], hay[b:]
    pre  = _WORD.findall(before)
    post = _WORD.findall(after)
    pre_slice  = pre[-pre_words:]
    post_slice = post[:post_words]
    left_ellipsis  = "…" if len(pre)  > len(pre_slice)  else ""
    right_ellipsis = "…" if len(post) > len(post_slice) else ""
    mid = hay[a:b]  # keep exact casing/punct
    return " ".join([left_ellipsis, *pre_slice, mid, *post_slice, right_ellipsis]).strip()

def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _anchor_key(hay: str, m: re.Match, pre_words=5, post_words=5) -> str:
    """
    Build a stable fingerprint: last N words before + match + first N words after.
    If there aren't enough words on one side, pad with boundary markers (^ or $)
    so edge cases dedupe consistently (no extra duplicates when the window slides).
    """
    a, b = m.start(), m.end()
    before_words = _WORD.findall(hay[:a])
    after_words  = _WORD.findall(hay[b:])

    pre = [w.lower() for w in before_words[-pre_words:]]
    post = [w.lower() for w in after_words[:post_words]]

    # pad to fixed width
    if len(pre) < pre_words:
        pre = (["^"] * (pre_words - len(pre))) + pre
    if len(post) < post_words:
        post = post + (["$"] * (post_words - len(post)))

    mid = _normalize_ws(hay[a:b]).lower()
    return " ".join(pre + [mid] + post)

def _timestamp():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _move_to_failed(video_id: str, reason: str = "unknown error"):
    approved = load_json("approved.json")
    failed   = load_json("failed.json")

    # find and remove from approved
    entry = next((v for v in approved if v.get("video_id") == video_id), None)
    if entry:
        approved = [v for v in approved if v.get("video_id") != video_id]
        save_json("approved.json", approved)
    else:
        entry = {"video_id": video_id, "title": "(unknown)"}

    # append to failed with details
    failed.append({
        **entry,
        "failed_at": _timestamp(),
        "reason": reason,
        "retries": MAX_RETRIES,
    })
    save_json("failed.json", failed)
    


def _load_one_json(path: Path):
    """Load a transcript json and normalize to: list[{'start': float, 'text': str}]"""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    segments = []

    # Case A: YouTubeTranscriptApi style: a list of {'text','start','duration'}
    if isinstance(data, list) and data and isinstance(data[0], dict) and "start" in data[0] and "text" in data[0]:
        for row in data:
            segments.append({"start": float(row.get("start", 0)), "text": row.get("text", "")})
        return segments

    # Case B: Faster-Whisper or your own cache: {'segments': [{'start','text',...}], 'video_id':..., 'title':...}
    if isinstance(data, dict) and "segments" in data and isinstance(data["segments"], list):
        for row in data["segments"]:
            segments.append({"start": float(row.get("start", 0)), "text": row.get("text", "")})
        return segments

    # Case C: simple txt (fallback): treat each line as a segment with incremental seconds
    if isinstance(data, str):
        lines = data.splitlines()
        for i, line in enumerate(lines):
            segments.append({"start": float(i * 5), "text": line})
        return segments

    # Unknown format — ignore gracefully
    return []
    
# --- FAST transcript index (concat text for quick searching) ---
from bisect import bisect_right  # safe to import even if already present

FAST = {}  # vid -> {"title", "segs", "text", "text_lc", "offsets"}

def build_fast_index(force: bool = False):
    """Concatenate each transcript into a single string + char offsets per segment."""
    global FAST
    if FAST and not force:
        return
    FAST = {}
    if not TRANSCRIPTS_DIR.exists():
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

    for p in TRANSCRIPTS_DIR.glob("*.json"):
        try:
            segs = _load_one_json(p)
            if not segs:
                continue

            parts, offsets, acc = [], [], 0
            for s in segs:
                offsets.append(acc)
                t = s.get("text", "")
                parts.append(t)
                acc += len(t) + 1  # +1 for the space we join with

            text = " ".join(parts)
            title = None
            try:
                with p.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
                if isinstance(raw, dict):
                    title = raw.get("title") or raw.get("video_title")
            except Exception:
                pass

            FAST[p.stem] = {
                "title": title,
                "segs": segs,
                "text": text,
                "text_lc": text.lower(),
                "offsets": offsets,
            }
        except Exception:
            continue
    

def build_index(force=False):
    """Scan data/transcripts and build in-memory index."""
    global INDEX, INDEX_BUILT
    if INDEX_BUILT and not force:
        return

    INDEX = {}
    if not TRANSCRIPTS_DIR.exists():
        TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

    for p in TRANSCRIPTS_DIR.glob("**/*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".json", ".txt"}:
            continue

        # Derive video_id from filename (e.g., abc123.json -> abc123)
        video_id = p.stem

        title = None
        segments = []
        try:
            if p.suffix.lower() == ".json":
                with p.open("r", encoding="utf-8") as f:
                    raw = json.load(f)
                # Try to pull a title if present
                if isinstance(raw, dict):
                    title = raw.get("title") or raw.get("video_title") or None
                # Normalize segments
                segments = _load_one_json(p)
            else:
                # TXT fallback
                with p.open("r", encoding="utf-8") as f:
                    lines = [ln.strip() for ln in f if ln.strip()]
                segments = [{"start": float(i * 5), "text": ln} for i, ln in enumerate(lines)]
        except Exception:
            # Skip malformed files silently
            continue

        if segments:
            INDEX[video_id] = {"title": title, "segments": segments}

    INDEX_BUILT = True

@app.before_request
def _ensure_index():
    build_index(force=False)
    
@app.before_request
def _ensure_fast_index():
    build_fast_index(force=False)    

def _yt_link(video_id: str, start: float) -> str:
    return f"https://www.youtube.com/watch?v={video_id}&t={int(start)}s"

@app.get("/api/search")
def api_search():
    q = (request.args.get("q") or "").strip()
    limit_per_video = int(request.args.get("per", 5))  # matches per video
    max_videos = int(request.args.get("max", 25))      # max videos in response

    if not q:
        return jsonify(results=[], count=0)

    # Case-insensitive substring (fast + simple). You can swap for regex if you want.
    q_norm = q.lower()

    results = []
    for vid, meta in INDEX.items():
        title = meta.get("title")
        matches = []
        for seg in meta["segments"]:
            text = seg.get("text", "")
            if q_norm in text.lower():
                matches.append({
                    "start": seg.get("start", 0.0),
                    "text": text,
                    "url": _yt_link(vid, seg.get("start", 0.0)),
                })
                if len(matches) >= limit_per_video:
                    break
        if matches:
            results.append({
                "video_id": vid,
                "title": title,
                "matches": matches,
            })
        if len(results) >= max_videos:
            break

    return jsonify(results=results, count=len(results))

# Optional: manual reindex endpoint (e.g., after new transcripts land)
@app.post("/admin/reindex")
def admin_reindex():
    build_index(force=True)
    return jsonify(ok=True, files=len(INDEX))

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_json(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

@app.route('/')
def home():
    return '''
        <h1>ClipMine Daily Search</h1>
        <form action="/search" method="get">
            <input type="text" name="query" placeholder="Search term" required>
            <button type="submit">Search</button>
        </form>
        <p>
          <a href="/pending">View Pending Videos</a> | 
          <a href="/approved">View Approved Videos</a> | 
          <a href="/transcripts">Go to Transcript Database</a>
        </p>
    '''

@app.route('/search')
def search():
    query = request.args.get('query')
    page_token = request.args.get('page_token', '')

    if not query:
        return redirect(url_for('home'))

    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": 20,
    }
    if page_token:
        params["pageToken"] = page_token

    response = requests.get("https://www.googleapis.com/youtube/v3/search", params=params)
    if response.status_code != 200:
        return f"<h2>Error fetching results from YouTube API: {response.status_code}</h2>"

    data = response.json()

    items = data.get("items", [])
    next_page_token = data.get("nextPageToken", None)

    seen_video_ids = set(load_json("seen_ids.json"))
    new_results = []

    for item in items:
        video_id = item["id"]["videoId"]
        if video_id in seen_video_ids:
            continue
        seen_video_ids.add(video_id)

        snippet = item["snippet"]
        title = snippet["title"]
        thumbnails = snippet.get("thumbnails", {})
        default_thumb = thumbnails.get("default", {}).get("url", "")

        new_results.append({
            "video_id": video_id,
            "title": title,
            "thumbnail": default_thumb
        })

    save_json("seen_ids.json", list(seen_video_ids))

    pending = load_json("pending.json")
    # Add new unique videos to pending
    existing_pending_ids = {v["video_id"] for v in pending}
    for video in new_results:
        if video["video_id"] not in existing_pending_ids:
            pending.append(video)
    save_json("pending.json", pending)

    # Build HTML for results (with inline JS for approve/reject)
    cards_html = ""
    for v in new_results:
        vid = v["video_id"]
        cards_html += f"""
            <div id="card-{vid}" style="margin-bottom:20px; border:1px solid #eee; padding:10px; border-radius:8px;">
                <img src="{v['thumbnail']}" alt="Thumbnail" />
                <p style="max-width:760px">{v['title']}</p>
                <a href="https://www.youtube.com/watch?v={vid}" target="_blank">Watch</a>
                &nbsp;|&nbsp;
                <button class="btn-approve" data-video="{vid}">Approve</button>
                &nbsp;|&nbsp;
                <button class="btn-reject" data-video="{vid}">Reject</button>
            </div>
        """

    next_link = ""
    if next_page_token:
        next_link = f'<a href="/search?query={query}&page_token={next_page_token}">Next Page &raquo;</a>'

    return f"""
        <h2>Results for "{query}"</h2>
        {cards_html}
        <p style="font-weight:bold; font-size:1.1em;">{next_link}</p>
        <p>
          <a href="/pending">View Pending Videos</a> | 
          <a href="/approved">View Approved Videos</a> | 
          <a href="/">Back to Search</a>
        </p>
        <script>
        async function postJSON(url) {{
          const resp = await fetch(url, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }} }});
          return resp.json();
        }}
        function removeCard(id) {{
          const el = document.getElementById('card-' + id);
          if (el) el.remove();
        }}
        document.querySelectorAll('.btn-approve').forEach(btn => {{
          btn.addEventListener('click', async (e) => {{
            const vid = e.currentTarget.getAttribute('data-video');
            try {{
              const res = await postJSON('/approve/' + vid);
              if (res && res.ok) removeCard(vid);
              else alert(res && res.message ? res.message : 'Approve failed');
            }} catch (err) {{
              alert('Approve error');
            }}
          }});
        }});
        document.querySelectorAll('.btn-reject').forEach(btn => {{
          btn.addEventListener('click', async (e) => {{
            const vid = e.currentTarget.getAttribute('data-video');
            try {{
              const res = await postJSON('/reject/' + vid);
              if (res && res.ok) removeCard(vid);
              else alert(res && res.message ? res.message : 'Reject failed');
            }} catch (err) {{
              alert('Reject error');
            }}
          }});
        }});
        </script>
    """


@app.route('/pending')
def view_pending():
    pending = load_json("pending.json")
    if not pending:
        return '''
            <h2>No pending videos.</h2>
            <p><a href="/">Back to Search</a></p>
        '''

    cards = []
    for v in pending:
        vid   = v.get("video_id","")
        thumb = v.get("thumbnail","")
        title = v.get("title","(no title)")
        cards.append(f"""
          <div id="card-{vid}" style="margin-bottom:20px; border:1px solid #eee; padding:10px; border-radius:8px;">
            <img src="{thumb}" alt="Thumbnail" />
            <p style="max-width:760px">{title} — <code>{vid}</code></p>
            <a href="https://www.youtube.com/watch?v={vid}" target="_blank">Watch</a>
            &nbsp;|&nbsp;
            <button class="btn-approve" data-id="{vid}">Approve</button>
            &nbsp;|&nbsp;
            <button class="btn-reject" data-id="{vid}">Reject</button>
            <span class="msg" id="msg-{vid}" style="margin-left:8px; color:#555;"></span>
          </div>
        """)

    return f"""
      <h1>Pending Videos</h1>
      {''.join(cards)}
      <p><a href="/">Back to Search</a></p>

      <script>
        async function post(url) {{
          const r = await fetch(url, {{ method:'POST', headers:{{'Content-Type':'application/json'}} }});
          let ok=false;
          try {{ const j = await r.json(); ok = r.ok && j && j.ok; }} catch (_){{
            ok = r.ok;
          }}
          return ok;
        }}
        function rm(id) {{
          const el = document.getElementById('card-'+id);
          if (el) el.remove();
        }}
        function msg(id, text) {{
          const el = document.getElementById('msg-'+id);
          if (el) el.textContent = text || '';
        }}

        document.querySelectorAll('.btn-approve').forEach(b => {{
          b.onclick = async () => {{
            const id = b.dataset.id;
            msg(id,'Saving…');
            if (await post('/approve/'+id)) rm(id); else msg(id,'Failed to approve');
          }};
        }});
        document.querySelectorAll('.btn-reject').forEach(b => {{
          b.onclick = async () => {{
            const id = b.dataset.id;
            msg(id,'Saving…');
            if (await post('/reject/'+id)) rm(id); else msg(id,'Failed to reject');
          }};
        }});
      </script>
    """



@app.route('/approved')
def view_approved():
    approved = load_json("approved.json")
    if not approved:
        return '''
            <h2>No approved videos.</h2>
            <p><a href="/">Back to Search</a></p>
        '''
    html = "<h1>Approved Videos</h1>"
    html += '''
        <p>
          <button id="btn-transcribe">Transcribe All</button>
          <span id="tx-status" style="margin-left:8px; color:#555;"></span>
        </p>
        <script>
          document.addEventListener('DOMContentLoaded', () => {
            const btn = document.getElementById('btn-transcribe');
            const out = document.getElementById('tx-status');
            btn.addEventListener('click', async () => {
              out.textContent = 'Starting…';
              try {
                const resp = await fetch('/transcribe-all', { method: 'POST' });
                const data = await resp.json();
                out.textContent = data && data.message ? data.message : 'Done';
              } catch (e) {
                out.textContent = 'Error starting transcription';
              }
            });
          });
        </script>
    '''
    for video in approved:
        html += f'''
            <div style="margin-bottom:20px;">
                <img src="{video.get("thumbnail","")}" alt="Thumbnail" />
                <p>{video.get("title","(no title)")} — <code>{video.get("video_id","")}</code></p>
                <a href="https://www.youtube.com/watch?v={video.get("video_id","")}" target="_blank">Watch</a>
            </div>
        '''
    html += '<p><a href="/">Back to Search</a></p>'
    return html

"""
@app.route('/approve/<video_id>')
def approve(video_id):
    query = request.args.get('query', '')
    page_token = request.args.get('page_token', '')

    pending = load_json("pending.json")
    approved = load_json("approved.json")

    video = next((v for v in pending if v["video_id"] == video_id), None)
    if video:
        approved.append(video)
        save_json("approved.json", approved)
        pending = [v for v in pending if v["video_id"] != video_id]
        save_json("pending.json", pending)

    if query:
        return redirect(url_for("search", query=query, page_token=page_token))
    else:
        return redirect(url_for("view_pending"))

@app.route('/reject/<video_id>')
def reject(video_id):
    query = request.args.get('query', '')
    page_token = request.args.get('page_token', '')

    pending = load_json("pending.json")
    rejected = load_json("rejected.json")

    video = next((v for v in pending if v["video_id"] == video_id), None)
    if video:
        rejected.append(video)
        save_json("rejected.json", rejected)
        pending = [v for v in pending if v["video_id"] != video_id]
        save_json("pending.json", pending)

    if query:
        return redirect(url_for("search", query=query, page_token=page_token))
    else:
        return redirect(url_for("view_pending"))
        
        """
        
@app.post('/approve/<video_id>')
def approve_api(video_id):
    pending = load_json("pending.json")
    approved = load_json("approved.json")

    video = next((v for v in pending if v["video_id"] == video_id), None)
    if not video:
        return jsonify(ok=False, message="Video not found in pending"), 404

    approved.append(video)
    save_json("approved.json", approved)
    pending = [v for v in pending if v["video_id"] != video_id]
    save_json("pending.json", pending)

    return jsonify(ok=True)

@app.post('/reject/<video_id>')
def reject_api(video_id):
    pending = load_json("pending.json")
    rejected = load_json("rejected.json")

    video = next((v for v in pending if v["video_id"] == video_id), None)
    if not video:
        return jsonify(ok=False, message="Video not found in pending"), 404

    rejected.append(video)
    save_json("rejected.json", rejected)
    pending = [v for v in pending if v["video_id"] != video_id]
    save_json("pending.json", pending)

    return jsonify(ok=True)
        

@app.route('/transcripts')
def transcripts():
    # Placeholder page for transcripts — you can implement transcript display here later
    return redirect('/tsearch')
    
@app.post("/transcribe-all")
def transcribe_all():
    try:
        stats = run_from_approved()  # make it return counts if you want
        msg = "Transcription complete!"
        if isinstance(stats, dict):
            msg += f" processed={stats.get('processed',0)}, moved_to_failed={stats.get('moved_to_failed',0)}"
        return jsonify(success=True, message=msg)
    except Exception as e:
        app.logger.exception("Transcribe-all failed")
        return jsonify(success=False, message=f"❌ Error: {e}"), 500
    
        
def _load_segments(p: Path):
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):   # YouTubeTranscriptApi style
        return [{"start": float(x.get("start",0)), "text": x.get("text","")} for x in data]
    if isinstance(data, dict) and "segments" in data:
        return [{"start": float(x.get("start",0)), "text": x.get("text","")} for x in data["segments"]]
    return []

def _yt_link(vid, start): return f"https://www.youtube.com/watch?v={vid}&t={int(start)}s"

def _stitch_window(segs, i, before=2, after=2, max_chars=600, max_gap=4.0):
    left = i
    chars = len(segs[i]["text"])
    while left > 0 and (segs[left]["start"] - segs[left-1]["start"] <= max_gap):
        if chars + 1 + len(segs[left-1]["text"]) > max_chars: break
        left -= 1; chars += 1 + len(segs[left]["text"])
    right = i
    while right + 1 < len(segs) and (segs[right+1]["start"] - segs[right]["start"] <= max_gap):
        if chars + 1 + len(segs[right+1]["text"]) > max_chars: break
        right += 1; chars += 1 + len(segs[right]["text"])
    text = " ".join(s["text"] for s in segs[left:right+1])
    start = segs[left]["start"]
    return text, start, left, right


def _normalize_ws(s: str) -> str:
    # collapse any whitespace (including \n) to single spaces
    return re.sub(r"\s+", " ", s).strip()

def _build_regex(raw: str):
    parts = [p.strip() for p in raw.splitlines() if p.strip()] or [raw.strip()]
    def pat(t):
        quoted = (t.startswith('"') and t.endswith('"')) or (t.startswith("'") and t.endswith("'"))
        core = re.escape(t[1:-1] if quoted else t)
        # whole-word by default; quoted = exact (but we normalized WS, so spaces are fine)
        return core if quoted else rf"(?<!\w){core}(?!\w)"
    return re.compile("|".join(f"(?:{pat(t)})" for t in parts), re.IGNORECASE)
    
def _match_to_segment_start(meta, pos_char: int):
    offs = meta["offsets"]
    idx = bisect_right(offs, pos_char) - 1
    if idx < 0:
        idx = 0
    segs = meta["segs"]
    if idx >= len(segs):
        idx = len(segs) - 1
    return float(segs[idx].get("start", 0.0)), idx

# Reuse your existing snippet builder for consistent display
def _display_snippet_from_text(text: str, m, pre_words=12, post_words=12):
    return _display_snippet(text, m, pre_words=pre_words, post_words=post_words)    


@app.get("/api/tsearch")
def api_tsearch():
    raw = (request.args.get("q") or "").strip()
    per = int(request.args.get("per", 5))
    if not raw:
        return jsonify(groups=[], count=0)

    rx = _build_regex(raw)  # your existing builder (word-boundary + quoted)

    groups = {}  # anchor -> {"text": display_snip, "hits":[{video_id,start,url}], "_seen_vids": set()}

    for vid, meta in FAST.items():
        text = meta["text"]
        hay  = meta["text_lc"]  # lowercased for matching
        added_here = set()
        for m in rx.finditer(hay):
            # Build a stable anchor on the ORIGINAL text (same span)
            # Note: rebuild a match object over original case by slicing indices
            class _Span:  # tiny shim to pass .start()/.end() to anchor/snippet helpers
                def __init__(self, a, b): self._a, self._b = a, b
                def start(self): return self._a
                def end(self): return self._b

            mm = _Span(m.start(), m.end())

            # Anchor (±5 words around the hit)
            key = _anchor_key(text, mm, pre_words=5, post_words=5)
            g = groups.get(key)
            disp = _display_snippet_from_text(text, mm, pre_words=12, post_words=12)

            if g is None:
                g = {"text": disp, "hits": [], "_seen_vids": set()}
                groups[key] = g
            else:
                if len(disp) > len(g["text"]):
                    g["text"] = disp

            if vid not in g["_seen_vids"]:
                # Map char pos to segment start
                start_sec, _ = _match_to_segment_start(meta, m.start())
                g["_seen_vids"].add(vid)
                g["hits"].append({
                    "video_id": vid,
                    "start": int(start_sec),
                    "url": _yt_link(vid, int(start_sec)),
                })

            # Avoid piling on tons of matches from same video for same anchor
            if len(g["hits"]) >= per:
                added_here.add(key)

        # (Optional) Early stop if this video produced enough anchors already

    out = []
    for _, g in groups.items():
        hits = g["hits"][:per] if per > 0 else g["hits"]
        out.append({"quote": g["text"], "occurrences": hits})

    # Order: show groups with most sources first
    out.sort(key=lambda x: (-len(x["occurrences"]), x["occurrences"][0]["start"] if x["occurrences"] else 0))

    return jsonify(groups=out, count=len(out))

@app.get("/tsearch")
def tsearch_page():
    return r'''
<!doctype html><meta charset="utf-8">
<title>Transcript Search</title>
<div style="max-width:960px;margin:24px auto;font-family:system-ui,Segoe UI,Roboto,sans-serif">
  <h1>Transcript Search</h1>
  <input id="q" placeholder="Type a phrase and press Enter…" style="padding:10px;width:70%">
  <button id="go">Search</button>
  <div id="list" style="margin-top:12px;color:#333;"></div>
</div>
<script>
(function(){
  function esc(s){ return s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'); }
  function buildRegex(q){
    const parts = q.split(/\r?\n/).map(s=>s.trim()).filter(Boolean);
    const pats = (parts.length?parts:[q]).map(t=>{
      const quoted = (t.startsWith('"')&&t.endsWith('"'))||(t.startsWith("'")&&t.endsWith("'"));
      const core = esc(quoted ? t.slice(1,-1) : t);
      return quoted ? `(${core})` : `(?<!\\w)(${core})(?!\\w)`;
    });
    return new RegExp(pats.join("|"), "gi");
  }
  function highlight(text, q){
    try { return text.replace(buildRegex(q), m=>`<mark>${m}</mark>`); }
    catch { return text; }
  }
  function groupCard(g, q){
    const links = (g.occurrences||[]).map(h =>
      `<a href="${h.url}" target="_blank" rel="noopener">${h.video_id}@${h.start}s</a>`
    ).join(' &middot; ');
    return `
      <div style="border:1px solid #eee;border-radius:10px;padding:12px;margin:12px 0">
        <div style="background:#fafafa;border-radius:8px;padding:10px;white-space:pre-wrap;">
          ${highlight(g.quote||'', q)}
        </div>
        <div style="margin-top:8px;font-size:14px;color:#444">
          ${links || '(no links)'}
        </div>
      </div>`;
  }
  async function run(){
    const q = document.getElementById('q').value.trim();
    const list = document.getElementById('list');
    if(!q){ list.innerHTML = '<p>Type a search.</p>'; return; }
    list.textContent = 'Searching…';
    try{
      const r = await fetch('/api/tsearch?q='+encodeURIComponent(q)+'&per=10');
      const d = await r.json();
      const groups = d.groups || [];
      list.innerHTML = groups.length ? groups.map(g=>groupCard(g,q)).join('') : '<p>No matches.</p>';
    }catch(e){
      console.error(e);
      list.textContent = 'Error.';
    }
  }
  window.addEventListener('DOMContentLoaded', ()=>{
    const q = document.getElementById('q');
    const go = document.getElementById('go');
    go.onclick = run;
    q.addEventListener('keydown', e=>{ if(e.key==='Enter'){ e.preventDefault(); run(); }});
  });
})();
</script>
'''

      

if __name__ == '__main__':
    app.run(debug=True)
