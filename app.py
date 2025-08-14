from flask import Flask, request, redirect, url_for, jsonify, render_template
from faster_whisper import WhisperModel
from pytube import YouTube
import requests
import os, pathlib
import json
from tscripter import run_from_approved

app = Flask(__name__)

def load_api_key(path="apikey.txt"):
    p = pathlib.Path(path)
    if not p.exists():
        raise RuntimeError("apikey.txt not found. Create it and add your key.")
    return p.read_text(encoding="utf-8").strip()

# Put your actual YouTube Data API v3 key here:
YOUTUBE_API_KEY = load_api_key()

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

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

    # Build HTML for results
    results_html = ""
    for video in new_results:
        results_html += f'''
            <div style="margin-bottom:20px;">
                <img src="{video["thumbnail"]}" alt="Thumbnail" />
                <p>{video["title"]}</p>
                <a href="https://www.youtube.com/watch?v={video["video_id"]}" target="_blank">Watch</a> |
                <a href="/approve/{video["video_id"]}?query={query}&page_token={page_token}">Approve</a> |
                <a href="/reject/{video["video_id"]}?query={query}&page_token={page_token}">Reject</a>
            </div>
        '''

    next_link = ""
    if next_page_token:
        next_link = f'<a href="/search?query={query}&page_token={next_page_token}">Next Page &raquo;</a>'

    results_html += f'''
        <p style="font-weight:bold; font-size:1.1em;">{next_link}</p>
        <p>
          <a href="/pending">View Pending Videos</a> | 
          <a href="/approved">View Approved Videos</a> | 
          <a href="/">Back to Search</a>
        </p>
    '''

    return f'''
        <h2>Results for "{query}"</h2>
        {results_html}
    '''

@app.route('/pending')
def view_pending():
    pending = load_json("pending.json")
    if not pending:
        return '''
            <h2>No pending videos.</h2>
            <p><a href="/">Back to Search</a></p>
        '''
    html = "<h1>Pending Videos</h1>"
    for video in pending:
        html += f'''
            <div style="margin-bottom:20px;">
                <img src="{video["thumbnail"]}" alt="Thumbnail" />
                <p>{video["title"]}</p>
                <a href="https://www.youtube.com/watch?v={video["video_id"]}" target="_blank">Watch</a> |
                <a href="/approve/{video["video_id"]}">Approve</a> |
                <a href="/reject/{video["video_id"]}">Reject</a>
            </div>
        '''
    html += '<p><a href="/">Back to Search</a></p>'
    return html

@app.route('/approved')
def view_approved():
    approved = load_json("approved.json")
    return render_template("approved.html", results=approved)

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

@app.route('/transcripts')
def transcripts():
    # Placeholder page for transcripts — you can implement transcript display here later
    return '''
        <h1>Transcript Database (Coming Soon)</h1>
        <p><a href="/">Back to Search</a></p>
    '''
    
@app.route("/transcribe-all", methods=["POST"])
@app.route("/transcribe-all", methods=["POST"])
def transcribe_all():
    try:
        run_from_approved()  # runs your transcription script
        return jsonify(success=True, message="Transcription complete!")
    except Exception as e:
        return jsonify(success=False, message=f"❌ Error: {e}"), 500   

if __name__ == '__main__':
    app.run(debug=True)
