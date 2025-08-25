import json
import os
import subprocess
from pathlib import Path

approved_file = "data/approved.json"
transcript_dir = Path("data/transcripts")
transcript_dir.mkdir(parents=True, exist_ok=True)

with open(approved_file, "r", encoding="utf-8") as f:
    approved_videos = json.load(f)

for video in approved_videos:
    video_id = video["video_id"]
    url = f"https://www.youtube.com/watch?v={video_id}"
    out_base = transcript_dir / video_id

    json_path = out_base.with_suffix(".json")
    if json_path.exists():
        print(f"✅ Transcript already exists for {video_id}, skipping.")
        continue

    print(f"\n🔽 Downloading and transcribing {video_id}...")

    try:
        subprocess.run([
            "yt-dlp",
            "--quiet",
            "--extract-audio",
            "--audio-format", "mp3",
            "-o", "audio.%(ext)s",
            url
        ], check=True)

        subprocess.run([
            "whisper",
            "audio.mp3",
            "--model", "small",
            "--language", "en",            # Force English
            "--output_format", "json",
            "--output_dir", str(transcript_dir),
            "--fp16", "False"
        ], check=True)

        os.rename(transcript_dir / "audio.json", json_path)
        os.remove("audio.mp3")

    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to process {video_id}: {e}")
