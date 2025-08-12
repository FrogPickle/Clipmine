from youtube_transcript_api import YouTubeTranscriptApi
import html
import os
import csv

def read_video_ids(filename):
    video_ids = []
    if filename.endswith(".txt"):
        with open(filename, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    video_ids.append(line)
    elif filename.endswith(".csv"):
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                video_ids.append(row.get("video_id", "").strip())
    return [vid for vid in video_ids if vid]

def generate_transcript_section(transcript, video_id, index):
    section = f"""
    <h2>Transcript {index + 1} — Video ID: {video_id}</h2>
    <table>
        <thead>
            <tr><th>Linked Snippet</th><th>Sliding Window Concatenation</th></tr>
        </thead>
        <tbody>
    """

    lines = []
    for entry in transcript:
        start = int(entry.start)
        text = html.escape(entry.text)
        url = f"https://www.youtube.com/watch?v={video_id}&t={start}s"
        lines.append((text, url))

    for i, (text, url) in enumerate(lines):
        start_idx = max(0, i - 2)
        window_text = " ".join(lines[j][0] for j in range(start_idx, i + 1))
        window_text = html.escape(window_text)

        section += f"""
            <tr>
                <td class="left-col"><a href="{url}" target="_blank">{text}</a></td>
                <td class="right-col">{window_text}</td>
            </tr>
        """

    section += "</tbody></table><hr>"
    return section

def generate_full_html(all_sections):
    return f"""
    <html>
    <head>
        <meta charset='UTF-8'>
        <title>Combined Transcripts</title>
        <style>
            body {{ font-family: sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; margin-bottom: 2em; }}
            th, td {{ border: 1px solid #ccc; padding: 8px; vertical-align: top; }}
            th {{ background-color: #f0f0f0; }}
            td.left-col {{ width: 40%; }}
            td.right-col {{ width: 60%; font-family: monospace; white-space: pre-wrap; }}
            h2 {{ border-bottom: 1px solid #ccc; padding-bottom: 4px; margin-top: 2em; }}
        </style>
    </head>
    <body>
        <h1>Combined Transcript Viewer</h1>
        {''.join(all_sections)}
    </body>
    </html>
    """

def save_html(content, filename="multi_transcript.html"):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

def main():
    input_file = "video_ids.csv"  # or change to 'video_ids.csv'
    video_ids = read_video_ids(input_file)

    if not video_ids:
        print("No valid video IDs found.")
        return

    api = YouTubeTranscriptApi()
    all_sections = []

    for idx, video_id in enumerate(video_ids):
        try:
            print(f"Fetching transcript for video {idx + 1}: {video_id}")
            transcript = api.fetch(video_id)
            section_html = generate_transcript_section(transcript, video_id, idx)
            all_sections.append(section_html)
        except Exception as e:
            print(f"Error fetching transcript for {video_id}: {e}")

    if all_sections:
        full_html = generate_full_html(all_sections)
        save_html(full_html)
        print("All transcripts saved to multi_transcript.html")
    else:
        print("No transcripts could be fetched.")

if __name__ == "__main__":
    main()
