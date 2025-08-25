# enrich_db_from_youtube.py
import os, argparse, sqlite3, requests, time, re

API = "https://www.googleapis.com/youtube/v3"
DUR_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")

def iso_to_seconds(s):
    if not s: return None
    m = DUR_RE.match(s)
    if not m: return None
    h = int(m.group(1) or 0); m_ = int(m.group(2) or 0); s_ = int(m.group(3) or 0)
    return h*3600 + m_*60 + s_

def chunked(xs, n=50):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def fetch_videos(ids, key):
    r = requests.get(f"{API}/videos", params={
        "key": key,
        "part": "snippet,contentDetails",
        "id": ",".join(ids)
    }, timeout=30)
    r.raise_for_status()
    return r.json().get("items", [])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/clipmine.db")
    ap.add_argument("--project-slug", required=True)
    ap.add_argument("--api-key", help="YouTube Data API key (or set YT_API_KEY env var)")
    ap.add_argument("--commit", action="store_true", help="Apply updates (default: dry run)")
    args = ap.parse_args()

    key = args.api_key or os.environ.get("YT_API_KEY")
    if not key:
        raise SystemExit("Missing API key. Pass --api-key or set YT_API_KEY.")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row

    proj = con.execute("SELECT id FROM projects WHERE slug=?", (args.project_slug,)).fetchone()
    if not proj:
        raise SystemExit(f"No project with slug '{args.project_slug}'")
    pid = proj["id"]

    # Find rows with missing metadata
    rows = con.execute("""
        SELECT id, source_id, title, channel_id, published_at, duration_sec
        FROM transcripts
        WHERE project_id=? AND source_id IS NOT NULL
          AND (channel_id IS NULL OR published_at IS NULL OR duration_sec IS NULL OR title IS NULL)
    """, (pid,)).fetchall()

    if not rows:
        print("Nothing to enrich.")
        return

    id_map = {r["source_id"]: r for r in rows}
    ids = list(id_map.keys())
    print(f"Found {len(ids)} transcript(s) needing enrichment.")

    updates = []
    for batch in chunked(ids, 50):
        items = fetch_videos(batch, key)
        found = set()
        for v in items:
            vid = v["id"]; found.add(vid)
            sn = v.get("snippet", {}) or {}
            cd = v.get("contentDetails", {}) or {}
            updates.append({
                "source_id": vid,
                "title": sn.get("title"),
                "channel_id": sn.get("channelId"),
                "published_at": sn.get("publishedAt"),
                "duration_sec": iso_to_seconds(cd.get("duration"))
            })
        for vid in batch:
            if vid not in found:
                print(f"  ! missing/private/deleted: {vid}")
        time.sleep(0.05)  # gentle

    # Show diffs (dry run)
    changed = 0
    for u in updates:
        old = id_map.get(u["source_id"])
        if not old: continue
        diffs = []
        for k in ("title","channel_id","published_at","duration_sec"):
            if u.get(k) and (old[k] is None or old[k] != u[k]):
                diffs.append(f"{k}: {old[k]} -> {u[k]}")
        if diffs:
            changed += 1
            print(f"- {u['source_id']}: " + "; ".join(diffs))

    if not args.commit:
        print(f"\nDry-run: {changed} transcript(s) would be updated. Re-run with --commit to apply.")
        return

    # Apply updates
    con.execute("BEGIN;")
    for u in updates:
        con.execute("""
            UPDATE transcripts
            SET title=COALESCE(?, title),
                channel_id=COALESCE(?, channel_id),
                published_at=COALESCE(?, published_at),
                duration_sec=COALESCE(?, duration_sec)
            WHERE project_id=? AND source_id=?
        """, (u["title"], u["channel_id"], u["published_at"], u["duration_sec"], pid, u["source_id"]))
    con.execute("COMMIT;")
    print(f"Committed {changed} transcript(s) with new metadata.")
    con.close()

if __name__ == "__main__":
    main()
