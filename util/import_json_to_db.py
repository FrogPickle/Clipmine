# import_json_to_db.py
import os, json, argparse, sqlite3, datetime as dt

DB_PATH = os.path.join("data", "clipmine.db")

def open_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def get_or_create_project(con, slug, name=None):
    cur = con.execute("SELECT id FROM projects WHERE slug=?", (slug,))
    row = cur.fetchone()
    if row:
        return row["id"]
    name = name or slug.title()
    cur = con.execute("INSERT INTO projects (slug, name) VALUES (?,?)", (slug, name))
    return cur.lastrowid

# ---- MAPPERS: tweak these to match your JSON -----------------

def map_transcript_meta(item):
    """Return a dict with keys matching the 'transcripts' table columns you want to set."""
    return {
        "source_id":   item.get("source_id") or item.get("video_id") or item.get("id"),
        "title":       item.get("title") or "",
        "channel_id":  item.get("channel_id"),
        "published_at":item.get("published_at"),      # ISO8601 if you have it
        "duration_sec":item.get("duration_sec") or item.get("duration"),
        "path_json":   item.get("transcript_path"),
        "path_txt":    item.get("transcript_txt_path"),  # optional
        "status":      "visible",   # default; change if you track approvals elsewhere
        "visible_rank":None,
        "signature":   item.get("signature")           # optional
    }

def load_segments_from_path(path_json):
    """Read your per-video transcript JSON and return a list of segments."""
    if not path_json or not os.path.exists(path_json):
        return []
    with open(path_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    segs = []
    default_role = data.get("speaker_role_default", "subject")
    for s in data.get("segments", []):
        start_ms = s.get("start_ms") or int(float(s.get("start", 0)) * 1000)
        end_ms   = s.get("end_ms")   or int(float(s.get("end",   0)) * 1000)
        text     = s.get("text") or ""
        role     = s.get("speaker_role") or default_role
        name     = s.get("speaker_name")  # optional
        if text.strip() and end_ms > start_ms:
            segs.append({
                "start_ms": start_ms,
                "end_ms":   end_ms,
                "speaker_role": role if role in
                   ("subject","journalist","host","audience","network_intro","other") else "other",
                "speaker_name": name,
                "text": text.strip()
            })
    return segs

# --------------------------------------------------------------

def upsert_transcript(con, project_id, meta, segments):
    # Avoid duplicates by (project_id, source_id)
    cur = con.execute(
        "SELECT id FROM transcripts WHERE project_id=? AND source_id=?",
        (project_id, meta["source_id"])
    )
    row = cur.fetchone()
    if row:
        t_id = row["id"]
        # Optionally update title/duration/paths if changed
        con.execute("""
            UPDATE transcripts SET title=COALESCE(?,title),
                                   channel_id=COALESCE(?,channel_id),
                                   published_at=COALESCE(?,published_at),
                                   duration_sec=COALESCE(?,duration_sec),
                                   path_json=COALESCE(?,path_json),
                                   path_txt=COALESCE(?,path_txt)
            WHERE id=?
        """, (meta.get("title"), meta.get("channel_id"), meta.get("published_at"),
              meta.get("duration_sec"), meta.get("path_json"), meta.get("path_txt"), t_id))
    else:
        cur = con.execute("""
            INSERT INTO transcripts
                (project_id, cluster_id, source_id, channel_id, title, published_at,
                 duration_sec, signature, status, visible_rank, path_json, path_txt)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (project_id, None, meta["source_id"], meta.get("channel_id"), meta.get("title"),
              meta.get("published_at"), meta.get("duration_sec"), meta.get("signature"),
              meta.get("status","visible"), meta.get("visible_rank"),
              meta.get("path_json"), meta.get("path_txt")))
        t_id = cur.lastrowid

    # Insert segments (dedupe lightly by (start_ms,end_ms,text) for this transcript)
    if segments:
        existing = set()
        for row in con.execute(
            "SELECT start_ms,end_ms,text FROM segments WHERE transcript_id=?", (t_id,)
        ):
            existing.add((row["start_ms"], row["end_ms"], row["text"]))
        to_insert = [s for s in segments if (s["start_ms"], s["end_ms"], s["text"]) not in existing]
        con.executemany("""
            INSERT INTO segments (transcript_id, start_ms, end_ms, speaker_role, speaker_name, text)
            VALUES (?,?,?,?,?,?)
        """, [(t_id, s["start_ms"], s["end_ms"], s["speaker_role"], s.get("speaker_name"), s["text"])
              for s in to_insert])
    return t_id

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-slug", default="trump", help="Project slug to import into")
    ap.add_argument("--project-name", default=None)
    ap.add_argument("--approved-json", default=os.path.join("data","approved.json"),
                    help="Path to your existing approved JSON list")
    ap.add_argument("--commit", action="store_true", help="Commit changes (default is dry-run)")
    args = ap.parse_args()

    if not os.path.exists(DB_PATH):
        raise SystemExit(f"DB not found at {DB_PATH}. Run init_db.py first.")

    with open(args.approved_json, "r", encoding="utf-8") as f:
        items = json.load(f)

    con = open_db()
    try:
        con.execute("BEGIN;")
        project_id = get_or_create_project(con, args.project_slug, args.project_name)
        print(f"Project id={project_id} slug={args.project_slug}")

        inserted = updated = seg_count = skipped = 0

        for i, item in enumerate(items, start=1):
            meta = map_transcript_meta(item)
            if not meta["source_id"]:
                skipped += 1
                print(f"- SKIP {i}: missing source_id")
                continue

            segs = load_segments_from_path(meta.get("path_json"))
            t_id = upsert_transcript(con, project_id, meta, segs)
            if segs:
                seg_count += len(segs)

            # Count insert vs update roughly by checking visibility of returned id;
            # (For simplicity, treat as inserted if path_json existed & segments added)
            # This is just logging—DB already avoids dupes via the SELECT above.
            print(f"✓ Upserted transcript {meta['source_id']} with {len(segs)} segments (id={t_id})")

        print(f"\nSummary: upserted={len(items)-skipped}, segments_added≈{seg_count}, skipped={skipped}")

        if args.commit:
            con.execute("COMMIT;")
            print("✔ Committed.")
        else:
            con.execute("ROLLBACK;")
            print("ℹ Dry-run complete (no changes saved). Add --commit to write.")
    finally:
        con.close()

if __name__ == "__main__":
    main()
