import os, json, glob, sqlite3, argparse

ROLES = {"subject","journalist","host","audience","network_intro","other"}

def connect(db_path):
    con = sqlite3.connect(db_path, isolation_level=None)  # explicit transactions only
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def ensure_project(con, slug):
    row = con.execute("SELECT id FROM projects WHERE slug=?", (slug,)).fetchone()
    if row: return row["id"]
    return con.execute("INSERT INTO projects(slug,name) VALUES(?,?)", (slug, slug.title())).lastrowid

def load_segments_obj(obj):
    segs = []
    for s in obj.get("segments", []):
        # your format: start (sec), duration (sec), text
        start_ms = int(round(float(s.get("start", 0.0)) * 1000))
        dur_ms   = int(round(float(s.get("duration", 0.0)) * 1000))
        end_ms   = start_ms + max(0, dur_ms)
        text     = (s.get("text") or "").strip()
        if text and end_ms > start_ms:
            segs.append((start_ms, end_ms, "subject", None, text))  # default role = subject
    return segs

def upsert_transcript(con, project_id, source_id, title, segs):
    row = con.execute(
        "SELECT id FROM transcripts WHERE project_id=? AND source_id=?",
        (project_id, source_id)
    ).fetchone()
    if row:
        t_id = row["id"]
        # update title if we got a better one (you probably don't yet)
        if title:
            con.execute("UPDATE transcripts SET title=COALESCE(?, title) WHERE id=?", (title, t_id))
    else:
        t_id = con.execute("""
            INSERT INTO transcripts (project_id, source_id, title, status)
            VALUES (?,?,?, 'visible')
        """, (project_id, source_id, title or None)).lastrowid

    if segs:
        # avoid duping same (start,end,text)
        existing = {(r["start_ms"], r["end_ms"], r["text"])
                    for r in con.execute("SELECT start_ms,end_ms,text FROM segments WHERE transcript_id=?", (t_id,))}
        to_add = [s for s in segs if (s[0], s[1], s[4]) not in existing]
        if to_add:
            con.executemany("""
              INSERT INTO segments (transcript_id, start_ms, end_ms, speaker_role, speaker_name, text)
              VALUES (?,?,?,?,?,?)
            """, [(t_id, *s) for s in to_add])
    return t_id

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/clipmine.db")
    ap.add_argument("--project-slug", required=True)
    ap.add_argument("--dir", default="data/transcripts", help="Folder with per-video JSONs")
    ap.add_argument("--commit", action="store_true", help="Write changes (default: dry run)")
    args = ap.parse_args()

    con = connect(args.db)
    pid = ensure_project(con, args.project_slug)

    files = sorted(glob.glob(os.path.join(args.dir, "**", "*.json"), recursive=True))
    # skip list files if they live alongside
    skip_names = {"rejected.json","approved.json","pending.json"}
    files = [f for f in files if os.path.basename(f).lower() not in skip_names]

    added_t = updated_t = added_segs = skipped = 0
    if args.commit:
        con.execute("BEGIN;")  # explicit tx only when committing

    for path in files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            skipped += 1
            continue

        # derive source_id (video_id) from filename
        source_id = os.path.splitext(os.path.basename(path))[0]
        title = obj.get("title")  # usually missing in your case

        # must have segments array
        if not isinstance(obj.get("segments"), list):
            skipped += 1
            continue

        segs = load_segments_obj(obj)

        if args.commit:
            # count segs before/after for stats
            before = con.execute("""
                SELECT COUNT(1) c
                FROM segments s JOIN transcripts t ON t.id=s.transcript_id
                WHERE t.project_id=? AND t.source_id=?""", (pid, source_id)).fetchone()["c"]

            existed = con.execute(
                "SELECT 1 FROM transcripts WHERE project_id=? AND source_id=?",
                (pid, source_id)
            ).fetchone() is not None

            _tid = upsert_transcript(con, pid, source_id, title, segs)

            after = con.execute("""
                SELECT COUNT(1) c
                FROM segments s JOIN transcripts t ON t.id=s.transcript_id
                WHERE t.project_id=? AND t.source_id=?""", (pid, source_id)).fetchone()["c"]

            added_segs += max(0, after - before)
            if existed: updated_t += 1
            else:       added_t += 1
        else:
            # dry run stats only
            existed = con.execute(
                "SELECT 1 FROM transcripts WHERE project_id=? AND source_id=?",
                (pid, source_id)
            ).fetchone() is not None
            if existed: updated_t += 1
            else:       added_t += 1
            added_segs += len(segs)

    if args.commit:
        con.execute("COMMIT;")
        mode = "Committed"
    else:
        mode = "Dry-run"

    con.close()
    print(f"{mode}: transcripts added={added_t}, updated={updated_t}, segments added≈{added_segs}, skipped={skipped}")
    print(f"Scanned {len(files)} file(s) under {args.dir}")

if __name__ == "__main__":
    main()
