# verify_db.py
import sqlite3

DB_PATH = "data/clipmine.db"

def open_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def print_counts(con):
    one = lambda q: con.execute(q).fetchone()[0]
    print("== PRAGMA checks ==")
    fk = con.execute("PRAGMA foreign_keys").fetchone()[0]
    integ = con.execute("PRAGMA integrity_check").fetchone()[0]
    print("foreign_keys:", fk)
    print("integrity_check:", integ)

    print("\n== Row counts ==")
    print("projects    :", one("SELECT COUNT(*) FROM projects"))
    print("transcripts :", one("SELECT COUNT(*) FROM transcripts"))
    print("segments    :", one("SELECT COUNT(*) FROM segments"))
    try:
        print("fts rows    :", one("SELECT COUNT(*) FROM fts_segments"))
    except sqlite3.OperationalError:
        print("fts rows    : (fts_segments table missing)")

def sample_segments(con, n=5):
    print("\n== Sample rows from segments ==")
    rows = con.execute(
        "SELECT id, transcript_id, speaker_role, start_ms, end_ms, text "
        "FROM segments ORDER BY id LIMIT ?", (n,)
    ).fetchall()
    for r in rows:
        print(dict(r))

def inspect_fts(con):
    print("\n== Inspect FTS table schema ==")
    try:
        rows = con.execute("PRAGMA table_info(fts_segments);").fetchall()
    except sqlite3.OperationalError:
        print("(fts_segments not found)")
        return None
    for r in rows:
        print(dict(r))

    print("\n== Sample raw rows from fts_segments ==")
    rows2 = con.execute("SELECT rowid, transcript_id, speaker_role, text FROM fts_segments LIMIT 5;").fetchall()
    for s in rows2:
        print(dict(s))

    # Return counts of NULL text rows to decide on rebuild
    nulls = con.execute("SELECT COUNT(*) FROM fts_segments WHERE text IS NULL;").fetchone()[0]
    total = con.execute("SELECT COUNT(*) FROM fts_segments;").fetchone()[0]
    return (nulls, total)

def rebuild_fts(con):
    print("\n== Rebuilding FTS from segments ==")
    # Clear a contentless FTS5 table:
    con.execute("INSERT INTO fts_segments(fts_segments) VALUES('delete-all');")
    # Repopulate from segments (rowid must mirror segments.id)
    con.execute("""
      INSERT INTO fts_segments (rowid, transcript_id, speaker_role, text)
      SELECT id, transcript_id, speaker_role, text
      FROM segments;
    """)
    con.commit()
    total = con.execute("SELECT COUNT(*) FROM fts_segments;").fetchone()[0]
    print(f"Rebuilt FTS. Rows now: {total}")

def ensure_triggers(con):
    print("\n== Ensuring FTS triggers exist ==")
    names = [r["name"] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='segments'").fetchall()]
    missing = {"seg_ai","seg_ad","seg_au"} - set(names)
    if not missing:
        print("Triggers present:", names)
        return
    print("Adding triggers:", ", ".join(sorted(missing)))
    con.executescript("""
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
    """)
    con.commit()
    print("Triggers ensured.")

def fts_sanity_search(con, query="father"):
    print(f"\n== Quick FTS sanity check (query={query!r}) ==")
    rows = con.execute("""
    SELECT t.source_id,
           s.start_ms,
           s.text AS preview
    FROM fts_segments
    JOIN segments s    ON s.id = fts_segments.rowid   -- <-- get real text from segments
    JOIN transcripts t ON t.id = s.transcript_id
    WHERE fts_segments MATCH ?
    ORDER BY t.id, s.start_ms
    LIMIT 10
    """, (query,)).fetchall()

    if not rows:
        print(f"(No FTS hits for: {query})  -- try editing the query term in this script.")
    else:
        for r in rows:
            txt = (r["preview"] or "")[:120]
            print(f"{r['source_id']} @{r['start_ms']}ms :: {txt}")

def trigger_self_test(con):
    print("\n== Trigger self-test (non-destructive) ==")
    # start an explicit transaction we will roll back
    con.execute("BEGIN;")
    try:
        tid = con.execute("SELECT id FROM transcripts ORDER BY id LIMIT 1;").fetchone()
        if not tid:
            print("(No transcripts to test with.)")
            con.execute("ROLLBACK;")
            return
        tid = tid[0]
        con.execute("""
          INSERT INTO segments (transcript_id,start_ms,end_ms,speaker_role,text)
          VALUES (?,?,?,?,?)
        """, (tid, 999000, 1000000, 'subject', 'zebra banana testphrase'))
        new_id = con.execute("SELECT last_insert_rowid();").fetchone()[0]
        fts_row = con.execute("SELECT rowid, text FROM fts_segments WHERE rowid=?;", (new_id,)).fetchone()
        if fts_row:
            print("✓ Trigger insert reflected in FTS:", dict(fts_row))
        else:
            print("✗ Trigger insert NOT reflected in FTS (check triggers).")
    finally:
        con.execute("ROLLBACK;")
        print("Rolled back test insert.")

def main():
    con = open_db()
    print_counts(con)
    sample_segments(con)

    # Look at FTS; if NULLs dominate, rebuild from segments
    fts_info = inspect_fts(con)
    if fts_info:
        nulls, total = fts_info
        if total == 0 or nulls == total:
            print("\n(FTS has NULL or zero rows; rebuilding from segments.)")
            rebuild_fts(con)

    ensure_triggers(con)

    # Run a sanity search — edit the query to a word you know appears
    fts_sanity_search(con, query="father")

    # Optional: confirm triggers with a safe test insert
    trigger_self_test(con)

    con.close()
    print("\nDone.")

if __name__ == "__main__":
    main()

