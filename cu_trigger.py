# OK
import argparse
from db_ops import get_rw, refresh_windows_all, build_first_cu

def main():
    ap = argparse.ArgumentParser(description="Trigger: rebuild windows (optional) + build first CU")
    ap.add_argument("--seed", type=int, default=1, help="Seed transcript id (default 1)")
    ap.add_argument("--k", type=int, default=2, help="Window size (default 2)")
    ap.add_argument("--refresh", action="store_true", help="Rebuild fts_windows before building")
    ap.add_argument("--clear", action="store_true", help="Clear canonical_units & cu_occurrences first")
    args = ap.parse_args()

    with get_rw() as conn:
        if args.clear:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM cu_occurrences")
            conn.execute("DELETE FROM canonical_units")
            conn.commit()
            print("Cleared canonical_units and cu_occurrences.")

        if args.refresh:
            refresh_windows_all(conn, window_size=args.k)
            n = conn.execute("SELECT COUNT(*) FROM fts_windows").fetchone()[0]
            print(f"Rebuilt fts_windows (k={args.k}) → {n} rows.")

        result = build_first_cu(conn, seed_tid=args.seed, window_size=args.k)
        if not result:
            print("No CU found starting from the seed transcript.")
            return

        # ...
        print("\n✅ Built first CU")
        print(f"CU ID: {result['cu_id']}")
        phrase = result['phrase']
        print(f"Tokens: {len(phrase.split())}")
        print("Phrase:", (phrase[:1000] + " …") if len(phrase) > 1000 else phrase)
        print("Members:")

        # NEW: fetch source_id for each transcript id in one query
        member_items = sorted(result["members"].items())  # [(tid, (s,e)), ...]
        tids = [tid for tid, _ in member_items]
        src_map = {}
        if tids:
            placeholders = ",".join(["?"] * len(tids))
            rows = conn.execute(
                f"SELECT id, source_id FROM transcripts WHERE id IN ({placeholders})",
                tids
            ).fetchall()
            src_map = {row[0]: row[1] for row in rows}  # id -> source_id

        for tid, (s, e) in member_items:
            src = src_map.get(tid, "?")
            print(f"  T{tid} [{src}]: segs {s}-{e}")
            
        if "children_created" in result:
            print(f"Children CUs created: {result['children_created']}")


if __name__ == "__main__":
    main()
