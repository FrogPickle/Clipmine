# db_ops.py — minimal, cleaned-up helpers for ClipMine
from __future__ import annotations
import sqlite3
from pathlib import Path
import re
from contextlib import closing
from collections import defaultdict, Counter

# ---------- Paths ----------
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "clipmine.db"   # adjust if your DB lives elsewhere

# ---------- Tunables ----------
MIN_SEED_TOKENS = 10     # require at least this many words in a seed window
DEFAULT_WINDOW_K = 3     # sliding window size used to build fts_windows
TOKEN_PAD_SEGS   = 8     # how many segments of context we fetch on each side for token refinement

# ---------- Connections ----------
def get_rw(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def get_ro(db_path: Path = DB_PATH) -> sqlite3.Connection:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)

# ---------- Rebuild contentless FTS: fts_windows ----------
def refresh_windows_all(conn: sqlite3.Connection, window_size: int = DEFAULT_WINDOW_K) -> None:
    """
    Atomic rebuild of ALL windows into existing fts_windows (contentless FTS5).
    Orders by start_ms, id; builds into a TEMP stage; swaps in one transaction.
    """
    conn.execute("BEGIN IMMEDIATE;")
    conn.execute("DROP TABLE IF EXISTS windows_stage;")
    conn.execute(
        """
        CREATE TEMP TABLE windows_stage AS
        WITH params(k) AS (SELECT ?),
             ordered AS (
               SELECT s.id, s.transcript_id, s.text,
                      ROW_NUMBER() OVER (
                        PARTITION BY s.transcript_id
                        ORDER BY s.start_ms, s.id
                      ) AS rn
               FROM segments s
             ),
             spans AS (
               SELECT o1.transcript_id,
                      MIN(o2.id) AS seg_start_id,
                      MAX(o2.id) AS seg_end_id,
                      TRIM(GROUP_CONCAT(o2.text, ' ')) AS text
               FROM ordered o1
               JOIN ordered o2
                 ON o2.transcript_id = o1.transcript_id
                AND o2.rn BETWEEN o1.rn AND o1.rn + (SELECT k FROM params) - 1
               GROUP BY o1.transcript_id, o1.rn
               HAVING COUNT(*) = (SELECT k FROM params)
             )
        SELECT transcript_id, seg_start_id, seg_end_id, text
        FROM spans;
        """,
        (int(window_size),),
    )

    cnt = conn.execute("SELECT COUNT(*) FROM windows_stage;").fetchone()[0]
    if cnt == 0:
        conn.execute("ROLLBACK;")
        raise RuntimeError("Refusing to swap in empty windows_stage (no spans built).")

    conn.execute("DELETE FROM fts_windows;")
    conn.execute(
        """
        INSERT INTO fts_windows(transcript_id, seg_start_id, seg_end_id, text)
        SELECT transcript_id, seg_start_id, seg_end_id, text
        FROM windows_stage;
        """
    )
    conn.commit()

# ---------- FTS helper ----------
def fts_hits(conn_ro: sqlite3.Connection, phrase: str):
    q = phrase.replace('"', '""')
    return conn_ro.execute(
        """
        SELECT rowid, transcript_id, seg_start_id, seg_end_id
        FROM fts_windows
        WHERE fts_windows MATCH '"' || ? || '"'
        """,
        (q,),
    ).fetchall()

# ---------- Tokenization & refinement ----------
_WORD = re.compile(r"[A-Za-z0-9']+")

def _grow_subset(streams, anchors_subset, debug=False):
    """
    streams: {tid: (tokens, tok2seg)}
    anchors_subset: {tid: [lo, hi]} token indices for this subset
    Expands RIGHT then LEFT while all subset members' next/prev tokens are equal.
    Returns new anchors {tid: [lo, hi]}.
    """
    active = set(anchors_subset.keys())
    # RIGHT
    while True:
        nxt = []
        for tid in active:
            tokens, _ = streams[tid]
            lo, hi = anchors_subset[tid]
            if hi + 1 >= len(tokens):
                nxt = None; break
            nxt.append(tokens[hi + 1])
        if not nxt or len(set(nxt)) != 1:
            break
        for tid in active:
            anchors_subset[tid][1] += 1
        if debug: print("→ subset step:", nxt[0])
    # LEFT
    while True:
        prv = []
        for tid in active:
            tokens, _ = streams[tid]
            lo, hi = anchors_subset[tid]
            if lo - 1 < 0:
                prv = None; break
            prv.append(tokens[lo - 1])
        if not prv or len(set(prv)) != 1:
            break
        for tid in active:
            anchors_subset[tid][0] -= 1
        if debug: print("← subset step:", prv[0])
    return anchors_subset

def refine_with_branching(
    conn_ro,
    per_tid_spans: dict[int, tuple[int, int]],
    seed_tid: int,
    seed_phrase: str,
    pad_segments: int = 8,          # kept for API compatibility
    min_child_size: int = 2,        # children only if already a group
    max_children_per_side: int = 4,
    debug: bool = False,
):
    """
    Tokenize FULL transcripts, anchor on seed_phrase near per_tid_spans.
    Expand RIGHT fully, then LEFT, always following the group containing the seed.
    Diverging groups are *frozen* (kept in parent with their last agreed bounds).
    Child branches are returned as CANDIDATES (not persisted).

    Guarantees:
      • Parent never expands with <2 active transcripts.
      • If survivors drop to <2, we fall back to the last state where ≥2 agreed.
    """

    pattern = _WORD.findall(seed_phrase.lower())
    if not pattern:
        return per_tid_spans, seed_phrase, []

    # 1) Tokenize FULL transcripts + locate anchor occurrence
    streams = {}   # tid -> (tokens, tok2seg)
    anchors = {}   # tid -> [lo, hi]
    for tid, (s_seg, e_seg) in per_tid_spans.items():
        rows = conn_ro.execute(
            "SELECT id, text, start_ms FROM segments WHERE transcript_id=? ORDER BY start_ms, id",
            (tid,)
        ).fetchall()
        ids  = [r[0] for r in rows]
        txts = [r[1] for r in rows]

        tokens, t2s = [], []
        for sid, t in zip(ids, txts):
            ws = _WORD.findall(t.lower())
            tokens.extend(ws)
            t2s.extend([sid]*len(ws))

        # choose the occurrence overlapping original span
        best = None; m = len(pattern)
        for i in range(0, len(tokens)-m+1):
            if tokens[i:i+m] == pattern:
                covered = t2s[i:i+m]
                overlap = min(max(covered), e_seg) - max(min(covered), s_seg) + 1
                if best is None or overlap > best[2]:
                    best = (i, i+m-1, overlap)
        if best is None:
            return per_tid_spans, seed_phrase, []

        anchors[tid] = [best[0], best[1]]
        streams[tid] = (tokens, t2s)

    frozen_outliers: dict[int, list[int]] = {}  # tid -> [lo,hi] at last agreement
    child_candidates = []

    # snapshot of the last state where ≥2 parent members still agreed
    last_multi_active: set[int] | None = set(anchors.keys())
    last_multi_snapshot: dict[int, list[int]] = {tid: a[:] for tid, a in anchors.items()}

    def make_child_snapshot(tids: set[int]):
        spans = {}
        rep_tid = seed_tid if seed_tid in tids else next(iter(tids))
        rep_tokens, _ = streams[rep_tid]
        lo, hi = anchors[rep_tid]
        phrase = " ".join(rep_tokens[lo:hi+1])
        for tid in tids:
            _, t2s = streams[tid]
            a_lo, a_hi = anchors[tid]
            segs = t2s[a_lo:a_hi+1]
            spans[tid] = (min(segs), max(segs))
        return {"spans": spans, "phrase": phrase}

    def branch(direction: str, start_active: set[int]) -> set[int]:
        nonlocal last_multi_active, last_multi_snapshot
        active = set(start_active)

        while True:
            if len(active) >= 2:
                # keep a fallback snapshot any time we still have ≥2
                last_multi_active = set(active)
                last_multi_snapshot = {tid: anchors[tid][:] for tid in active}

            groups = defaultdict(set)  # proposed token -> tids
            seed_exhausted = False
            exhausted = set()

            for tid in list(active):
                tokens, _ = streams[tid]
                lo, hi = anchors[tid]
                nxt_idx = (hi + 1) if direction == "RIGHT" else (lo - 1)
                if nxt_idx < 0 or nxt_idx >= len(tokens):
                    if tid == seed_tid: seed_exhausted = True
                    else: exhausted.add(tid)
                else:
                    groups[tokens[nxt_idx]].add(tid)

            if seed_exhausted:
                if debug: print(f"seed exhausted @{direction}")
                break

            # Freeze exhausted non-seed and remove from active
            if exhausted:
                for tid in exhausted:
                    frozen_outliers.setdefault(tid, anchors[tid][:])
                active -= exhausted
                if len(active) < 2:
                    # do NOT continue with single member; stop here
                    break

            if not groups:
                break

            # Choose parent group that includes seed if possible
            parent_token, parent_supporters = None, set()
            for tok, tids in groups.items():
                if seed_tid in tids:
                    parent_token, parent_supporters = tok, tids
                    break
            if parent_token is None:
                parent_token, parent_supporters = max(groups.items(), key=lambda kv: len(kv[1]))

            # Spawn children from other groups (size ≥ min_child_size), freeze & drop them
            spawned = 0
            for tok, tids in groups.items():
                if tok == parent_token: continue
                if len(tids) >= min_child_size and spawned < max_children_per_side:
                    child_candidates.append(make_child_snapshot(set(tids)))
                    spawned += 1
                for tid in (tids - parent_supporters):
                    frozen_outliers.setdefault(tid, anchors[tid][:])
                active -= (tids - parent_supporters)

            if len(active) < 2:
                # about to become single-member: stop now (we saved last_multi_snapshot)
                break

            # Advance parent one token
            for tid in parent_supporters:
                if direction == "RIGHT": anchors[tid][1] += 1
                else:                    anchors[tid][0] -= 1

        return active

    # RIGHT first, then LEFT — carrying only survivors forward
    survivors = branch("RIGHT", set(anchors.keys()))
    survivors = branch("LEFT", survivors)

    # If we ended up with <2 survivors, fall back to the last multi-member snapshot
    if not survivors or len(survivors) < 2:
        survivors = set(last_multi_active or [])
        # restore anchors for those survivors
        for tid in list(anchors.keys()):
            if tid in survivors:
                anchors[tid] = last_multi_snapshot[tid][:]
            else:
                # out of survivors: if we had frozen bounds, keep them; otherwise drop
                if tid not in frozen_outliers:
                    frozen_outliers[tid] = anchors[tid][:]

    # Final tighten within survivors only (but ONLY if ≥2)
    def grow_subset(anchors_subset: dict[int, list[int]]):
        act = set(anchors_subset.keys())
        if len(act) < 2:
            return anchors_subset  # <-- no single-member growth
        # RIGHT
        while True:
            nxt = []
            for tid in act:
                tokens, _ = streams[tid]
                lo, hi = anchors_subset[tid]
                if hi + 1 >= len(tokens): nxt = None; break
                nxt.append(tokens[hi + 1])
            if not nxt or len(set(nxt)) != 1: break
            for tid in act: anchors_subset[tid][1] += 1
        # LEFT
        while True:
            prv = []
            for tid in act:
                tokens, _ = streams[tid]
                lo, hi = anchors_subset[tid]
                if lo - 1 < 0: prv = None; break
                prv.append(tokens[lo - 1])
            if not prv or len(set(prv)) != 1: break
            for tid in act: anchors_subset[tid][0] -= 1
        return anchors_subset

    parent_live = {tid: anchors[tid][:] for tid in survivors}
    parent_live = grow_subset(parent_live)

    # Map to segment ranges
    def token_span_to_seg_span(tid: int, lo: int, hi: int):
        _, t2s = streams[tid]
        segs = t2s[lo:hi+1]
        return (min(segs), max(segs))

    parent_spans = {}
    for tid, a in parent_live.items():
        parent_spans[tid] = token_span_to_seg_span(tid, a[0], a[1])
    for tid, a in frozen_outliers.items():
        if tid not in parent_spans:
            parent_spans[tid] = token_span_to_seg_span(tid, a[0], a[1])

    # Parent phrase from seed within survivors
    seed_tokens, _ = streams[seed_tid]
    s_lo, s_hi = parent_live[seed_tid]
    parent_phrase = " ".join(seed_tokens[s_lo:s_hi+1])

    # De-dup child candidates identical to parent
    deduped_children = []
    parent_fp = (tuple(sorted(parent_spans.items())), parent_phrase)
    seen = {parent_fp}
    for ch in child_candidates:
        fp = (tuple(sorted(ch["spans"].items())), ch["phrase"])
        if ch["spans"] and fp not in seen:
            deduped_children.append(ch)
            seen.add(fp)

    return parent_spans, parent_phrase, deduped_children
 



def _word_tokens(s: str):
    return _WORD.findall(s.lower())

def _good_seed(phrase: str, min_tokens: int = MIN_SEED_TOKENS) -> bool:
    return len(_word_tokens(phrase)) >= min_tokens

def _fetch_ordered_segments(conn_ro: sqlite3.Connection, tid: int):
    rows = conn_ro.execute(
        """
        SELECT id, text
        FROM segments
        WHERE transcript_id=?
        ORDER BY start_ms, id
        """,
        (tid,),
    ).fetchall()
    ids  = [r[0] for r in rows]
    text = [r[1] for r in rows]
    id2idx = {sid: i for i, sid in enumerate(ids)}
    return ids, text, id2idx

def _tokens_with_segmap(seg_ids, seg_texts):
    tokens, tok2seg = [], []
    for sid, txt in zip(seg_ids, seg_texts):
        ws = _word_tokens(txt)
        tokens.extend(ws)
        tok2seg.extend([sid] * len(ws))
    return tokens, tok2seg

def _find_anchor_in_tokens(tokens, pattern, tok2seg, seg_lo, seg_hi):
    best = None
    m = len(pattern)
    for i in range(0, len(tokens) - m + 1):
        if tokens[i:i+m] == pattern:
            covered = tok2seg[i:i+m]
            overlap = min(max(covered), seg_hi) - max(min(covered), seg_lo) + 1
            if best is None or overlap > best[2]:
                best = (i, i+m-1, overlap)
    return best  # (tok_start, tok_end, overlap)

def refine_members_by_tokens(
    conn_ro,
    per_tid_spans: dict[int, tuple[int, int]],
    seed_tid: int,
    seed_phrase: str,
    pad_segments: int = 8,
    debug: bool = False,
):
    """
    Pure token-equality refinement:
    - find the seed phrase (tokenized) near each transcript's matched segment span
    - expand right/left one token at a time WHILE all transcripts' next/prev tokens are exactly equal
    """
    # tokenized seed anchor
    pattern = _WORD.findall(seed_phrase.lower())
    if not pattern:
        return per_tid_spans, seed_phrase

    # build token streams per transcript around the span
    streams = {}   # tid -> (tokens, tok2seg)
    anchors = {}   # tid -> [lo, hi] token indices
    for tid, (s_seg, e_seg) in per_tid_spans.items():
        rows = conn_ro.execute("""
            SELECT id, text, start_ms
            FROM segments
            WHERE transcript_id=?
            ORDER BY start_ms, id
        """, (tid,)).fetchall()
        ids  = [r[0] for r in rows]
        txts = [r[1] for r in rows]
        idx  = {sid:i for i, sid in enumerate(ids)}
        if not ids:
            return per_tid_spans, seed_phrase

        a = max(0, idx.get(s_seg, 0) - pad_segments)
        b = min(len(ids)-1, idx.get(e_seg, len(ids)-1) + pad_segments)

        # tokenize + segment map
        tokens, tok2seg = [], []
        for sid, t in zip(ids[a:b+1], txts[a:b+1]):
            ws = _WORD.findall(t.lower())
            tokens.extend(ws)
            tok2seg.extend([sid]*len(ws))

        # find the anchor occurrence overlapping [s_seg, e_seg]
        best = None
        m = len(pattern)
        for i in range(0, len(tokens)-m+1):
            if tokens[i:i+m] == pattern:
                covered = tok2seg[i:i+m]
                # overlap score uses seg id bounds just to bias toward the intended region
                overlap = min(max(covered), e_seg) - max(min(covered), s_seg) + 1
                if best is None or overlap > best[2]:
                    best = (i, i+m-1, overlap)
        if best is None:
            return per_tid_spans, seed_phrase  # fallback if anchor not found

        streams[tid] = (tokens, tok2seg)
        anchors[tid] = [best[0], best[1]]

    # expand RIGHT while next token matches in ALL transcripts
    while True:
        can = True
        nxt = []
        for tid, (tokens, _) in streams.items():
            lo, hi = anchors[tid]
            if hi + 1 >= len(tokens):
                can = False; break
            nxt.append(tokens[hi + 1])
        if not can or len(set(nxt)) != 1:
            break
        for tid in anchors:
            anchors[tid][1] += 1
        if debug:
            print("→ step:", nxt[0])

    # expand LEFT while prev token matches in ALL transcripts
    while True:
        can = True
        prv = []
        for tid, (tokens, _) in streams.items():
            lo, hi = anchors[tid]
            if lo - 1 < 0:
                can = False; break
            prv.append(tokens[lo - 1])
        if not can or len(set(prv)) != 1:
            break
        for tid in anchors:
            anchors[tid][0] -= 1
        if debug:
            print("← step:", prv[0])

    # map tokens back to segment-id spans and build the exact phrase (seed transcript)
    refined = {}
    seed_tokens, seed_t2s = streams[seed_tid]
    s_lo, s_hi = anchors[seed_tid]
    exact_tokens = seed_tokens[s_lo:s_hi+1]

    for tid, (tokens, t2s) in streams.items():
        lo, hi = anchors[tid]
        segs = t2s[lo:hi+1]
        refined[tid] = (min(segs), max(segs))

    return refined, " ".join(exact_tokens)

# ---------- Build first CU ----------
def build_first_cu(
    conn_rw: sqlite3.Connection,
    seed_tid: int = 1,
    window_size: int = DEFAULT_WINDOW_K,
    min_tokens: int = MIN_SEED_TOKENS,
):
    """
    Scan seed transcript left→right; find first window (of size k) that appears in ≥2 transcripts,
    greedily expand by segments, then refine bounds token-by-token across all matched transcripts.
    Writes canonical_units + cu_occurrences in one transaction and returns a summary dict.
    """
    with closing(get_ro()) as ro:
        # Preload ordered segment ids for seed
        seed_ids = [r[0] for r in ro.execute(
            "SELECT id FROM segments WHERE transcript_id=? ORDER BY start_ms, id",
            (seed_tid,),
        )]
        if len(seed_ids) < window_size:
            return None

        def seed_phrase_for(idx0: int, idx1: int) -> str:
            rows = ro.execute(
                """
                SELECT text FROM segments
                WHERE transcript_id=? AND id BETWEEN ? AND ?
                ORDER BY start_ms, id
                """,
                (seed_tid, seed_ids[idx0], seed_ids[idx1]),
            ).fetchall()
            return " ".join(t[0].strip() for t in rows).strip()

        def tids_from_hits(hs):
            return {h[1] for h in hs}

        # Walk windows in the seed
        for i in range(0, len(seed_ids) - window_size + 1):
            s_id = seed_ids[i]
            e_id = seed_ids[i + window_size - 1]
            phrase = seed_phrase_for(i, i + window_size - 1)

            # Skip tiny/junky seeds by token count only (no stripping of words)
            if not _good_seed(phrase, min_tokens=min_tokens):
                continue

            hits = fts_hits(ro, phrase)
            tids = tids_from_hits(hits)
            if len(tids) >= 2 and seed_tid in tids:
                # ---- Greedy expand by segments (right then left) ----
                cur_s, cur_e = s_id, e_id
                cur_hits = hits

                # expand RIGHT
                j = i + window_size
                while j < len(seed_ids):
                    cand_e = seed_ids[j]
                    cand_phrase = seed_phrase_for(i, j)
                    cand_hits = fts_hits(ro, cand_phrase)
                    if len(tids_from_hits(cand_hits)) >= 2:
                        cur_e = cand_e
                        cur_hits = cand_hits
                        j += 1
                    else:
                        break

                # expand LEFT
                j = i - 1
                while j >= 0:
                    cand_s = seed_ids[j]
                    rows = ro.execute(
                        """
                        SELECT text FROM segments
                        WHERE transcript_id=? AND id BETWEEN ? AND ?
                        ORDER BY start_ms, id
                        """,
                        (seed_tid, cand_s, cur_e),
                    ).fetchall()
                    cand_phrase = " ".join(t[0].strip() for t in rows).strip()
                    cand_hits = fts_hits(ro, cand_phrase)
                    if len(tids_from_hits(cand_hits)) >= 2:
                        cur_s = cand_s
                        cur_hits = cand_hits
                        j -= 1
                    else:
                        break

                # Collapse windows to min/max span per transcript
                per_tid = {}
                for _, tid, ws, we in cur_hits:
                    if tid not in per_tid:
                        per_tid[tid] = (ws, we)
                    else:
                        lo, hi = per_tid[tid]
                        per_tid[tid] = (min(lo, ws), max(hi, we))

                # ---- Branching token refinement ----
                parent_spans, parent_phrase, children = refine_with_branching(
                    ro,
                    per_tid_spans=per_tid,
                    seed_tid=seed_tid,
                    seed_phrase=phrase,          # seed window text is fine
                    pad_segments=TOKEN_PAD_SEGS,
                    min_child_size=1,            # <-- set 1 to keep singletons as children
                    max_children_per_side=4,
                    debug=False,
                )

                # ---- Persist parent ----
                conn_rw.execute("BEGIN IMMEDIATE;")
                parent_token_count = len(parent_phrase.split())
                conn_rw.execute(
                    "INSERT INTO canonical_units(rep_text, token_len) VALUES(?,?)",
                    (parent_phrase, parent_token_count),
                )
                parent_cu_id = conn_rw.execute("SELECT last_insert_rowid()").fetchone()[0]
                for tid, (ms, me) in parent_spans.items():
                    conn_rw.execute(
                        "INSERT INTO cu_occurrences(cu_id, transcript_id, segment_start_id, segment_end_id) VALUES(?,?,?,?)",
                        (parent_cu_id, tid, ms, me),
                    )

                # ---- Persist children (each as its own CU) ----
                for ch in children:
                    ch_phrase = ch["phrase"]
                    ch_spans  = ch["spans"]
                    ch_token_count = len(ch_phrase.split())
                    conn_rw.execute(
                        "INSERT INTO canonical_units(rep_text, token_len) VALUES(?,?)",
                        (ch_phrase, ch_token_count),
                    )
                    ch_cu_id = conn_rw.execute("SELECT last_insert_rowid()").fetchone()[0]
                    for tid, (ms, me) in ch_spans.items():
                        conn_rw.execute(
                            "INSERT INTO cu_occurrences(cu_id, transcript_id, segment_start_id, segment_end_id) VALUES(?,?,?,?)",
                            (ch_cu_id, tid, ms, me),
                        )

                conn_rw.commit()

                # Return parent summary (children are created as separate rows)
                return {
                    "cu_id": parent_cu_id,
                    "phrase": parent_phrase,
                    "members": parent_spans,
                    "children_created": len(children),
}


    return None

# ---------- Optional: clear CU output ----------
def clear_cus(conn_rw: sqlite3.Connection) -> None:
    conn_rw.execute("BEGIN IMMEDIATE;")
    conn_rw.execute("DELETE FROM cu_occurrences;")
    conn_rw.execute("DELETE FROM canonical_units;")
    conn_rw.commit()
