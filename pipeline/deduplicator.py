"""
pipeline/deduplicator.py
────────────────────────
Task 3 — Duplicate Detection & Flagging

Spec definition of a duplicate:
    Two emails are duplicates if they share ALL THREE of:
      1. Same from_address
      2. Same subject after stripping Re:/Fwd: prefixes
      3. Body similarity >= DUPLICATE_SIMILARITY_THRESHOLD (90%) via fuzz.ratio

Within each duplicate group:
    - Earliest email by date  → original  (left untouched in DB)
    - All later emails        → flagged   (is_duplicate=1, duplicate_of=<earliest msg_id>)
    - Latest email in group   → primary target for Task 4 notification

Outputs:
    - DB updates:          is_duplicate, duplicate_of, similarity_score columns
    - duplicates_report.csv: one row per flagged duplicate
    - Logged stats:        total groups, total flagged, average group size

Public interface:
    run(conn) -> dict     Pass an open sqlite3 connection. Returns stats dict.

Approach — Hash-first, then Fuzzy:
    Phase 1: SHA-256 hash of normalised body → instant exact-duplicate grouping.
             Newsletters (BTU Weekly, Energy Issues etc.) are byte-identical and
             collapse here in O(n) time with no fuzzy overhead.
    Phase 2: Fuzzy comparison (fuzz.ratio) ONLY between hash-group representatives.
             After exact dedup the representative pool is tiny, so fuzzy runs fast
             even for 150+ member groups.
    No body truncation needed — integrity is preserved across the full body.

── PREVIOUS APPROACH (body truncation + cdist) ──────────────────────────────────
The approach below was the prior implementation. It was replaced because truncating
bodies to 2000 chars introduced false-positive / false-negative risks for emails
where meaningful content falls after the cutoff (e.g. performance-review templates,
personalised newsletter headers). Kept here for reference in case the hash approach
needs to be reverted.

PREVIOUS _find_duplicate_clusters (truncation + cdist):
──────────────────────────────────────────────────────────
# _MIN_BODY_LEN = 20
#
# def _find_duplicate_clusters(members):
#     n = len(members)
#     uf = _UnionFind(n)
#     scores = {}
#     bodies = [m.get("body") or "" for m in members]
#
#     if n == 2:
#         # Fast path: direct fuzz.ratio, no cdist overhead, body truncated to 2000 chars
#         _BODY_TRUNCATE = 2000
#         body_0, body_1 = bodies[0][:_BODY_TRUNCATE], bodies[1][:_BODY_TRUNCATE]
#         if len(body_0) >= _MIN_BODY_LEN and len(body_1) >= _MIN_BODY_LEN:
#             score = float(fuzz.ratio(body_0, body_1))
#         else:
#             score = 0.0
#         scores[(0, 1)] = score
#         if score >= DUPLICATE_SIMILARITY_THRESHOLD:
#             uf.union(0, 1)
#
#     else:
#         # Matrix path: cdist on truncated bodies (2000 chars) to reduce cost
#         # for large newsletter groups (293-member Energy Issues, 159-member BTU).
#         # Tradeoff: may miss duplicates where content differs in first 2000 chars.
#         _BODY_TRUNCATE = 2000
#         truncated_bodies = [b[:_BODY_TRUNCATE] for b in bodies]
#         score_matrix = cdist(truncated_bodies, truncated_bodies, scorer=fuzz.ratio)
#
#         for i in range(n):
#             for j in range(i + 1, n):
#                 if len(bodies[i]) < _MIN_BODY_LEN or len(bodies[j]) < _MIN_BODY_LEN:
#                     scores[(i, j)] = 0.0
#                     continue
#                 score = float(score_matrix[i][j])
#                 scores[(i, j)] = score
#                 if score >= DUPLICATE_SIMILARITY_THRESHOLD:
#                     uf.union(i, j)
#
#     clusters = []
#     for index_group in uf.clusters():
#         cluster = [members[idx] for idx in index_group]
#         clusters.append((index_group, cluster))
#     return clusters, scores
──────────────────────────────────────────────────────────────────────────────────
"""

import re
import csv
import hashlib
from collections import defaultdict
from rapidfuzz import fuzz
# Note: cdist (from rapidfuzz.process) was used in the previous truncation approach.
# It is no longer needed — the hash-first approach avoids matrix comparisons entirely.

from config import DUPLICATE_SIMILARITY_THRESHOLD, DUPLICATES_REPORT
from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Subject normalisation
# ─────────────────────────────────────────────────────────────────────────────

# Regex that matches common reply/forward prefixes at the START of a subject.
# Handles: Re:, RE:, re:, Fwd:, FWD:, Fw:, FW:, Re[2]:, Re[3]: etc.
# The outer loop in _normalise_subject strips these repeatedly until none remain.
_SUBJECT_PREFIX_RE = re.compile(
    r'^(?:re|fwd?|fw)\s*(?:\[\d+\])?\s*:\s*',
    re.IGNORECASE
)


def _normalise_subject(subject: str) -> str:
    """
    Strip all leading Re:/Fwd: prefix variants from a subject line and
    return a lowercased, stripped string for grouping.

    Examples:
        "Re: FW: Re: California Update" → "california update"
        "RE:[2] Hello"                  → "hello"
        "Fwd: Fwd: Fwd: Test"          → "test"
        ""                             → ""

    We lowercase so that "Hello" and "hello" group together.
    We strip repeatedly in a loop because there can be multiple nested prefixes.
    """
    if not subject:
        return ""

    s = subject.strip()
    # Keep stripping prefixes until there are no more left
    while True:
        stripped = _SUBJECT_PREFIX_RE.sub("", s).strip()
        if stripped == s:
            break   # Nothing more to strip
        s = stripped

    return s.lower()


# ─────────────────────────────────────────────────────────────────────────────
# Union-Find (Disjoint Set Union) for cluster building
# ─────────────────────────────────────────────────────────────────────────────

class _UnionFind:
    """
    Simple Union-Find data structure for grouping connected emails.

    Used to handle the case where a group has 3+ emails:
        If A~B (≥90%) and B~C (≥90%), all three form one cluster
        even if A~C scores lower.

    This correctly implements the spec's requirement:
        "If a group has more than two emails, flag all except the earliest."
    """

    def __init__(self, n: int):
        # Each element starts as its own parent (n separate components)
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        """Find root of x with path compression for efficiency."""
        while self.parent[x] != x:
            # Path compression: point directly to grandparent
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x: int, y: int) -> None:
        """Merge the components containing x and y."""
        self.parent[self.find(x)] = self.find(y)

    def clusters(self) -> list[list[int]]:
        """
        Return all components as a list of lists of indices.
        Only returns components with 2+ members (single-member = not a duplicate).
        """
        groups = defaultdict(list)
        for i in range(len(self.parent)):
            groups[self.find(i)].append(i)
        return [g for g in groups.values() if len(g) >= 2]


# ─────────────────────────────────────────────────────────────────────────────
# Pairwise comparison within a candidate group — Hash-first, then Fuzzy
# ─────────────────────────────────────────────────────────────────────────────

# Minimum body length to attempt comparison.
# Two emails with empty or near-empty bodies (e.g. calendar stubs) would
# score 100% by default — that's a false positive. We skip those pairs.
_MIN_BODY_LEN = 20


def _normalise_for_hash(body: str) -> str:
    """
    Normalise a body string before hashing so that cosmetic differences
    (CRLF vs LF, leading/trailing whitespace) don't produce different hashes
    for what is effectively the same content.
    """
    return body.strip().replace("\r\n", "\n").replace("\r", "\n")


def _find_duplicate_clusters(
    members: list[dict],
) -> tuple[list[tuple], dict[tuple, float], list[str], dict[str, int]]:
    """
    Detect duplicate clusters within a candidate group using a two-phase approach:

    Phase 1 — Exact dedup (SHA-256 hash):
        Hash each body after normalising whitespace/line-endings.
        Emails with the same hash are byte-identical → score 100.0, union instantly.
        This handles newsletters (BTU Weekly, Energy Issues etc.) in O(n) time.

    Phase 2 — Fuzzy dedup (fuzz.ratio on representatives):
        After exact dedup each distinct hash group has one "representative" (rep).
        We compare reps pairwise with fuzz.ratio on the FULL body — no truncation.
        The rep pool is tiny (e.g. 159 BTU Weekly emails → typically 1-3 reps),
        so full-body fuzzy runs fast even for large original groups.
        If two reps score >= threshold, their entire hash groups are merged.

    Score propagation for the CSV report:
        - Intra-hash-group pair (i, j): score = 100.0 (stored explicitly)
        - Inter-hash-group rep pair (i, j): score = computed fuzz.ratio (stored)
        - Non-rep vs original (different hash groups): derived via hash_to_rep
          lookup in run() — the rep-to-rep score is used as the proxy.

    Args:
        members: List of dicts with keys: message_id, date, body.

    Returns:
        clusters:      List of (index_group, cluster_emails) tuples.
        scores:        Dict mapping (i, j) index pair → similarity score.
        body_hashes:   Per-member SHA-256 hash strings (index-aligned with members).
        hash_to_rep:   Maps each hash string → its representative index in members.
    """
    n = len(members)
    uf = _UnionFind(n)
    scores: dict[tuple, float] = {}

    bodies = [m.get("body") or "" for m in members]

    # ── Phase 1: Exact dedup via SHA-256 ─────────────────────────────────────
    body_hashes = [
        hashlib.sha256(
            _normalise_for_hash(b).encode("utf-8", errors="replace")
        ).hexdigest()
        for b in bodies
    ]

    # Group member indices by their hash
    hash_to_indices: dict[str, list[int]] = defaultdict(list)
    for i, h in enumerate(body_hashes):
        hash_to_indices[h].append(i)

    # Union all members within the same hash group → exact duplicates, score 100
    for indices in hash_to_indices.values():
        for k in range(len(indices)):
            for l in range(k + 1, len(indices)):
                i, j = indices[k], indices[l]
                scores[(min(i, j), max(i, j))] = 100.0
                uf.union(i, j)

    # ── Phase 2: Fuzzy comparison between hash-group representatives ──────────
    # One rep per distinct hash group. For the BTU Weekly example:
    #   159 emails → e.g. 157 share hash_A (rep=0), 1 has hash_B (rep=157),
    #   1 has hash_C (rep=158) → only 3 reps → 3 pairwise fuzzy comparisons.
    # This replaces what would have been 159*158/2 = 12,561 comparisons.

    # hash_to_rep: hash string → index of its representative in members[]
    hash_to_rep: dict[str, int] = {
        h: indices[0] for h, indices in hash_to_indices.items()
    }
    reps = list(hash_to_rep.values())  # one index per distinct hash group

    for k in range(len(reps)):
        for l in range(k + 1, len(reps)):
            i, j = reps[k], reps[l]
            body_i, body_j = bodies[i], bodies[j]

            # Skip pairs where either body is too short — avoids false positives
            # from empty/stub emails that would trivially score 100% each other.
            if len(body_i) < _MIN_BODY_LEN or len(body_j) < _MIN_BODY_LEN:
                scores[(min(i, j), max(i, j))] = 0.0
                continue

            # Full-body comparison — no truncation needed because the rep pool
            # is tiny after exact dedup collapses large newsletter groups.
            score = float(fuzz.ratio(body_i, body_j))
            scores[(min(i, j), max(i, j))] = score

            if score >= DUPLICATE_SIMILARITY_THRESHOLD:
                # Unioning the reps also connects all their hash-group members
                # because the reps are already roots (or connected) via Phase 1.
                uf.union(i, j)

    # Build cluster dicts from connected components (≥2 members only)
    clusters = []
    for index_group in uf.clusters():
        cluster = [members[idx] for idx in index_group]
        clusters.append((index_group, cluster))

    return clusters, scores, body_hashes, hash_to_rep


# ─────────────────────────────────────────────────────────────────────────────
# Database update
# ─────────────────────────────────────────────────────────────────────────────

def _flag_in_db(conn, all_groups: list[dict]) -> int:
    """
    Bulk-update the emails table to flag all duplicate records.

    For each duplicate in every group, sets:
        is_duplicate    = 1
        duplicate_of    = message_id of the original (earliest) email
        similarity_score = fuzz.ratio score against the original

    Uses executemany for efficiency — one DB round-trip for all updates
    rather than one per row.

    Returns:
        Total number of rows updated.
    """
    updates = []
    for group in all_groups:
        for dup in group["duplicates"]:
            updates.append((
                1,                              # is_duplicate = TRUE
                group["original_message_id"],   # duplicate_of → original
                round(dup["similarity_score"], 4),  # similarity_score
                dup["message_id"],              # WHERE message_id = ?
            ))

    if updates:
        conn.executemany(
            """UPDATE emails
               SET is_duplicate     = ?,
                   duplicate_of     = ?,
                   similarity_score = ?
               WHERE message_id = ?""",
            updates,
        )

    logger.info("Flagged %d duplicate rows in the database.", len(updates))
    return len(updates)


# ─────────────────────────────────────────────────────────────────────────────
# CSV report
# ─────────────────────────────────────────────────────────────────────────────

def _write_report(all_groups: list[dict]) -> None:
    """
    Write duplicates_report.csv with one row per flagged duplicate email.

    Columns (per spec Section 3):
        duplicate_message_id  — message_id of the flagged (later) email
        original_message_id   — message_id of the earliest email in the group
        subject               — normalised subject shared by the group
        from_address          — sender (same for all emails in the group)
        duplicate_date        — date of the flagged email
        original_date         — date of the original email
        similarity_score      — fuzz.ratio score (0–100) of duplicate vs original

    All flagged duplicates appear here — not just the latest one.
    The latest duplicate per group is identified separately for Task 4.
    """
    fieldnames = [
        "duplicate_message_id",
        "original_message_id",
        "subject",
        "from_address",
        "duplicate_date",
        "original_date",
        "similarity_score",
    ]

    total_rows = 0
    with open(DUPLICATES_REPORT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for group in all_groups:
            for dup in group["duplicates"]:
                writer.writerow({
                    "duplicate_message_id":  dup["message_id"],
                    "original_message_id":   group["original_message_id"],
                    "subject":               group["subject"],
                    "from_address":          group["from_address"],
                    "duplicate_date":        dup["date"],
                    "original_date":         group["original_date"],
                    "similarity_score":      round(dup["similarity_score"], 2),
                })
                total_rows += 1

    logger.info("Wrote %d rows to %s", total_rows, DUPLICATES_REPORT)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(conn) -> dict:
    """
    Run the full Task 3 duplicate detection pipeline.

    Steps:
      1. Load all email metadata (no bodies yet) from DB.
      2. Normalise subjects and group emails by (from_address, normalised_subject).
      3. Filter to candidate groups (≥ 2 emails) — skip obvious non-duplicates early.
      4. Fetch bodies ONLY for emails in candidate groups (saves memory).
      5. Within each candidate group: Phase 1 exact hash dedup, Phase 2 fuzzy on reps.
      6. For each cluster: earliest = original, rest = duplicates.
      7. Bulk-update the DB to flag duplicates.
      8. Write duplicates_report.csv.
      9. Compute and return statistics.

    Args:
        conn: Open sqlite3 connection with schema already applied and
              emails table populated (run extractor.run() first).

    Returns:
        Stats dict:
            total_groups   — number of duplicate groups found
            total_flagged  — total emails flagged as is_duplicate=1
            avg_group_size — average emails per duplicate group (including original)
    """

    # ── Step 0: Clear any existing duplicate flags ────────────────────────────
    # Task 3 must be idempotent: re-running it should always produce the same
    # result, not accumulate stale flags from a previous run.
    # Without this, old false positives (e.g. the 66% transitive cases from a
    # prior run) would remain flagged in the DB even after the stricter filter
    # is applied, because _flag_in_db() only sets flags — it never clears them.
    logger.info("Step 0: Clearing previous duplicate flags...")
    conn.execute(
        "UPDATE emails SET is_duplicate = 0, duplicate_of = NULL, similarity_score = NULL"
    )
    conn.commit()
    logger.info("Step 0: Previous flags cleared.")

    # ── Step 1: Load metadata for all emails ─────────────────────────────────
    # We load only the fields needed for grouping first (no body yet).
    # This keeps memory usage low for the initial grouping pass.
    logger.info("Step 1: Loading email metadata from database...")
    rows = conn.execute(
        "SELECT message_id, from_address, subject, date FROM emails"
    ).fetchall()
    logger.info("Loaded %d email records.", len(rows))

    # ── Step 2: Group by (from_address, normalised_subject) ──────────────────
    # Two emails can only be duplicates if they share the same sender and
    # the same subject (with Re:/Fwd: noise stripped). This is the cheap
    # pre-filter before we run the expensive body comparison.
    logger.info("Step 2: Grouping emails by sender + normalised subject...")
    candidate_groups: dict[tuple, list[dict]] = defaultdict(list)

    for row in rows:
        norm_subj = _normalise_subject(row["subject"] or "")

        # Skip emails with no subject after normalisation — too ambiguous
        # to reliably match as duplicates without a subject anchor.
        if not norm_subj:
            continue

        key = (row["from_address"], norm_subj)
        candidate_groups[key].append({
            "message_id": row["message_id"],
            "date":       row["date"] or "",
        })

    # ── Step 3: Filter to groups with ≥ 2 emails ─────────────────────────────
    # Single-member groups are definitionally not duplicates — drop them.
    multi_groups = {
        key: members
        for key, members in candidate_groups.items()
        if len(members) >= 2
    }
    logger.info(
        "Step 3: %d candidate groups with ≥2 emails (from %d total groups).",
        len(multi_groups), len(candidate_groups),
    )

    # ── Step 4: Batch-fetch bodies only for candidate emails ─────────────────
    # Collecting all message_ids in candidate groups and fetching their
    # bodies in one query. This avoids one DB round-trip per group.
    candidate_ids = {
        m["message_id"]
        for members in multi_groups.values()
        for m in members
    }
    logger.info("Step 4: Fetching bodies for %d candidate emails...", len(candidate_ids))

    # SQLite has a variable limit per query (~32k). Chunk if needed.
    body_map: dict[str, str] = {}
    candidate_id_list = list(candidate_ids)
    chunk_size = 500  # safe batch size well under SQLite's variable limit

    for start in range(0, len(candidate_id_list), chunk_size):
        chunk = candidate_id_list[start : start + chunk_size]
        placeholders = ",".join("?" * len(chunk))
        body_rows = conn.execute(
            f"SELECT message_id, body FROM emails WHERE message_id IN ({placeholders})",
            chunk,
        ).fetchall()
        for r in body_rows:
            body_map[r["message_id"]] = r["body"] or ""

    # ── Step 5: Hash-first dedup + fuzzy on representatives ──────────────────
    # For each candidate group, Phase 1 collapses exact copies via SHA-256 hash
    # (handles newsletters instantly). Phase 2 runs fuzz.ratio only between
    # hash-group representatives — a tiny pool after exact dedup.
    logger.info("Step 5: Running hash dedup + fuzzy comparison within candidate groups...")
    all_groups: list[dict] = []
    total_comparisons = 0

    for (from_addr, norm_subj), members in multi_groups.items():

        # Attach the fetched body to each member dict
        for m in members:
            m["body"] = body_map.get(m["message_id"], "")

        if len(members) > 50:
            logger.debug(
                "Large candidate group (%d members): (%s, %s)",
                len(members), from_addr, norm_subj[:40],
            )

        # Run Phase 1 (exact hash) + Phase 2 (fuzzy on reps)
        clusters_with_indices, scores, body_hashes, hash_to_rep = \
            _find_duplicate_clusters(members)

        # Count comparisons: exact pairs + fuzzy rep pairs
        n_reps = len(hash_to_rep)
        total_comparisons += n_reps * (n_reps - 1) // 2

        # ── Step 6: Process each cluster ─────────────────────────────────────
        for index_group, cluster_emails in clusters_with_indices:

            # Sort cluster by date ascending: earliest = original
            cluster_emails_indexed = list(zip(index_group, cluster_emails))
            cluster_emails_indexed.sort(key=lambda x: x[1]["date"])

            original_idx, original = cluster_emails_indexed[0]
            duplicates_indexed     = cluster_emails_indexed[1:]

            group_result = {
                "original_message_id": original["message_id"],
                "original_date":       original["date"],
                "from_address":        from_addr,
                "subject":             norm_subj,
                "duplicates":          [],
            }

            for dup_idx, dup in duplicates_indexed:
                pair_key = (min(original_idx, dup_idx), max(original_idx, dup_idx))

                if pair_key in scores:
                    # Direct score stored: either intra-hash-group (100.0)
                    # or rep-to-rep fuzzy score.
                    score = scores[pair_key]
                elif body_hashes[original_idx] == body_hashes[dup_idx]:
                    # Same hash group but pair not explicitly stored — exact match.
                    score = 100.0
                else:
                    # Non-rep vs original in different hash groups.
                    # Use the rep-to-rep score as a proxy: every non-rep in a
                    # hash group is byte-identical to its rep, so the rep's
                    # score against the original applies equally to all members.
                    rep_orig = hash_to_rep[body_hashes[original_idx]]
                    rep_dup  = hash_to_rep[body_hashes[dup_idx]]
                    score = scores.get(
                        (min(rep_orig, rep_dup), max(rep_orig, rep_dup)), 0.0
                    )

                # ── Strict pairwise filter ────────────────────────────────────
                # Union-Find can pull in emails transitively: if A~B (≥90%) and
                # B~C (≥90%), C ends up in A's cluster even if A~C scores 66%.
                # The spec defines duplicates as pairs scoring ≥90% directly —
                # there is no mention of transitivity. An email that scores below
                # the threshold against the original is a false positive caused
                # by the chain effect, not a genuine duplicate. We drop it here.
                #
                # Tradeoff: we may miss a few "drifted" duplicates (e.g. a
                # template edited slightly over several versions) where no single
                # version scores ≥90% against the very first one. But a 66–88%
                # score is too ambiguous to confidently call a duplicate, and
                # the spec's wording is explicit: the threshold applies pairwise.
                if score < DUPLICATE_SIMILARITY_THRESHOLD:
                    logger.debug(
                        "Dropping transitive false positive: %s vs original %s "
                        "(direct score %.1f%% < threshold %d%%)",
                        dup["message_id"][:40],
                        original["message_id"][:40],
                        score,
                        DUPLICATE_SIMILARITY_THRESHOLD,
                    )
                    continue  # skip — not a genuine duplicate of this original

                group_result["duplicates"].append({
                    "message_id":       dup["message_id"],
                    "date":             dup["date"],
                    "similarity_score": score,
                })

            # Only register this group if at least one genuine duplicate remains
            # after the strict pairwise filter. A cluster whose only members were
            # transitive false positives produces an empty duplicates list and
            # should not appear in the report or DB.
            if group_result["duplicates"]:
                all_groups.append(group_result)

    logger.info(
        "Step 5-6 complete: %d effective pairwise comparisons, %d duplicate groups found.",
        total_comparisons, len(all_groups),
    )

    # ── Step 7: Update the database ──────────────────────────────────────────
    logger.info("Step 7: Updating database flags for duplicate records...")
    total_flagged = _flag_in_db(conn, all_groups)
    conn.commit()

    # ── Step 8: Write duplicates_report.csv ──────────────────────────────────
    logger.info("Step 8: Writing duplicates_report.csv...")
    _write_report(all_groups)

    # ── Step 9: Compute and return stats ─────────────────────────────────────
    total_groups = len(all_groups)
    avg_group_size = (
        round((total_flagged + total_groups) / total_groups, 2)
        if total_groups > 0 else 0.0
    )

    stats = {
        "total_groups":    total_groups,
        "total_flagged":   total_flagged,
        "avg_group_size":  avg_group_size,
    }

    logger.info(
        "Duplicate detection complete: %d groups found, %d emails flagged, "
        "average group size %.2f",
        total_groups, total_flagged, avg_group_size,
    )

    return stats
