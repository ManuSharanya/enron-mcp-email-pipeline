"""
pipeline/extractor.py
─────────────────────
Task 1 — Data Extraction Pipeline (orchestration layer).

This is the top-level coordinator for Task 1. It:
  1. Walks each selected mailbox directory and discovers every email file.
  2. For each file, calls utils.email_parser.parse() to extract fields.
  3. On success: calls storage.insert_email() + storage.insert_addresses().
  4. On failure: calls logger.log_parse_error() and continues (no crash).
  5. Commits to the database every BATCH_SIZE emails for performance.
  6. After all files: calls storage.verify() and computes + returns stats.

Public interface:
    run(conn) -> dict     Runs the full extraction. conn is an open sqlite3
                          connection with schema already applied.
"""

import os
from pathlib import Path

from config import MAILDIR_ROOT, SELECTED_MAILBOXES, BASE_DIR, BATCH_SIZE
from utils import email_parser
from utils.email_parser import ParseError
from utils.logger import get_logger, log_parse_error
from pipeline import storage

logger = get_logger(__name__)

# The parent of MAILDIR_ROOT is used to compute relative source_file paths.
# e.g.  absolute: .../enron_mail_20150507/maildir/kaminski-v/inbox/1.
#        relative:              maildir/kaminski-v/inbox/1.
_MAILDIR_PARENT = Path(MAILDIR_ROOT).parent


def _discover_files(mailbox_path: Path):
    """
    Yield every file path under a mailbox directory, recursively.

    We use os.walk() rather than Path.rglob() + path.is_file() because on
    Windows, files whose names end with a dot (e.g. "1.", "10.") cause
    is_file() to return False — the Win32 API silently strips trailing dots
    during stat() calls. os.walk() delivers filenames directly from the
    directory listing, bypassing that normalisation step.

    Hidden files and directories (name starts with ".") are skipped — they
    are OS metadata files (.DS_Store, desktop.ini), not email data.

    Args:
        mailbox_path: Path to one mailbox directory (e.g. .../kaminski-v/).

    Yields:
        pathlib.Path objects for each discovered file.
    """
    for dirpath, dirnames, filenames in os.walk(mailbox_path):
        # Prune hidden subdirectories in-place so os.walk() doesn't descend into them
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        for filename in filenames:
            if filename.startswith("."):
                continue  # Skip hidden OS metadata files
            yield Path(dirpath) / filename


def _process_file(filepath: Path, conn, stats: dict) -> bool:
    """
    Parse one email file and insert it into the database.

    This is the per-file unit of work. All exceptions are caught here
    so that one bad file never stops the pipeline.

    On success:
      - Inserts the email row via storage.insert_email().
      - Inserts address rows for to/cc/bcc via storage.insert_addresses().
      - Updates stats["parsed"] and per-field counters.
      - Returns True.

    On failure (ParseError or unexpected exception):
      - Logs the error to error_log.txt via log_parse_error().
      - Updates stats["failed"].
      - Returns False.

    Args:
        filepath: Path to the email file to process.
        conn:     Open sqlite3 connection (transaction not committed here).
        stats:    Mutable stats dict updated in-place.

    Returns:
        True if the file was successfully parsed and inserted, False otherwise.
    """
    # Compute the source_file as a relative path from the project root.
    # The spec example: "maildir/lay-k/inbox/45"
    # We use forward slashes for cross-platform consistency in the DB.
    try:
        source_file = filepath.relative_to(_MAILDIR_PARENT).as_posix()
    except ValueError:
        # Fallback: if relative_to fails, use the absolute path
        source_file = str(filepath)

    try:
        # ── Parse the email file ───────────────────────────────────────────
        record = email_parser.parse(filepath, source_file)

        # ── Insert main email row ──────────────────────────────────────────
        # insert_email returns False if message_id already exists (dupe skip).
        inserted = storage.insert_email(conn, record)

        if inserted:
            # ── Insert normalised address rows ─────────────────────────────
            # Each address list goes into the email_addresses table with its
            # field label ("to", "cc", "bcc").
            msg_id = record["message_id"]
            storage.insert_addresses(conn, msg_id, "to",  record.get("to_addresses",  []))
            storage.insert_addresses(conn, msg_id, "cc",  record.get("cc_addresses",  []))
            storage.insert_addresses(conn, msg_id, "bcc", record.get("bcc_addresses", []))

            # ── Update per-field completeness counters ─────────────────────
            # We track how often each optional field is present so the final
            # stats can report field-level completeness percentages.
            for field in _OPTIONAL_TRACKED_FIELDS:
                value = record.get(field)
                # A field "counts" as present if it's non-None and non-empty
                if value is not None and value != "" and value != [] and value is not False:
                    stats["field_counts"][field] = stats["field_counts"].get(field, 0) + 1

            # Track to_addresses specifically (it's mandatory but variable)
            if record.get("to_addresses"):
                stats["field_counts"]["to_addresses"] = stats["field_counts"].get("to_addresses", 0) + 1

            stats["parsed"] += 1
        else:
            # The row was silently skipped by INSERT OR IGNORE (exact dupe).
            # Count it separately so we know how many duplicate message_ids exist.
            stats["skipped_duplicates"] = stats.get("skipped_duplicates", 0) + 1

        return inserted

    except ParseError as exc:
        # Expected failure: a mandatory field was missing or unparseable.
        log_parse_error(source_file, str(exc))
        stats["failed"] += 1
        return False

    except Exception as exc:
        # Unexpected failure: malformed file, encoding error, etc.
        # We catch broadly here so the pipeline never crashes mid-run.
        log_parse_error(source_file, f"Unexpected error: {type(exc).__name__}: {exc}")
        stats["failed"] += 1
        return False


# Fields we track for the optional-field completeness report.
# Mandatory fields (message_id, date, from_address, subject, body, source_file)
# are always present in a successfully parsed record by definition.
_OPTIONAL_TRACKED_FIELDS = [
    "cc_addresses", "bcc_addresses",
    "x_from", "x_to", "x_cc", "x_bcc",
    "x_folder", "x_origin",
    "content_type", "has_attachment",
    "forwarded_content", "quoted_content", "headings",
]


def _compute_stats(stats: dict) -> dict:
    """
    Build the final statistics summary from the accumulated counters.

    Calculates:
      - Total files found, parsed, failed, skipped.
      - Parse success rate as a percentage.
      - Per-field completeness rate (% of successfully parsed emails
        where each field is non-null / non-empty).

    Args:
        stats: The mutable dict that _process_file() updated throughout the run.

    Returns:
        The same stats dict, augmented with "success_rate" and
        "field_completeness" keys.
    """
    parsed = stats.get("parsed", 0)
    if parsed > 0:
        stats["success_rate"] = round(parsed / stats["total_found"] * 100, 2)

        # Completeness = how often each field was non-empty across parsed emails
        completeness = {}
        # Mandatory fields that are always present if parsing succeeded
        for field in ["message_id", "date", "from_address", "subject", "body", "source_file"]:
            completeness[field] = 100.0  # By definition — ParseError if absent

        completeness["to_addresses"] = round(
            stats["field_counts"].get("to_addresses", 0) / parsed * 100, 2
        )
        for field in _OPTIONAL_TRACKED_FIELDS:
            completeness[field] = round(
                stats["field_counts"].get(field, 0) / parsed * 100, 2
            )
        stats["field_completeness"] = completeness
    else:
        stats["success_rate"] = 0.0
        stats["field_completeness"] = {}

    return stats


def run(conn) -> dict:
    """
    Run the full Task 1 extraction pipeline.

    Iterates over SELECTED_MAILBOXES, discovers all email files, parses
    each one, inserts into the DB in batches, then computes and returns
    the final statistics dict.

    The stats dict structure:
        {
          "total_found":       int,   # files discovered
          "parsed":            int,   # successfully inserted
          "failed":            int,   # parse or insert errors
          "skipped_duplicates":int,   # exact message_id dupes skipped
          "success_rate":      float, # parsed / total_found * 100
          "field_counts":      dict,  # per-field presence counts
          "field_completeness":dict,  # per-field presence percentages
        }

    Args:
        conn: Open sqlite3 connection with schema already applied
              (call storage.init_schema(conn) before run()).

    Returns:
        The final stats dict.
    """
    stats = {
        "total_found":        0,
        "parsed":             0,
        "failed":             0,
        "skipped_duplicates": 0,
        "field_counts":       {},   # populated by _process_file
    }

    # batch_count tracks how many files we've processed since the last commit.
    # We commit every BATCH_SIZE files to balance write performance vs.
    # data durability (a crash loses at most BATCH_SIZE records, not all).
    batch_count = 0

    for mailbox_name in SELECTED_MAILBOXES:
        mailbox_path = Path(MAILDIR_ROOT) / mailbox_name

        if not mailbox_path.exists():
            logger.warning("Mailbox directory not found, skipping: %s", mailbox_path)
            continue

        logger.info("Processing mailbox: %s", mailbox_name)
        mailbox_file_count = 0

        for filepath in _discover_files(mailbox_path):
            stats["total_found"] += 1
            mailbox_file_count   += 1

            # Process the file — all errors are caught inside _process_file
            _process_file(filepath, conn, stats)

            batch_count += 1
            if batch_count >= BATCH_SIZE:
                # Commit the current batch to disk and reset the counter
                conn.commit()
                batch_count = 0
                logger.debug(
                    "Committed batch. Total so far: %d parsed, %d failed",
                    stats["parsed"], stats["failed"],
                )

        logger.info(
            "Finished mailbox %s: %d files found",
            mailbox_name, mailbox_file_count,
        )

    # Commit any remaining rows that didn't fill a full batch
    conn.commit()
    logger.info("All mailboxes processed. Final commit done.")

    # Run DB row counts as a sanity check
    db_counts = storage.verify(conn)
    stats["db_rows_emails"]          = db_counts.get("emails", 0)
    stats["db_rows_email_addresses"] = db_counts.get("email_addresses", 0)

    # Compute success rate and field-level completeness percentages
    _compute_stats(stats)

    return stats
