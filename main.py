"""
main.py
───────
Entry point for the Enron email pipeline. Run with:

    python main.py                        # Tasks 1–3 only (dry-run for Task 4)
    python main.py --send-live            # Tasks 1–4 with live MCP email send
    python main.py --skip-extract         # Skip Task 1, run Tasks 3–4 on existing DB
    python main.py --skip-extract --send-live  # Most common for re-testing Task 4

Environment:
  Secrets are loaded from .env via python-dotenv.  The file must contain:
    ANTHROPIC_API_KEY          — Anthropic API key for Claude
    NOTIFICATION_OVERRIDE_EMAIL — Gmail address to receive all test notifications
  See .env.example for the template.
"""

import argparse
import sys

# Load .env before anything else so ANTHROPIC_API_KEY and
# NOTIFICATION_OVERRIDE_EMAIL are available to all modules at import time.
from dotenv import load_dotenv
load_dotenv()

import utils.logger as logger_setup
from pipeline import storage, extractor, deduplicator, notifier


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    --send-live  If set, Task 4 will send real notification emails via
                 the Gmail MCP server. Without this flag the notifier
                 runs in dry-run mode and only writes .eml draft files.
    """
    parser = argparse.ArgumentParser(
        description="Enron email data extraction pipeline",
    )
    parser.add_argument(
        "--send-live",
        action="store_true",
        default=False,
        help="Send live notification emails via MCP (Task 4). "
             "Omit for dry-run mode.",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        default=False,
        help="Skip Task 1 extraction (use when DB is already populated). "
             "Jumps straight to Task 3 duplicate detection.",
    )
    parser.add_argument(
        "--task4-only",
        action="store_true",
        default=False,
        help="Skip Tasks 1 and 3 (extraction + dedup). Run Task 4 only. "
             "Use when the DB is already populated and duplicates already flagged.",
    )
    return parser.parse_args()


def print_stats(stats: dict) -> None:
    """
    Print the Task 1 extraction statistics to stdout in a readable table.

    Called after extractor.run() completes so the user gets an immediate
    summary without needing to open error_log.txt or query the database.
    """
    print("\n" + "=" * 60)
    print("  TASK 1 — EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"  Total files found:        {stats['total_found']:>10,}")
    print(f"  Successfully parsed:      {stats['parsed']:>10,}  ({stats.get('success_rate', 0):.1f}%)")
    print(f"  Failed (logged):          {stats['failed']:>10,}")
    print(f"  Skipped (exact dupes):    {stats.get('skipped_duplicates', 0):>10,}")
    print(f"  DB rows — emails:         {stats.get('db_rows_emails', 0):>10,}")
    print(f"  DB rows — email_addresses:{stats.get('db_rows_email_addresses', 0):>10,}")

    completeness = stats.get("field_completeness", {})
    if completeness:
        print("\n  Field completeness (of successfully parsed emails):")
        print("  " + "-" * 44)
        # Print mandatory fields first, then optional
        mandatory = [
            "message_id", "date", "from_address", "to_addresses",
            "subject", "body", "source_file",
        ]
        optional = [
            "cc_addresses", "bcc_addresses", "x_from", "x_to",
            "x_cc", "x_bcc", "x_folder", "x_origin",
            "content_type", "has_attachment",
            "forwarded_content", "quoted_content", "headings",
        ]
        for field in mandatory + optional:
            pct = completeness.get(field)
            if pct is not None:
                label = "(mandatory)" if field in mandatory else "(optional) "
                print(f"  {field:<24} {label}  {pct:>6.1f}%")

    print("=" * 60 + "\n")


def print_dedup_stats(stats: dict) -> None:
    """
    Print the Task 3 duplicate detection statistics to stdout.
    Called after deduplicator.run() completes.
    """
    print("\n" + "=" * 60)
    print("  TASK 3 — DUPLICATE DETECTION COMPLETE")
    print("=" * 60)
    print(f"  Duplicate groups found:   {stats['total_groups']:>10,}")
    print(f"  Emails flagged:           {stats['total_flagged']:>10,}")
    print(f"  Average group size:       {stats['avg_group_size']:>10.2f}")
    print(f"  Report written to:        duplicates_report.csv")
    print("=" * 60 + "\n")


def print_notif_stats(stats: dict, send_live: bool) -> None:
    """
    Print the Task 4 notification statistics to stdout.
    Called after notifier.run() completes.
    """
    mode = "LIVE" if send_live else "DRY-RUN"
    print("\n" + "=" * 60)
    print(f"  TASK 4 — NOTIFICATION EMAILS COMPLETE ({mode})")
    print("=" * 60)
    if send_live:
        print(f"  Emails sent:              {stats['total_sent']:>10,}")
        print(f"  Failed:                   {stats['total_failed']:>10,}")
    else:
        print(f"  Draft .eml files written: {stats['total_dry_run']:>10,}")
        print(f"  Location:                 output/replies/")
    print(f"  Log written to:           output/send_log.csv")
    print("=" * 60 + "\n")


def main() -> int:
    """
    Orchestrate the full pipeline.

    Returns:
        0 on success, 1 on unrecoverable error (used as process exit code).
    """
    args = parse_args()

    # ── Setup logging ─────────────────────────────────────────────────────────
    # Must be called before any other module logs anything.
    logger_setup.setup()
    logger = logger_setup.get_logger(__name__)
    logger.info("Pipeline starting. send_live=%s", args.send_live)

    # ── Open database and apply schema ────────────────────────────────────────
    conn = storage.get_connection()
    storage.init_schema(conn)

    try:
        # ── Task 1: Data Extraction ───────────────────────────────────────────
        if args.task4_only or args.skip_extract:
            logger.info("Task 1: Skipped. Using existing DB.")
        else:
            logger.info("Starting Task 1: Data Extraction")
            stats = extractor.run(conn)
            print_stats(stats)

        # ── Task 2: Sample queries ────────────────────────────────────────────
        # Schema and queries are in sample_queries.sql — run manually in
        # DB Browser or via: sqlite3 enron_emails.db < sample_queries.sql
        logger.info("Task 2 complete: schema + indexes + sample_queries.sql ready.")

        # ── Task 3: Duplicate Detection ───────────────────────────────────────
        if args.task4_only:
            logger.info("Task 3: Skipped (--task4-only flag set). Using existing duplicate flags.")
        else:
            logger.info("Starting Task 3: Duplicate Detection")
            dedup_stats = deduplicator.run(conn)
            print_dedup_stats(dedup_stats)

        # ── Task 4: MCP Notifications ─────────────────────────────────────────
        # Sends one notification email per duplicate group.
        # Dry-run (default): writes .eml drafts to output/replies/ — safe to run anytime.
        # Live (--send-live): Claude reads the notification and calls the Gmail
        #   MCP send_email tool to actually deliver each email.
        logger.info("Starting Task 4: Notification Emails (send_live=%s)", args.send_live)
        notif_stats = notifier.run(conn, send_live=args.send_live)
        print_notif_stats(notif_stats, send_live=args.send_live)

    except Exception as exc:
        # Top-level catch: something truly unexpected happened.
        # Log it and exit with a non-zero code so CI/scripts detect the failure.
        logger.error("Pipeline aborted with unrecoverable error: %s", exc, exc_info=True)
        return 1

    finally:
        # Always close the DB connection, even if an exception occurred.
        conn.close()
        logger.info("Database connection closed.")

    logger.info("Pipeline finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
