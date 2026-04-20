"""
pipeline/storage.py
───────────────────
Task 2 — Database Storage (connection, schema, and insert layer).

This module owns everything that touches the SQLite database:
  - Opening the connection with sensible settings.
  - Applying schema.sql so the DB is created/updated on first run.
  - Inserting parsed email records (one row per email + address rows).
  - A verify() helper that counts rows for a quick sanity check.

It does NOT commit transactions — extractor.py calls conn.commit()
every BATCH_SIZE emails so we get bulk performance without holding an
enormous transaction open for 100k rows.

Public interface:
    get_connection()  -> sqlite3.Connection
    init_schema(conn) -> None
    insert_email(conn, record) -> bool      True = inserted, False = skipped (dupe)
    insert_addresses(conn, message_id, field, addresses) -> None
    verify(conn)      -> dict[str, int]
"""

import os
import sqlite3
from pathlib import Path

from config import DATABASE_PATH, BATCH_SIZE
from utils.logger import get_logger

logger = get_logger(__name__)

# Path to schema.sql relative to this file's location
_SCHEMA_PATH = Path(__file__).parent.parent / "schema.sql"


def get_connection() -> sqlite3.Connection:
    """
    Open (or create) the SQLite database at config.DATABASE_PATH.

    Settings applied:
      - WAL journal mode: allows concurrent reads during writes.
        Important when the pipeline is running and we want to query
        the DB in a separate terminal without locking.
      - row_factory = sqlite3.Row: lets callers access columns by name
        (e.g. row["from_address"]) instead of index, which is safer.
      - foreign_keys = ON: enforces the REFERENCES constraint between
        email_addresses.message_id and emails.message_id.

    Returns:
        An open sqlite3.Connection. Caller is responsible for closing it.
    """
    os.makedirs(os.path.dirname(os.path.abspath(DATABASE_PATH)), exist_ok=True)

    conn = sqlite3.connect(DATABASE_PATH)

    # WAL mode is significantly faster for write-heavy workloads
    conn.execute("PRAGMA journal_mode=WAL")
    # Enforce FK constraints (SQLite disables them by default)
    conn.execute("PRAGMA foreign_keys=ON")
    # Named column access: row["column_name"] instead of row[0]
    conn.row_factory = sqlite3.Row

    logger.info("Opened database at %s", DATABASE_PATH)
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    """
    Apply schema.sql to the open connection.

    All CREATE TABLE and CREATE INDEX statements use IF NOT EXISTS so this
    is safe to call on an existing database — it won't drop or alter any
    data. On a fresh database it creates all tables and indexes from scratch.

    Args:
        conn: Open sqlite3 connection (from get_connection()).
    """
    schema_sql = _SCHEMA_PATH.read_text(encoding="utf-8")

    # executescript commits any pending transaction and runs the SQL as a script.
    # This is the correct way to run multi-statement DDL in sqlite3.
    conn.executescript(schema_sql)

    logger.info("Schema applied from %s", _SCHEMA_PATH)


def insert_email(conn: sqlite3.Connection, record: dict) -> bool:
    """
    Insert one parsed email record into the emails table.

    Uses INSERT OR IGNORE so that if the message_id already exists in the
    DB (exact duplicate header), the row is silently skipped rather than
    raising an IntegrityError. This handles the case where the pipeline
    is re-run on an already-populated database.

    Address lists (to_addresses, cc_addresses, bcc_addresses) are NOT
    inserted here — the caller (extractor._process_file) calls
    insert_addresses() separately for each list.

    Args:
        conn:   Open sqlite3 connection.
        record: Dict returned by email_parser.parse(), with source_file set.

    Returns:
        True if the row was inserted, False if it was skipped (duplicate).
    """
    # These list fields belong in the email_addresses table, not emails.
    # We pop them out rather than leaving them in the dict to avoid
    # "table emails has no column named to_addresses" errors.
    # We do NOT mutate the caller's dict — work on a copy.
    r = dict(record)
    r.pop("to_addresses",  None)
    r.pop("cc_addresses",  None)
    r.pop("bcc_addresses", None)

    # Convert bool to int for SQLite (it has no native BOOLEAN type)
    r["has_attachment"] = int(r.get("has_attachment", False))

    sql = """
        INSERT OR IGNORE INTO emails (
            message_id, date, from_address, subject, body, source_file,
            x_from, x_to, x_cc, x_bcc, x_folder, x_origin,
            content_type, has_attachment,
            forwarded_content, quoted_content, headings
        ) VALUES (
            :message_id, :date, :from_address, :subject, :body, :source_file,
            :x_from, :x_to, :x_cc, :x_bcc, :x_folder, :x_origin,
            :content_type, :has_attachment,
            :forwarded_content, :quoted_content, :headings
        )
    """
    cursor = conn.execute(sql, r)

    # rowcount = 0 means INSERT OR IGNORE skipped the row (duplicate message_id)
    inserted = cursor.rowcount == 1
    if not inserted:
        logger.debug("Skipped duplicate message_id: %s", r.get("message_id"))

    return inserted


def insert_addresses(
    conn: sqlite3.Connection,
    message_id: str,
    field: str,
    addresses: list[str],
) -> None:
    """
    Insert all addresses for one field (to/cc/bcc) of one email.

    Uses executemany for efficiency — one DB round-trip per field
    rather than one per address. On 100k emails with an average of
    5 recipients each, this saves ~500k individual INSERT calls.

    Args:
        conn:       Open sqlite3 connection.
        message_id: The message_id of the parent email (FK to emails table).
        field:      One of "to", "cc", or "bcc".
        addresses:  List of bare email address strings (from email_parser).
    """
    if not addresses:
        return  # Nothing to insert; executemany([]) is a no-op but skip the call

    rows = [(message_id, field, addr) for addr in addresses]
    conn.executemany(
        "INSERT INTO email_addresses (message_id, field, address) VALUES (?, ?, ?)",
        rows,
    )


def verify(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Count rows in each table and return a summary dict.

    Used by extractor.run() after the full pipeline completes to print
    a quick sanity check: "emails: 99,843 rows / email_addresses: 312,441 rows".

    Returns:
        {"emails": <count>, "email_addresses": <count>}
    """
    counts = {}
    for table in ("emails", "email_addresses"):
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        counts[table] = row[0]
        logger.info("Table %s: %d rows", table, row[0])
    return counts
