-- schema.sql
-- Enron Email Pipeline — SQLite database schema
-- Applied automatically by pipeline/storage.py on first run.
-- Can also be applied manually: sqlite3 enron_emails.db < schema.sql
--
-- Design decisions:
--   • to/cc/bcc addresses live in a separate email_addresses table (normalised).
--     No comma-joined strings in the emails table.
--   • UNIQUE constraint on message_id prevents duplicate inserts (INSERT OR IGNORE).
--   • Indexes on date, from_address, subject support Task 2 sample queries.
--   • is_duplicate / duplicate_of / similarity_score support Task 3 dedup logic.
--   • notification_sent / notification_date support Task 4 MCP send tracking.
--   • date stored as ISO-8601 TEXT (e.g. "2001-05-14T09:25:00+00:00").
--     SQLite has no native DATETIME; TEXT with ISO-8601 sorts correctly.
-- ─────────────────────────────────────────────────────────────────────────────

-- Main emails table.
-- One row per unique email (keyed on message_id).
CREATE TABLE IF NOT EXISTS emails (
    -- ── Mandatory fields (spec Section 3.1) ──────────────────────────────────
    message_id          TEXT    PRIMARY KEY,        -- RFC 2822 Message-ID header; UNIQUE enforced by PK
    date                TEXT,                       -- Parsed, normalised to UTC, stored as ISO-8601 string
    from_address        TEXT    NOT NULL,           -- Sender email address (bare address, no display name)
    subject             TEXT,                       -- Full subject line; Re:/Fwd: prefixes preserved
    body                TEXT,                       -- Primary body text (forwarded/quoted content removed)
    source_file         TEXT    NOT NULL,           -- Relative path to raw file, e.g. maildir/kaminski-v/inbox/1.

    -- ── Optional fields (spec Section 3.2) ───────────────────────────────────
    x_from              TEXT,                       -- Display name from X-From header (Enron-specific)
    x_to                TEXT,                       -- Display name(s) from X-To header
    x_cc                TEXT,                       -- Display name(s) from X-cc header
    x_bcc               TEXT,                       -- Display name(s) from X-bcc header
    x_folder            TEXT,                       -- X-Folder header (mailbox folder path)
    x_origin            TEXT,                       -- X-Origin header (originating mailbox name)
    content_type        TEXT,                       -- MIME Content-Type if present
    has_attachment      INTEGER DEFAULT 0,          -- 1 if attachment inferred; 0 otherwise (SQLite has no BOOLEAN)
    forwarded_content   TEXT,                       -- Forwarded email block separated from primary body
    quoted_content      TEXT,                       -- Quoted reply lines (>-prefixed) separated from primary body
    headings            TEXT,                       -- Extracted headings from body, newline-separated

    -- ── Duplicate detection fields (Task 3) ──────────────────────────────────
    is_duplicate        INTEGER DEFAULT 0,          -- 1 if flagged as duplicate; 0 otherwise
    duplicate_of        TEXT    REFERENCES emails(message_id),  -- message_id of the earliest (original) email in the group
    similarity_score    REAL,                       -- rapidfuzz similarity score (0.0–100.0) against the original

    -- ── Notification tracking fields (Task 4) ────────────────────────────────
    notification_sent   INTEGER DEFAULT 0,          -- 1 if notification email was sent via MCP; 0 otherwise
    notification_date   TEXT                        -- ISO-8601 timestamp of when notification was sent
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Normalised address table.
-- Stores all To, CC, and BCC addresses as individual rows.
-- A single email with 10 recipients = 10 rows here.
-- Querying "all CC'd emails" = SELECT WHERE field = 'cc' (indexed).
CREATE TABLE IF NOT EXISTS email_addresses (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id  TEXT    NOT NULL REFERENCES emails(message_id) ON DELETE CASCADE,
    field       TEXT    NOT NULL CHECK(field IN ('to', 'cc', 'bcc')),   -- which header this address came from
    address     TEXT    NOT NULL                                         -- bare email address, lowercased
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Indexes
-- All created with IF NOT EXISTS so re-running schema.sql is safe.

-- emails table indexes
CREATE INDEX IF NOT EXISTS idx_emails_date          ON emails(date);            -- range queries on date
CREATE INDEX IF NOT EXISTS idx_emails_from          ON emails(from_address);    -- group-by sender queries
CREATE INDEX IF NOT EXISTS idx_emails_subject       ON emails(subject);         -- subject search / dedup
CREATE INDEX IF NOT EXISTS idx_emails_is_duplicate  ON emails(is_duplicate);    -- Task 3: fast duplicate scan
CREATE INDEX IF NOT EXISTS idx_emails_notif_sent    ON emails(notification_sent); -- Task 4: find unsent notifications

-- email_addresses table indexes
CREATE INDEX IF NOT EXISTS idx_addr_message_id      ON email_addresses(message_id);    -- JOIN from emails
CREATE INDEX IF NOT EXISTS idx_addr_field           ON email_addresses(field);          -- filter by to/cc/bcc
CREATE INDEX IF NOT EXISTS idx_addr_address         ON email_addresses(address);        -- lookup by address
