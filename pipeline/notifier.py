"""
pipeline/notifier.py
────────────────────
Task 4 — MCP Email Notifications

For each of the top NOTIFICATION_LIMIT duplicate groups (by number of flagged
emails), this module sends one notification email to the sender of the latest
duplicate, informing them that their message was flagged as a duplicate.

Two modes:
  Dry-run (default):  writes a .eml draft file to output/replies/ for every
                      group. No emails are actually sent. Safe to run anytime.
  Live (--send-live): calls the Anthropic API → Claude reads the notification
                      content → Claude calls the send_email tool on the Gmail
                      MCP server → the email is delivered to NOTIFICATION_OVERRIDE_EMAIL.

Why Claude in the loop?
  The spec requires using a Gmail MCP server via an AI tool. Claude acts as
  the agent: it receives the notification details as a prompt, decides to call
  the send_email MCP tool, and confirms once sent. This is the "AI-assisted
  tooling" pattern the spec is testing.

Outputs:
  output/replies/<message_id>.eml  — one draft per group (dry-run)
  output/send_log.csv              — one row per attempt (live and dry-run)
  DB update: notification_sent=1, notification_date on the latest duplicate row

Public interface:
  run(conn, send_live=False) -> dict
"""

import asyncio
import csv
import os
from datetime import datetime, timezone
from email.utils import formatdate
from pathlib import Path

import anthropic
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from config import (
    NOTIFICATION_OVERRIDE_EMAIL,
    NOTIFICATION_LIMIT,
    SEND_LOG_PATH,
    REPLIES_DIR,
    OUTPUT_DIR,
    MCP_SEND_TOOL,
)
from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Email template (from spec Section 4.2)
# ─────────────────────────────────────────────────────────────────────────────

# The body template exactly matches the spec.
# Placeholders are filled by _build_notification().
_NOTIFICATION_BODY = """\
This is an automated notification from the Email Deduplication System.

Your email has been identified as a potential duplicate:

  Your Email (Flagged):
    Message-ID:  {message_id_latest}
    Date Sent:   {date_latest}
    Subject:     {subject}

  Original Email on Record:
    Message-ID:  {message_id_original}
    Date Sent:   {date_original}

  Similarity Score: {similarity_score}%

If this was NOT a duplicate and you intended to send this email,
please reply with CONFIRM to restore it to active status.

No action is required if this is indeed a duplicate.\
"""


# ─────────────────────────────────────────────────────────────────────────────
# Notification content builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_notification(group: dict) -> dict:
    """
    Build the notification email content for one duplicate group.

    The recipient is NOTIFICATION_OVERRIDE_EMAIL (your Gmail) rather than the
    original Enron sender, because those addresses no longer exist.

    Args:
        group: Dict with original + latest duplicate details from the DB.

    Returns:
        Dict with keys: to, subject, body, references.
    """
    # Redirect to the override address (your Gmail) for testing.
    # In a production system this would be group["latest_from_address"].
    to = NOTIFICATION_OVERRIDE_EMAIL or group["latest_from_address"]

    # Subject follows the spec template exactly.
    subject = f"[Duplicate Notice] Re: {group['original_subject']}"

    # Fill in the body template with the group's details.
    body = _NOTIFICATION_BODY.format(
        message_id_latest=group["latest_message_id"],
        date_latest=group["latest_date"],
        subject=group["original_subject"],
        message_id_original=group["original_message_id"],
        date_original=group["original_date"],
        similarity_score=round(group["similarity_score"], 1),
    )

    return {
        "to":         to,
        "subject":    subject,
        "body":       body,
        # References header links this notification to the duplicate email.
        "references": group["latest_message_id"],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run: write .eml draft files
# ─────────────────────────────────────────────────────────────────────────────

def _write_eml(notification: dict, group: dict, replies_dir: Path) -> Path:
    """
    Write a standard RFC 2822 .eml draft file for one notification.

    .eml files can be opened in any email client (Outlook, Thunderbird, etc.)
    to preview exactly what would be sent in live mode.

    Args:
        notification: Built by _build_notification().
        group:        The duplicate group dict (used for the filename).
        replies_dir:  Directory to write into (output/replies/).

    Returns:
        Path to the written .eml file.
    """
    # Sanitise the message_id to make it safe as a filename.
    # message_ids often contain characters like <, >, / that can't be in filenames.
    safe_id = (
        group["latest_message_id"]
        .replace("<", "").replace(">", "")
        .replace("/", "_").replace("\\", "_")
        .replace(":", "_")[:80]          # cap length to avoid OS path limits
    )
    filepath = replies_dir / f"{safe_id}.eml"

    # Build the .eml content: headers + blank line + body (RFC 2822 format).
    content = (
        f"To: {notification['to']}\n"
        f"Subject: {notification['subject']}\n"
        f"Date: {formatdate()}\n"
        f"References: {notification['references']}\n"
        f"MIME-Version: 1.0\n"
        f"Content-Type: text/plain; charset=utf-8\n"
        f"\n"                            # blank line separates headers from body
        f"{notification['body']}"
    )

    filepath.write_text(content, encoding="utf-8")
    logger.info("Dry-run: wrote draft to %s", filepath.name)
    return filepath


# ─────────────────────────────────────────────────────────────────────────────
# Live mode: Claude + MCP Gmail server
# ─────────────────────────────────────────────────────────────────────────────

async def _send_all_live(
    notifications: list[dict],
    groups: list[dict],
) -> list[dict]:
    """
    Send all notifications via the Anthropic API + Gmail MCP server.

    We open ONE MCP server process (one npx subprocess) for all notifications,
    then loop over each one. This is efficient — no repeated subprocess startup.

    MCP flow for each email:
      1. Anthropic SDK sends a prompt to Claude asking it to call send_email.
      2. Claude responds with a tool_use block (its decision to call send_email).
      3. We execute that tool call on the MCP server (which delivers the email).
      4. We send the tool result back to Claude so it can confirm success.
      5. Claude responds with end_turn (confirmation text).

    Args:
        notifications: List of notification dicts (from _build_notification).
        groups:        Parallel list of group dicts (for logging).

    Returns:
        List of result dicts: {"status": "sent"|"failed", "error": str|None}
    """
    results = []

    # StdioServerParameters tells the MCP client how to launch the server.
    # "npx @gongrzhe/server-gmail-autoauth-mcp" starts the Gmail MCP server
    # as a child process. It reads OAuth token from ~/.gmail-mcp/credentials.json
    # (written during the one-time auth setup).
    server_params = StdioServerParameters(
        command="npx",
        args=["@gongrzhe/server-gmail-autoauth-mcp"],
    )

    # Open the MCP client session. This launches the npx process and
    # performs the MCP handshake (initialize request/response).
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            logger.info("MCP Gmail server initialised and ready.")

            # Ask the MCP server which tools it exposes.
            # This gives us the send_email tool's name, description, and
            # input schema dynamically — no hardcoding of parameter names.
            tools_result = await session.list_tools()
            logger.info(
                "MCP tools available: %s",
                [t.name for t in tools_result.tools],
            )

            # Filter to only the send_email tool.
            # We don't want Claude to accidentally call read/list/delete tools.
            send_tool = next(
                (t for t in tools_result.tools if t.name == MCP_SEND_TOOL),
                None,
            )
            if not send_tool:
                logger.error(
                    "'%s' tool not found on MCP server. Available: %s",
                    MCP_SEND_TOOL,
                    [t.name for t in tools_result.tools],
                )
                # Return all as failed if the tool doesn't exist.
                return [
                    {"status": "failed", "error": f"Tool '{MCP_SEND_TOOL}' not found"}
                    for _ in notifications
                ]

            # Convert the MCP tool schema to the format the Anthropic SDK expects.
            anthropic_tools = [{
                "name":         send_tool.name,
                "description":  send_tool.description or "Send an email via Gmail.",
                "input_schema": send_tool.inputSchema,
            }]

            # Create the Anthropic client.
            # It reads ANTHROPIC_API_KEY from the environment (loaded from .env).
            client = anthropic.Anthropic()

            # Send each notification one at a time using the shared MCP session.
            for i, (notification, group) in enumerate(zip(notifications, groups), 1):
                logger.info(
                    "Sending notification %d/%d | group: '%s' | to: %s",
                    i, len(notifications),
                    group["original_subject"][:40],
                    notification["to"],
                )
                result = await _send_one(session, client, anthropic_tools, notification)
                results.append(result)

    return results


async def _send_one(
    session: ClientSession,
    client: anthropic.Anthropic,
    anthropic_tools: list[dict],
    notification: dict,
) -> dict:
    """
    Send a single notification email via the Claude agentic loop + MCP.

    The "agentic loop":
      - We send a prompt to Claude describing what to send.
      - Claude calls send_email (tool_use response).
      - We execute the tool call on the MCP server.
      - We return the result to Claude.
      - Claude confirms (end_turn response).
      - Done.

    This prompt is also the "example prompt" documented in AI_USAGE.md.

    Args:
        session:         Active MCP client session (shared across all emails).
        client:          Anthropic SDK client.
        anthropic_tools: send_email tool schema in Anthropic format.
        notification:    Built by _build_notification().

    Returns:
        {"status": "sent"|"failed", "error": str|None}
    """
    # ── Build the prompt ──────────────────────────────────────────────────────
    # This is the prompt sent to Claude. It tells Claude exactly what email
    # to send and instructs it to use the send_email tool.
    # The tool schema (discovered dynamically above) tells Claude what
    # parameters send_email expects (to, subject, body, etc.).
    prompt = (
        f"Send a notification email using the {MCP_SEND_TOOL} tool "
        f"with the following details:\n\n"
        f"To: {notification['to']}\n"
        f"Subject: {notification['subject']}\n\n"
        f"Body:\n{notification['body']}\n\n"
        f"Use the {MCP_SEND_TOOL} tool to send this email now."
    )

    # Start the conversation with just the user prompt.
    messages = [{"role": "user", "content": prompt}]

    try:
        # ── Agentic loop ──────────────────────────────────────────────────────
        # We loop because Claude might need more than one round-trip
        # (e.g. if a tool result needs further processing before end_turn).
        # In practice for send_email this is always exactly 2 turns.
        while True:
            response = client.messages.create(
                model="claude-opus-4-6",
                max_tokens=1024,
                tools=anthropic_tools,
                messages=messages,
            )

            # ── Claude is done ────────────────────────────────────────────────
            if response.stop_reason == "end_turn":
                # Extract Claude's confirmation text (e.g. "Email sent successfully.")
                confirmation = next(
                    (b.text for b in response.content if hasattr(b, "text")),
                    "Email sent.",
                )
                logger.info("Claude confirmed: %s", confirmation[:120])
                return {"status": "sent", "error": None}

            # ── Claude wants to call a tool ───────────────────────────────────
            if response.stop_reason == "tool_use":
                tool_results = []

                for block in response.content:
                    if block.type == "tool_use":
                        logger.info(
                            "Claude calling '%s' with args: %s",
                            block.name, block.input,
                        )
                        # Execute the tool call on the MCP server.
                        # The MCP server authenticates with Gmail via the stored
                        # OAuth token and delivers the email.
                        mcp_result = await session.call_tool(block.name, block.input)

                        # Package the tool result to send back to Claude.
                        tool_results.append({
                            "type":        "tool_result",
                            "tool_use_id": block.id,
                            "content":     str(mcp_result.content),
                        })

                # Add Claude's tool_use response and our tool results to the
                # message history so Claude can continue the conversation.
                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user",      "content": tool_results})

            else:
                # Unexpected stop reason — log and return failure.
                msg = f"Unexpected stop_reason: {response.stop_reason}"
                logger.warning(msg)
                return {"status": "failed", "error": msg}

    except Exception as exc:
        logger.error("Failed to send notification: %s", exc, exc_info=True)
        return {"status": "failed", "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Database update
# ─────────────────────────────────────────────────────────────────────────────

def _update_db(conn, latest_message_id: str) -> None:
    """
    Mark the LATEST DUPLICATE in a group as notified.

    Only the latest duplicate is flagged (not all duplicates in the group)
    because we sent exactly one notification per group targeting that email.
    The notification_date records when the pipeline ran this task.
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """UPDATE emails
              SET notification_sent = 1,
                  notification_date = ?
            WHERE message_id = ?""",
        (now, latest_message_id),
    )


# ─────────────────────────────────────────────────────────────────────────────
# send_log.csv
# ─────────────────────────────────────────────────────────────────────────────

_SEND_LOG_FIELDS = [
    "timestamp",            # ISO-8601 time when this row was written
    "recipient",            # Email address the notification was sent to
    "subject",              # Notification subject line
    "original_message_id",  # message_id of the original (earliest) email
    "latest_message_id",    # message_id of the latest duplicate (trigger)
    "similarity_score",     # Body similarity score (0–100)
    "status",               # "sent" | "dry_run" | "failed"
    "error",                # Error message if failed, else empty string
]


def _append_send_log(send_log_path: Path, row: dict) -> None:
    """
    Append one row to send_log.csv.

    Creates the file with a header row if it doesn't already exist.
    Appending (not overwriting) means multiple pipeline runs accumulate
    a full history of notification attempts.
    """
    file_exists = send_log_path.exists()
    with open(send_log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_SEND_LOG_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(conn, send_live: bool = False) -> dict:
    """
    Run Task 4 — duplicate notification emails.

    Steps:
      1. Ensure output directories exist (output/ and output/replies/).
      2. Query DB: top NOTIFICATION_LIMIT groups by duplicate count.
      3. For each group: fetch original + latest-duplicate details.
      4. Build notification content from the spec template.
      5a. Dry-run: write .eml files to output/replies/.
      5b. Live:    Claude API → MCP send_email → Gmail delivery.
      6. Update DB: notification_sent=1 on the latest duplicate row.
      7. Append result row to output/send_log.csv.
      8. Return stats dict.

    Args:
        conn:      Open sqlite3 connection (schema already applied).
        send_live: True = send via MCP. False = write .eml drafts only.

    Returns:
        Dict: total_sent, total_failed, total_dry_run.
    """
    mode = "LIVE" if send_live else "DRY-RUN"
    logger.info("Task 4 starting in %s mode. Limit: %d groups.", mode, NOTIFICATION_LIMIT)

    # ── Step 1: Ensure output directories exist, clear stale drafts ──────────
    replies_dir = Path(REPLIES_DIR)
    replies_dir.mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # Remove any .eml files left over from a previous run so the folder always
    # reflects only the current run's output. Without this, old drafts
    # accumulate and it becomes impossible to tell which files are current.
    # We only delete .eml files — any other files in the folder are untouched.
    stale = list(replies_dir.glob("*.eml"))
    for f in stale:
        f.unlink()
    if stale:
        logger.info("Step 1: Cleared %d stale .eml file(s) from output/replies/.", len(stale))

    # ── Step 2: Find top N unnotified duplicate groups ────────────────────────
    # Group all flagged duplicates by their original (duplicate_of column).
    # Count duplicates per group, order by most duplicates first.
    # Only pick groups whose latest duplicate hasn't been notified yet.
    rows = conn.execute(
        """
        SELECT
            duplicate_of   AS original_message_id,
            COUNT(*)       AS dup_count
        FROM  emails
        WHERE is_duplicate     = 1
          AND notification_sent = 0
        GROUP BY duplicate_of
        ORDER BY dup_count DESC
        LIMIT ?
        """,
        (NOTIFICATION_LIMIT,),
    ).fetchall()

    if not rows:
        logger.info("No unnotified duplicate groups found. Task 4 already complete.")
        return {"total_sent": 0, "total_failed": 0, "total_dry_run": 0}

    logger.info("Step 2: Found %d groups to notify.", len(rows))

    # ── Step 3: Fetch original + latest duplicate details for each group ──────
    groups = []
    for row in rows:
        original_id = row["original_message_id"]

        # Fetch the original (canonical) email — the earliest in the group.
        original = conn.execute(
            "SELECT message_id, date, subject FROM emails WHERE message_id = ?",
            (original_id,),
        ).fetchone()

        if not original:
            logger.warning("Original email not found: %s — skipping group.", original_id)
            continue

        # Fetch the latest (most recent) duplicate in this group.
        # This is the email whose sender receives the notification.
        # ORDER BY date DESC LIMIT 1 gives us the most recent duplicate.
        latest_dup = conn.execute(
            """
            SELECT message_id, date, from_address, similarity_score
            FROM   emails
            WHERE  duplicate_of    = ?
              AND  is_duplicate    = 1
              AND  notification_sent = 0
            ORDER BY date DESC
            LIMIT 1
            """,
            (original_id,),
        ).fetchone()

        if not latest_dup:
            continue

        groups.append({
            "original_message_id":  original["message_id"],
            "original_date":        original["date"] or "",
            "original_subject":     original["subject"] or "(no subject)",
            "latest_message_id":    latest_dup["message_id"],
            "latest_date":          latest_dup["date"] or "",
            "latest_from_address":  latest_dup["from_address"],
            "similarity_score":     latest_dup["similarity_score"] or 0.0,
            "dup_count":            row["dup_count"],
        })

    if not groups:
        logger.info("No groups with fetchable details. Task 4 complete.")
        return {"total_sent": 0, "total_failed": 0, "total_dry_run": 0}

    # ── Step 4: Build notification content ────────────────────────────────────
    # NOTIFICATION_OVERRIDE_EMAIL redirects all emails to your Gmail inbox.
    notifications = [_build_notification(g) for g in groups]

    # ── Step 5: Send or write drafts ──────────────────────────────────────────
    if send_live:
        logger.info(
            "Live mode: sending %d emails via Claude + MCP...", len(notifications)
        )
        # asyncio.run() bridges the synchronous pipeline with the async MCP client.
        # The entire MCP session (one npx subprocess) stays open for all emails.
        results = asyncio.run(_send_all_live(notifications, groups))
    else:
        logger.info(
            "Dry-run mode: writing %d .eml files to output/replies/", len(notifications)
        )
        results = []
        for notification, group in zip(notifications, groups):
            _write_eml(notification, group, replies_dir)
            results.append({"status": "dry_run", "error": None})

    # ── Steps 6 & 7: Update DB + write send_log.csv ───────────────────────────
    timestamp = datetime.now(timezone.utc).isoformat()
    total_sent = total_failed = total_dry_run = 0

    for group, notification, result in zip(groups, notifications, results):
        status = result["status"]

        if status in ("sent", "dry_run"):
            # Mark only the latest duplicate as notified in the DB.
            _update_db(conn, group["latest_message_id"])
            if status == "sent":
                total_sent += 1
            else:
                total_dry_run += 1
        else:
            total_failed += 1

        # Always log every attempt — successful, dry-run, or failed.
        _append_send_log(
            Path(SEND_LOG_PATH),
            {
                "timestamp":           timestamp,
                "recipient":           notification["to"],
                "subject":             notification["subject"],
                "original_message_id": group["original_message_id"],
                "latest_message_id":   group["latest_message_id"],
                "similarity_score":    round(group["similarity_score"], 2),
                "status":              status,
                "error":               result.get("error") or "",
            },
        )

    conn.commit()

    logger.info(
        "Task 4 complete: %d sent, %d dry_run, %d failed.",
        total_sent, total_dry_run, total_failed,
    )

    return {
        "total_sent":    total_sent,
        "total_failed":  total_failed,
        "total_dry_run": total_dry_run,
    }
