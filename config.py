"""
config.py
─────────
Central configuration for the Enron email pipeline.
All tunable constants live here so that main.py and pipeline modules
import from a single source of truth.

What goes here:
  - MAILDIR_ROOT: absolute or relative path to the enron maildir folder
  - SELECTED_MAILBOXES: list of mailbox folder names to process
  - DATABASE_PATH: path to the SQLite database file
  - OUTPUT_DIR / REPLIES_DIR: where generated .eml drafts are written
  - LOG paths for error_log.txt, send_log.csv, duplicates_report.csv
  - DUPLICATE_SIMILARITY_THRESHOLD: minimum rapidfuzz ratio (default 90)
  - MCP server name / tool name constants for Task 4
  - Any other pipeline-wide settings (batch size, log level, etc.)
"""

import os

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MAILDIR_ROOT = os.path.join(BASE_DIR, "enron_mail_20150507", "maildir")

DATABASE_PATH = os.path.join(BASE_DIR, "enron_emails.db")

OUTPUT_DIR   = os.path.join(BASE_DIR, "output")
REPLIES_DIR  = os.path.join(OUTPUT_DIR, "replies")

ERROR_LOG_PATH       = os.path.join(BASE_DIR, "error_log.txt")
SEND_LOG_PATH        = os.path.join(OUTPUT_DIR, "send_log.csv")
DUPLICATES_REPORT    = os.path.join(BASE_DIR, "duplicates_report.csv")

# ── Mailbox selection (Task 1) ────────────────────────────────────────────────
# Five mailboxes chosen for breadth and executive/functional diversity.
# See README for rationale; roles in brief:
#   kaminski-v  — VP Research (quantitative / risk)
#   dasovich-j  — Government Affairs (regulatory)
#   skilling-j  — CEO (executive decision-making)
#   taylor-m    — Legal (general counsel)
#   haedicke-m  — Legal (managing director / legal)
SELECTED_MAILBOXES = [
    "kaminski-v",
    "dasovich-j",
    "skilling-j",
    "taylor-m",
    "haedicke-m",
]

# ── Duplicate detection (Task 3) ──────────────────────────────────────────────
# Minimum rapidfuzz partial_ratio score (0–100) to consider two bodies similar.
DUPLICATE_SIMILARITY_THRESHOLD = 90

# ── MCP / Gmail (Task 4) ──────────────────────────────────────────────────────
MCP_SERVER_NAME = "gmail"      # As registered in mcp_config.json / .mcp.json
MCP_SEND_TOOL   = "send_email" # Tool name exposed by the GongRzhe Gmail MCP server

# All notification emails are redirected to this address for testing.
# The original Enron senders no longer have active mailboxes, so we send
# to our own Gmail to demonstrate the pipeline end-to-end.
# Set via NOTIFICATION_OVERRIDE_EMAIL env var in .env file.
NOTIFICATION_OVERRIDE_EMAIL = os.getenv("NOTIFICATION_OVERRIDE_EMAIL")

# Maximum number of duplicate groups to notify in one pipeline run.
# Capped at 15 for the demo — avoids sending hundreds of emails.
NOTIFICATION_LIMIT = int(os.getenv("NOTIFICATION_LIMIT", "15"))

# ── Misc ──────────────────────────────────────────────────────────────────────
LOG_LEVEL  = "INFO"    # DEBUG | INFO | WARNING | ERROR
BATCH_SIZE = 500       # Number of rows inserted per DB transaction
