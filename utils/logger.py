"""
utils/logger.py
───────────────
Logging setup for the entire pipeline.

Two output streams:
  1. Console (stdout) — INFO and above. Shows progress as the pipeline runs.
  2. error_log.txt   — WARNING and above. Every parse failure lands here
                       with the file path and reason so failures can be
                       audited after the run without re-scanning stdout.

Call setup() exactly once at the top of main.py before anything else.
Every other module then calls get_logger(__name__) to get a named child
logger that inherits the root configuration.
"""

import logging
import os
from config import ERROR_LOG_PATH, LOG_LEVEL


def setup(level: str = None) -> None:
    """
    Configure the root logger with two handlers:
      - StreamHandler: prints INFO+ to stdout (human-readable progress)
      - FileHandler:   writes WARNING+ to error_log.txt (parse failures)

    Call this once at the start of main.py. Subsequent calls are no-ops
    because we check if handlers are already attached.

    Args:
        level: Log level string ("DEBUG", "INFO", "WARNING", "ERROR").
               Defaults to the value in config.LOG_LEVEL.
    """
    # Use the config default if the caller didn't specify a level
    resolved_level = getattr(logging, (level or LOG_LEVEL).upper(), logging.INFO)

    root = logging.getLogger()

    # Guard: don't add duplicate handlers if setup() is called more than once
    if root.handlers:
        return

    root.setLevel(logging.DEBUG)  # Root captures everything; handlers filter

    # ── Console handler ───────────────────────────────────────────────────────
    # Shows INFO and above so the user sees pipeline progress in real time.
    console_handler = logging.StreamHandler()
    console_handler.setLevel(resolved_level)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(console_handler)

    # ── File handler for error_log.txt ────────────────────────────────────────
    # Captures WARNING and above. This is where every parse failure is written
    # so they can be reviewed after the run.
    # Create parent directories for the log file if they don't exist.
    os.makedirs(os.path.dirname(os.path.abspath(ERROR_LOG_PATH)), exist_ok=True)
    file_handler = logging.FileHandler(ERROR_LOG_PATH, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(
        # Pipe-separated format makes it easy to parse with a spreadsheet or grep:
        # 2001-05-14 09:25:00 | WARNING | maildir/kaminski-v/inbox/1. | Missing message_id
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(file_handler)


def log_parse_error(source_file: str, reason: str) -> None:
    """
    Write one parse-failure entry to error_log.txt.

    This is a convenience wrapper so every caller uses the same format.
    The message format is:  <source_file> | <reason>
    which the FileHandler then prefixes with timestamp and level.

    Args:
        source_file: Relative path to the email file that failed
                     (e.g. "maildir/kaminski-v/inbox/1.").
        reason:      Human-readable description of why parsing failed
                     (e.g. "Missing message_id", "Unparseable date: ...").
    """
    # Use a dedicated "errors" logger so callers can filter by logger name if needed
    logging.getLogger("pipeline.errors").warning("%s | %s", source_file, reason)


def get_logger(name: str) -> logging.Logger:
    """
    Return a named child logger for use within a module.

    Usage (at the top of any pipeline/utils module):
        logger = get_logger(__name__)
        logger.info("Processing %d files", count)

    The child logger inherits the root logger's handlers and level,
    so no additional configuration is needed.

    Args:
        name: Typically __name__ of the calling module.
    """
    return logging.getLogger(name)
