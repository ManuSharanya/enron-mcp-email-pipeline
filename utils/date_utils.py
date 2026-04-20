"""
utils/date_utils.py
───────────────────
Date/time parsing and normalisation for Enron email Date headers.

Enron emails span 1998–2002 and contain a wide variety of date formats
that we confirmed by sampling the actual data:

  Format                          Example
  ──────────────────────────────────────────────────────────────────────
  RFC 2822 standard               Fri, 1 Dec 2000 13:47:05 -0800
  RFC 2822 + TZ label             Fri, 1 Dec 2000 03:40:00 -0800 (PST)
  Date only, no time              Friday, January 26, 2001
  Slash format                    12/01/2000 1:47 PM
  Y2K artefact (year=100)         3/8/100 5:20 PM   → treated as 2000
  Corrupted year                  Wed, 21 Dec 0001 22:30:55 -0800
  No year at all                  Monday, August 21
  Completely non-parseable        Thursday (weekly call)

Strategy:
  1. Strip parenthetical TZ comment: "(PST)" → avoids parser confusion.
  2. Try email.utils.parsedate_to_datetime (fast, handles RFC 2822).
  3. Fallback to dateutil.parser.parse with a manual TZ abbreviation map.
  4. Sanity check: year must be in [1990, 2010] — Enron operated 1985–2002;
     outside that window means a corrupted date (e.g. year 0001 or 100).
  5. Convert to UTC. Return None on any failure — extractor treats this
     as a mandatory field miss and logs it.
"""

import re
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from dateutil import parser as dateutil_parser

from utils.logger import get_logger

logger = get_logger(__name__)

# ── Timezone abbreviation → UTC offset (hours) ────────────────────────────────
# Covers every TZ abbreviation seen in Enron emails.
# dateutil needs timezone objects, not raw hour integers, so we build both.
_TZ_ABBREV_HOURS: dict[str, int] = {
    "PST": -8,  "PDT": -7,   # US Pacific
    "MST": -7,  "MDT": -6,   # US Mountain
    "CST": -6,  "CDT": -5,   # US Central
    "EST": -5,  "EDT": -4,   # US Eastern
    "UTC":  0,  "GMT":  0,   # Universal
    "CET":  1,  "CEST": 2,   # Central European (some international Enron offices)
    "IST":  5,               # India (Enron had Dabhol plant)
    "JST":  9,               # Japan
}

# Map from abbreviation → timezone object, used by dateutil's tzinfos parameter
_TZINFOS: dict[str, timezone] = {
    abbr: timezone(timedelta(hours=h))
    for abbr, h in _TZ_ABBREV_HOURS.items()
}

# ── Sanity bounds for Enron email dates ───────────────────────────────────────
# Any parsed year outside this range is treated as corrupted and returns None.
_YEAR_MIN = 1990
_YEAR_MAX = 2010

# Regex to strip trailing parenthetical TZ annotations like "(PST)" or "(PDT)"
# These appear in RFC 2822 format and confuse some parsers.
_TZ_COMMENT_RE = re.compile(r'\s*\([A-Z]{2,5}\)\s*$')


def _strip_tz_comment(raw: str) -> str:
    """
    Remove a trailing parenthetical timezone label from a date string.

    Example:
        "Fri, 1 Dec 2000 03:40:00 -0800 (PST)"
        →  "Fri, 1 Dec 2000 03:40:00 -0800"

    The numeric offset (-0800) is the authoritative TZ info; the label
    in parentheses is redundant and can trip up some parsers.
    """
    return _TZ_COMMENT_RE.sub("", raw).strip()


def _is_year_sane(dt: datetime) -> bool:
    """
    Return True if the parsed year is within the expected Enron range.

    This catches:
      - year=0001  (Concur expense system bug seen in kean-s)
      - year=100   (Y2K artefact: 3/8/100 → year 100, not 2000)
    dateutil may "succeed" on these but the result is garbage.
    """
    return _YEAR_MIN <= dt.year <= _YEAR_MAX


def normalise_date(raw: str | None) -> datetime | None:
    """
    Parse a raw email Date header string and return a UTC-aware datetime.

    Returns None (never raises) if the string cannot be reliably parsed,
    allowing the caller to decide whether to treat it as a mandatory
    field failure.

    Parsing attempts in order:
      1. email.utils.parsedate_to_datetime  — handles RFC 2822 cleanly.
      2. dateutil.parser.parse with our TZ abbreviation map  — handles
         non-standard formats like "Friday, January 26, 2001".
      3. Both fallbacks fail → return None.

    In all successful cases the result is converted to UTC before returning.

    Args:
        raw: The raw string value of the Date header, or None.

    Returns:
        A UTC-aware datetime object, or None on failure.
    """
    if not raw or not raw.strip():
        return None

    # Remove the parenthetical TZ comment so parsers don't choke on it
    cleaned = _strip_tz_comment(raw.strip())

    # ── Attempt 1: stdlib RFC 2822 parser ─────────────────────────────────────
    # parsedate_to_datetime handles the standard Enron date format correctly
    # and is faster than dateutil for the common case.
    try:
        dt = parsedate_to_datetime(cleaned)
        if _is_year_sane(dt):
            return dt.astimezone(timezone.utc)
        # Year is outside sane range — don't trust this parse
    except Exception:
        pass  # Fall through to dateutil

    # ── Attempt 2: dateutil with TZ abbreviation map ──────────────────────────
    # Handles formats like "Friday, January 26, 2001", "12/01/2000 1:47 PM",
    # and dates that use TZ abbreviations without numeric offsets.
    try:
        dt = dateutil_parser.parse(
            cleaned,
            tzinfos=_TZINFOS,   # Supply our TZ map so "PST" is recognised
            dayfirst=False,      # Enron is US-based; month comes before day
        )
        if _is_year_sane(dt):
            # If dateutil produced a naive datetime (no TZ info), assume PST
            # (UTC-8) because Enron's Houston HQ ran on CST/CDT but most
            # email servers were configured for PST. Naive is better than wrong.
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone(timedelta(hours=-8)))
            return dt.astimezone(timezone.utc)
        # Year out of range — fall through to failure
    except Exception:
        pass

    # ── All attempts failed ───────────────────────────────────────────────────
    # Caller (extractor.py) will log this as a mandatory field failure.
    logger.debug("Could not parse date: %r", raw)
    return None
