"""
utils/email_parser.py
─────────────────────
Parses a single raw Enron email file and returns a flat dict of extracted
fields matching the spec (Sections 3.1 and 3.2).

Called by pipeline/extractor.py once per file. Does NOT write to disk
or the database — it is purely a data-extraction unit.

Public interface:
    parse(filepath) -> dict      Main entry point.
    class ParseError(Exception)  Raised when a mandatory field is absent.

Field extraction overview:
    Mandatory  →  _extract_mandatory()
    Optional   →  _extract_optional()
    Addresses  →  _parse_address_list()  (used by both)
    Body split →  _split_body()
    Headings   →  _extract_headings()
    Date       →  utils.date_utils.normalise_date()

NOTE: source_file is NOT set here. It is set by pipeline/extractor.py
before calling parse(), because the parser only sees file contents,
not its own path.
"""

import re
import sys
import chardet
from email.parser import BytesParser
from email.utils import getaddresses, parseaddr
from email.header import decode_header, make_header
from pathlib import Path

from utils.date_utils import normalise_date
from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Custom exception
# ─────────────────────────────────────────────────────────────────────────────

class ParseError(Exception):
    """
    Raised when a mandatory field (message_id, date, from_address, subject,
    or body) cannot be extracted from an email file.

    Caught by extractor._process_file(), which logs it and skips the file.
    """
    pass


# ─────────────────────────────────────────────────────────────────────────────
# Regex patterns — compiled once at module load for performance
# ─────────────────────────────────────────────────────────────────────────────

# Detects the start of a forwarded email block.
# Covers the two formats we confirmed in the actual Enron data:
#   1. Lotus Notes:  "---------------------- Forwarded by Vince J Kaminski ..."
#   2. Outlook:      "-----Original Message-----"
# Plus the Apple Mail style as a safety net.
_FORWARDED_RE = re.compile(
    r'(-{3,}\s*Forwarded by\s'          # Lotus Notes / Enron internal
    r'|-{5,}\s*Original Message\s*-{5,}'  # Outlook / Outlook Express
    r'|Begin forwarded message:)',        # Apple Mail
    re.IGNORECASE
)

# Detects a quoted reply line — one or more ">" characters at the start.
# Matches:  "> text", ">> text", ">text" (no space)
_QUOTED_LINE_RE = re.compile(r'^\s*>+', re.MULTILINE)

# Attachment inference: body text clues.
# Matches "attached", "attachment", "attachments" as whole words (case-insensitive).
_ATTACHMENT_BODY_RE = re.compile(r'\battach(?:ed|ment|ments?)?\b', re.IGNORECASE)

# Heading detection patterns (plain-text emails have no HTML headings):
#   - Lines that are ALL CAPS, ≥ 4 chars, not just dashes/equals/stars
#   - Short lines (≤ 80 chars) that end with a colon (section labels)
#   - Numbered sections like "1. TITLE" or "1) Title"
_HEADING_ALLCAPS_RE  = re.compile(r'^[A-Z][A-Z\s\d\-_/]{3,}$')
_HEADING_COLON_RE    = re.compile(r'^[A-Z][^\n]{0,78}:$')
_HEADING_NUMBERED_RE = re.compile(r'^\s*\d+[\.\)]\s+[A-Z]')


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _win32_safe_path(filepath: Path) -> str:
    """
    Return a path string that can be opened on Windows even when the filename
    ends with a dot (e.g. "1.", "10.").

    Windows Win32 API silently strips trailing dots from path components, so
    open("inbox/1.", "rb") silently becomes open("inbox/1", "rb") -> FileNotFoundError.
    The extended-length path prefix (written as two backslashes + ? + backslash)
    tells the kernel to skip normalisation, preserving trailing dots.

    On non-Windows platforms the path is returned unchanged.
    """
    if sys.platform != "win32":
        return str(filepath)

    # Build the prefix using chr() to avoid escape-sequence confusion:
    #   chr(92) = backslash,  chr(63) = question mark
    # Result: \\?\C:\Users\...\inbox\1.
    prefix = chr(92) * 2 + chr(63) + chr(92)
    # Normalise to backslashes (Windows) and make absolute
    abs_str = str(filepath.resolve()) if filepath.is_absolute() else str(Path.cwd() / filepath)
    return prefix + abs_str.replace("/", chr(92))


def _read_raw(filepath: Path) -> bytes:
    """
    Read the email file as raw bytes.

    We read bytes (not text) because:
      a) We need to pass bytes to BytesParser so the stdlib can handle
         Content-Transfer-Encoding (base64, quoted-printable) itself.
      b) We need the raw bytes for chardet encoding detection when the
         body decode step fails.

    On Windows, uses the extended-length path prefix to handle files whose
    names end with a dot (e.g. "1.", "10.") -- a quirk of the Enron dataset
    that Win32 path normalisation would otherwise silently break.

    Raises:
        OSError: If the file cannot be read (propagated to extractor).
    """
    safe_path = _win32_safe_path(filepath)
    with open(safe_path, "rb") as f:
        return f.read()


def _decode_header_value(raw: str | None) -> str:
    """
    Decode a potentially RFC 2047-encoded header value.

    Headers can contain encoded words like =?utf-8?Q?Hello_World?= for
    international characters. Python's make_header(decode_header(...))
    chain handles all encoding schemes (Q, B) and charsets.

    Returns an empty string if raw is None or decoding fails, so callers
    don't need to guard against None.
    """
    if not raw:
        return ""
    try:
        # decode_header returns a list of (bytes_or_str, charset) tuples.
        # make_header joins them into a single Header object we can stringify.
        return str(make_header(decode_header(raw)))
    except Exception:
        # Fall back to the raw value if decoding fails
        return raw


def _parse_address_list(raw: str | None) -> list[str]:
    """
    Parse a raw address header value into a list of bare email addresses.

    Handles all the formats we observed in the Enron data:
      - Simple:           "jeff.dasovich@enron.com"
      - With display name:"Jeff Dasovich <jeff.dasovich@enron.com>"
      - Multi-line:       "addr1@e.com,\n\taddr2@e.com"
      - Exchange DN:      "</O=ENRON/OU=NA/CN=RECIPIENTS/CN=JDASOVIC>"
                          (no @ symbol — filtered out)
      - Mixed:            real addresses and Exchange DNs in same header

    The Exchange DN format appears in X-To/X-Cc headers, not bare To/Cc.
    For bare To/Cc the From header usually has a real email address.
    We filter out any "address" that doesn't contain "@" because it's
    either an Exchange DN or garbage.

    Returns:
        Deduplicated list of lowercase email addresses, preserving order.
        Empty list if raw is None, empty, or no valid addresses found.
    """
    if not raw or not raw.strip():
        return []

    # getaddresses handles comma-separated lists, quoted display names,
    # and line continuations. It returns [(display_name, addr), ...].
    parsed = getaddresses([raw])

    seen = {}  # Use dict to deduplicate while preserving insertion order
    for _name, addr in parsed:
        addr = addr.strip().lower()
        # Skip empty strings and Exchange Distinguished Names (no "@")
        if addr and "@" in addr and addr not in seen:
            seen[addr] = True

    return list(seen.keys())


def _get_body_text(msg) -> str:
    """
    Extract the plain-text body from a parsed email message object.

    Handles both simple (non-multipart) and multipart (MIME) messages.
    For multipart, we walk the parts and take the first text/plain section
    that isn't a named attachment (no Content-Disposition: attachment).

    The get_payload(decode=True) call handles Content-Transfer-Encoding
    (base64, quoted-printable, 7bit) automatically, returning raw bytes.
    We then decode those bytes to a string using the declared charset,
    with chardet as a fallback and latin-1 as the last resort (latin-1
    never raises UnicodeDecodeError — it covers the full 0x00–0xFF range).

    Returns:
        The decoded body text as a string, or "" if no body found.
    """
    def _decode_payload(payload_bytes: bytes, charset: str | None) -> str:
        """Decode payload bytes to string, with chardet fallback."""
        if not payload_bytes:
            return ""
        # Try the declared charset first
        for enc in [charset, "utf-8", None]:
            if enc is None:
                break  # Skip to chardet
            try:
                return payload_bytes.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        # chardet sniffs the encoding from the bytes themselves
        detected_enc = chardet.detect(payload_bytes).get("encoding") or "latin-1"
        try:
            return payload_bytes.decode(detected_enc)
        except (UnicodeDecodeError, LookupError):
            # latin-1 is the absolute last resort — covers all byte values
            return payload_bytes.decode("latin-1", errors="replace")

    if msg.is_multipart():
        # Walk all MIME parts and find the first suitable text/plain section
        for part in msg.walk():
            # Skip parts that are explicitly named attachments
            if part.get_content_disposition() == "attachment":
                continue
            if part.get_content_type() == "text/plain":
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset()
                return _decode_payload(payload, charset)
        return ""  # No text/plain part found in a multipart message
    else:
        # Simple non-multipart message
        payload = msg.get_payload(decode=True)
        if payload is None:
            # get_payload(decode=True) returns None when there's no encoding
            # applied. Fall back to get_payload() which returns the raw string.
            raw = msg.get_payload()
            return raw if isinstance(raw, str) else ""
        charset = msg.get_content_charset()
        return _decode_payload(payload, charset)


def _split_body(raw_body: str) -> tuple[str, str, str]:
    """
    Split the raw body text into three parts:
      1. primary_body       — the author's own words
      2. forwarded_content  — everything after a "Forwarded by" / "Original Message" marker
      3. quoted_content     — lines starting with ">" (inline quoted replies)

    Algorithm:
      Step 1: Find the first forwarded block marker in the body.
              Everything before it = primary + possibly quoted lines.
              Everything from the marker onwards = forwarded_content.
      Step 2: Within the pre-marker section, separate lines that start
              with ">" (quoted_content) from the rest (primary_body).

    We confirmed both forwarded formats in the Enron data:
      - "--- Forwarded by Vince J Kaminski/HOU/ECT on ..."  (Lotus Notes)
      - "-----Original Message-----"                          (Outlook)

    Returns:
        (primary_body, forwarded_content, quoted_content)
        All three are stripped strings; empty string if the section is absent.
    """
    if not raw_body:
        return ("", "", "")

    # ── Step 1: Separate forwarded block ─────────────────────────────────────
    match = _FORWARDED_RE.search(raw_body)
    if match:
        pre_forward   = raw_body[:match.start()]
        forwarded_content = raw_body[match.start():].strip()
    else:
        pre_forward   = raw_body
        forwarded_content = ""

    # ── Step 2: Separate quoted lines from primary body ───────────────────────
    primary_lines = []
    quoted_lines  = []

    for line in pre_forward.splitlines():
        if _QUOTED_LINE_RE.match(line):
            # Remove the leading ">" markers so we store the clean text
            clean_quoted = re.sub(r'^\s*>+\s?', '', line)
            quoted_lines.append(clean_quoted)
        else:
            primary_lines.append(line)

    primary_body   = "\n".join(primary_lines).strip()
    quoted_content = "\n".join(quoted_lines).strip()

    return (primary_body, forwarded_content, quoted_content)


def _extract_headings(body: str) -> str:
    """
    Extract heading-like lines from a plain-text email body.

    Enron emails are plain text — there are no HTML <h1>/<h2> tags.
    We infer headings using three heuristics observed in the data:
      1. ALL-CAPS lines of ≥ 4 characters (not just separator lines of dashes)
      2. Short lines (≤ 80 chars) ending with ":" — typical section labels
      3. Numbered section starters like "1. OVERVIEW" or "1) Policy Scope:"

    Each matched heading is stripped and collected. Duplicates are dropped.

    Returns:
        Matched headings joined by newline, or "" if none found.
    """
    if not body:
        return ""

    headings = []
    seen = set()

    for line in body.splitlines():
        line = line.strip()
        if not line or len(line) < 4:
            continue
        # Skip lines that are purely separator characters (----, ====, ****)
        if re.match(r'^[\-=\*_]{4,}$', line):
            continue

        matched = (
            _HEADING_ALLCAPS_RE.match(line) or
            _HEADING_COLON_RE.match(line)   or
            _HEADING_NUMBERED_RE.match(line)
        )
        if matched and line not in seen:
            headings.append(line)
            seen.add(line)

    return "\n".join(headings)


def _infer_has_attachment(msg, body: str) -> bool:
    """
    Infer whether this email has an attachment.

    Two signals (either is sufficient):
      1. Content-Type header indicates multipart or a binary MIME type.
         (Most Enron emails are text/plain; multipart means something is attached.)
      2. The body text contains attachment-reference keywords.
         We confirmed Enron senders write things like "please find attached"
         and "see attached" in plain-text emails where the attachment was
         stored separately by the mail system.

    Returns:
        True if attachment evidence found, False otherwise.
    """
    content_type = (msg.get("Content-Type") or "").lower()

    # Signal 1: MIME type suggests non-plain content
    mime_signals = ("multipart/", "application/", "image/", "audio/", "video/")
    if any(sig in content_type for sig in mime_signals):
        return True

    # Signal 2: Attachment keywords in the body text
    if body and _ATTACHMENT_BODY_RE.search(body):
        return True

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Mandatory field extractor
# ─────────────────────────────────────────────────────────────────────────────

def _extract_mandatory(msg, source_file: str) -> dict:
    """
    Extract all mandatory fields from the parsed message object.

    Raises ParseError immediately for any field that cannot be extracted,
    because the spec requires all mandatory fields to be present for a
    record to be stored in the database.

    Fields extracted here:
        message_id, date, from_address, to_addresses, subject, body

    NOTE: source_file is passed in as a parameter (not extracted from msg)
    because it is set by the extractor, not derived from email content.

    Args:
        msg:         Parsed email.message.Message object from BytesParser.
        source_file: Relative path string set by extractor.py.

    Returns:
        Dict with all mandatory field values.

    Raises:
        ParseError: If message_id, date, from_address, or body is absent.
    """
    record = {}

    # ── message_id ─────────────────────────────────────────────────────────
    # The Message-ID header is required for the UNIQUE constraint in the DB.
    # Strip angle brackets if present: <abc@xyz> → abc@xyz
    raw_msgid = msg.get("Message-ID")
    if not raw_msgid or not raw_msgid.strip():
        raise ParseError("Missing Message-ID header")
    record["message_id"] = raw_msgid.strip().strip("<>")

    # ── date ───────────────────────────────────────────────────────────────
    # Normalised to UTC ISO-8601 string. Returns None for unparseable dates.
    raw_date = msg.get("Date")
    parsed_dt = normalise_date(raw_date)
    if parsed_dt is None:
        raise ParseError(f"Unparseable or missing date: {raw_date!r}")
    record["date"] = parsed_dt.isoformat()  # e.g. "2001-05-14T09:25:00+00:00"

    # ── from_address ───────────────────────────────────────────────────────
    # parseaddr extracts (display_name, addr) from the From header.
    # We want just the bare email address, lowercased.
    raw_from = _decode_header_value(msg.get("From"))
    _name, from_addr = parseaddr(raw_from)
    from_addr = from_addr.strip().lower()
    if not from_addr or "@" not in from_addr:
        raise ParseError(f"Missing or invalid from_address: {raw_from!r}")
    record["from_address"] = from_addr

    # ── to_addresses ───────────────────────────────────────────────────────
    # Mandatory per spec Section 3.1. If the To: header is absent or yields
    # no valid email addresses, the email is logged as a failed parse.
    # This will catch calendar items and automated system emails with no To:
    # header — those records are skipped and logged in error_log.txt.
    raw_to = _decode_header_value(msg.get("To"))
    to_addresses = _parse_address_list(raw_to)
    if not to_addresses:
        raise ParseError(f"Missing or empty to_addresses (raw To: {raw_to!r})")
    record["to_addresses"] = to_addresses

    # ── subject ────────────────────────────────────────────────────────────
    # Preserve Re:/Fwd: prefixes as required by spec.
    # An empty or absent subject is stored as empty string, not a parse failure.
    raw_subject = _decode_header_value(msg.get("Subject"))
    record["subject"] = raw_subject.strip() if raw_subject else ""

    # ── body ───────────────────────────────────────────────────────────────
    # Extract the decoded plain-text body, then split out forwarded/quoted
    # sections. The "body" field stores only the primary author content.
    raw_body = _get_body_text(msg)
    primary_body, forwarded_content, quoted_content = _split_body(raw_body)

    # Store all three body components in the record
    record["body"]              = primary_body
    record["forwarded_content"] = forwarded_content or None   # NULL in DB if absent
    record["quoted_content"]    = quoted_content or None      # NULL in DB if absent

    # ── source_file ────────────────────────────────────────────────────────
    # Set by extractor.py; passed through here to keep the record complete.
    record["source_file"] = source_file

    return record


# ─────────────────────────────────────────────────────────────────────────────
# Optional field extractor
# ─────────────────────────────────────────────────────────────────────────────

def _extract_optional(msg, body: str) -> dict:
    """
    Extract all optional fields from the parsed message object.

    None of these fields cause a ParseError if missing — they are stored
    as NULL in the database.

    Fields extracted here:
        cc_addresses, bcc_addresses,
        x_from, x_to, x_cc, x_bcc, x_folder, x_origin,
        content_type, has_attachment, headings

    Note: forwarded_content and quoted_content are extracted in
    _extract_mandatory() as a by-product of the body split, so they
    are not repeated here.

    Args:
        msg:  Parsed email.message.Message object.
        body: Already-extracted primary body text (used for headings
              and attachment inference).

    Returns:
        Dict with optional field values (None where absent).
    """
    record = {}

    # ── cc_addresses / bcc_addresses ───────────────────────────────────────
    # Same parsing logic as to_addresses.
    raw_cc  = _decode_header_value(msg.get("Cc"))
    raw_bcc = _decode_header_value(msg.get("Bcc"))
    record["cc_addresses"]  = _parse_address_list(raw_cc)
    record["bcc_addresses"] = _parse_address_list(raw_bcc)

    # ── Enron-specific X-* headers ─────────────────────────────────────────
    # These are custom headers added by Enron's mail infrastructure.
    # X-From/X-To/X-cc/X-bcc hold display names (not necessarily real addresses).
    # We store them as raw strings — no address parsing applied here.
    record["x_from"]   = _decode_header_value(msg.get("X-From"))   or None
    record["x_to"]     = _decode_header_value(msg.get("X-To"))     or None
    record["x_cc"]     = _decode_header_value(msg.get("X-cc"))     or None   # Note: lowercase "cc"
    record["x_bcc"]    = _decode_header_value(msg.get("X-bcc"))    or None   # Note: lowercase "bcc"
    record["x_folder"] = _decode_header_value(msg.get("X-Folder")) or None
    record["x_origin"] = _decode_header_value(msg.get("X-Origin")) or None

    # ── content_type ───────────────────────────────────────────────────────
    # Store the raw Content-Type value (e.g. "text/plain; charset=us-ascii").
    record["content_type"] = _decode_header_value(msg.get("Content-Type")) or None

    # ── has_attachment ─────────────────────────────────────────────────────
    # Inferred from MIME type and body keywords (see _infer_has_attachment).
    record["has_attachment"] = _infer_has_attachment(msg, body)

    # ── headings ───────────────────────────────────────────────────────────
    # Extracted from the primary body text using plain-text heuristics.
    headings = _extract_headings(body)
    record["headings"] = headings if headings else None

    return record


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse(filepath: Path, source_file: str) -> dict:
    """
    Parse a single raw Enron email file and return a flat record dict.

    This is the only public function in this module. Called once per file
    by pipeline/extractor._process_file().

    Process:
      1. Read the file as raw bytes.
      2. Parse with stdlib BytesParser (handles MIME, encodings, multi-line headers).
      3. Extract mandatory fields → raise ParseError if any are absent.
      4. Extract optional fields → None if absent (never raises).
      5. Merge into a single flat dict and return.

    The returned dict maps directly to the emails table columns plus
    address list fields (to_addresses, cc_addresses, bcc_addresses) that
    pipeline/storage.py will route to the email_addresses table.

    Args:
        filepath:    Path object pointing to the raw email file.
        source_file: Relative path string (set by extractor, stored in DB).

    Returns:
        Flat dict with all field values.

    Raises:
        ParseError:  If a mandatory field cannot be extracted.
        OSError:     If the file cannot be read (propagated to extractor).
    """
    # ── Step 1: Read raw bytes ─────────────────────────────────────────────
    raw_bytes = _read_raw(filepath)

    # ── Step 2: Parse with BytesParser ────────────────────────────────────
    # BytesParser handles:
    #   - RFC 2822 header parsing
    #   - Multi-line (folded) header values
    #   - MIME structure
    # We use policy=email.policy.compat32 (the default) because the Enron
    # dataset predates the newer email6 policy and some headers are
    # non-standard in ways the strict policy rejects.
    msg = BytesParser().parsebytes(raw_bytes)

    # ── Step 3: Extract mandatory fields ──────────────────────────────────
    # ParseError raised here propagates to extractor for logging.
    mandatory = _extract_mandatory(msg, source_file)

    # ── Step 4: Extract optional fields ───────────────────────────────────
    # body is already in mandatory; pass it to optional so it can be used
    # for heading extraction and attachment inference without re-reading.
    optional = _extract_optional(msg, mandatory["body"])

    # ── Step 5: Merge and return ───────────────────────────────────────────
    # Mandatory fields take precedence in case of key collision (shouldn't
    # happen, but being explicit is safer).
    return {**optional, **mandatory}
