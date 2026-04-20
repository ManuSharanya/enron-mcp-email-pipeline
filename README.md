# Enron email data extraction pipeline

End-to-end pipeline that ingests raw Enron email files, extracts structured
fields, stores them in SQLite, detects duplicates, and sends notifications via
a Gmail MCP server.

---

## Project Structure

```
.
├── main.py                   # Entry point — run the full pipeline
├── config.py                 # All tunable constants (paths, thresholds, etc.)
├── requirements.txt          # Python dependencies
├── schema.sql                # SQLite DDL — apply automatically or manually
├── sample_queries.sql        # 4 sample queries with expected output notes
├── mcp_config.json.example   # MCP server config template (credentials redacted)
├── pipeline/
│   ├── extractor.py          # Task 1: parse email files → DB
│   ├── storage.py            # Task 2: SQLite schema + insert helpers
│   ├── deduplicator.py       # Task 3: duplicate detection + flagging
│   └── notifier.py           # Task 4: draft / send notification emails
├── utils/
│   ├── email_parser.py       # Low-level email parsing (fields, body split)
│   ├── date_utils.py         # Timezone-aware date normalisation to UTC
│   └── logger.py             # Logging setup (console + error_log.txt)
├── output/
│   └── replies/              # Generated .eml draft files (dry-run output)
├── duplicates_report.csv     # Generated — duplicate groups with scores
├── error_log.txt             # Generated — parse failures
└── AI_USAGE.md               # AI tool usage documentation (required)
```

---

## Selected Mailboxes

| **Total Emails**     | **~76,545** |                                                |

Kaminski-v (Email count: 27,759): High-volume mailbox covering quantitative modelling, risk analysis, and academic research correspondence. Selected for dataset richness and volume (more duplicates).

dasovich-j (Email count: 27,502): High-volume mailbox covering government affairs, regulatory filings, and policy discussions. Selected for topical diversity. More chances of duplicates.

skilling-j (Email count: 3,161): CEO-level executive communications covering strategic decisions and company-wide directives. Replaced kean-s as it contained a large recurring newsletter group that caused significant performance bottlenecks during duplicate detection, making it unsuitable for demonstrating the deduplication pipeline cleanly.

taylor-m (Email count: 13,346): Mid-volume mailbox covering legal counsel, contract reviews, and compliance communications. Selected for functional diversity.

haedicke-m (Email count: 4,777): Senior legal management covering compliance directives and contract oversight. Selected alongside taylor-m to maximise duplicate detection coverage — legal emails are heavily forwarded and CC'd across counsel, making them ideal candidates for demonstrating the deduplication pipeline.

---

## Running

```bash
# Full pipeline (Tasks 1,2,3,4) — dry-run. Writes .eml files to output/replies/
python main.py

# Full pipeline — sends live emails via MCP. Does NOT write .eml files.
python main.py --send-live

# Skip Task 1 (DB already populated) — re-runs Tasks 3 and 4 dry-run. Writes .eml files.
python main.py --skip-extract

# Skip Tasks 1 and 3 — Task 4 dry-run only. Writes .eml files to output/replies/
python main.py --task4-only

# Skip Tasks 1 and 3 — Task 4 live send via MCP. Does NOT write .eml files.
python main.py --task4-only --send-live
```

> **Note:** `.eml` draft files are only written in dry-run mode. Live mode (`--send-live`) delivers emails directly via Gmail and logs results to `output/send_log.csv` instead.

---


## Setup

### Prerequisites
- Python 3.9+
- Node.js 18+ and npm (required for the Gmail MCP server)
- A Gmail account (for sending notification emails)
- An Anthropic API key (for Task 4 live send)

### 1. Install Python dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment variables
```bash
cp .env.example .env
```
Edit `.env` with your values:
```
ANTHROPIC_API_KEY=sk-ant-...
NOTIFICATION_OVERRIDE_EMAIL=you@gmail.com
NOTIFICATION_LIMIT=15
```

### 3. Download the Enron dataset
Download from https://www.cs.cmu.edu/~enron/ and extract so the path matches:
```
enron_mail_20150507/maildir/
```

### 4. MCP Setup
Follow the MCP Setup section below before running Task 4.

---

## MCP Setup

### 1. Create a Google Cloud Project
- Go to [console.cloud.google.com](https://console.cloud.google.com)
- Click **New Project** → give it a name -> click **Create**

### 2. Enable the Gmail API
- In the project, go to **APIs & Services**-> **Library**
- Search for **Gmail API** -> click it -> click **Enable**

### 3. Create OAuth 2.0 Credentials
- Go to **APIs & Services** -> **Credentials**
- Click **Create Credentials** -> **OAuth 2.0 Client ID**
- Application type: **Desktop app** -> click **Create**
- Click **Download JSON** -> rename the file to `credentials.json` -> place it in the project root

### 4. Configure OAuth Consent Screen
- Go to **APIs & Services** -> **OAuth consent screen**
- Set publishing status to **Testing**
- Under **Test users** -> click **Add users** -> add your Gmail address

### 5. Install the MCP Server
```bash
npm install -g @gongrzhe/server-gmail-autoauth-mcp
```

### 6. Authenticate
```bash
npx @gongrzhe/server-gmail-autoauth-mcp auth
```
- A browser window opens -> sign in with your Gmail account -> click **Allow** -> you will see "Authentication successful". The OAuth token is stored automatically.

### 7. Configure the Pipeline
Copy the example config:
```bash
cp mcp_config.json.example mcp_config.json
```
- Add your Gmail address to .env: 
NOTIFICATION_OVERRIDE_EMAIL=you@gmail.com
ANTHROPIC_API_KEY=sk-ant-...


### 8. Verify the MCP Server
```bash
npx @gongrzhe/server-gmail-autoauth-mcp 
```
- If no errors appear, the server is running correctly and ready to send emails.
---

## Architecture Overview

```
maildir files (5 mailboxes)
│
▼
extractor.py ──► SQLite DB (emails + email_addresses tables)
│
▼
deduplicator.py
(Phase 1: SHA-256 hash → exact duplicates)
(Phase 2: fuzz.ratio on representatives → near-duplicates)
(Union-Find clustering → duplicate groups)
│
├──► duplicates_report.csv
│
▼
notifier.py
│
├── Dry-run ──► output/replies/*.eml
│
└── Live ──► Claude API (Anthropic SDK)
              │
              ▼
         Gmail MCP Server
         (send_email tool)
              │
              ▼
         Gmail Inbox
```
