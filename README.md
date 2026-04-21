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

| **Total Emails**     | **25,065** |                                                |

These mailboxes are primarly selected for live demo purposes:

taylor-m (Email count: 13,875): Legal counsel mailbox — contract reviews, compliance, and general counsel correspondence. Largest mailbox in this set and the anchor for duplicate detection since legal emails get heavily forwarded across the team.

haedicke-m (Email count: 5,246): Senior legal management. Selected alongside taylor-m because these two mailboxes overlap heavily — the same emails get CC'd and forwarded between legal counsel and legal management, which produces a strong duplicate signal.This is reflected in the optional field completeness: cc_addresses populated at 31.9% and forwarded_content at 33.1% across the dataset, both driven largely by these two legal mailboxes.

skilling-j (Email count: 4,139): CEO communications — strategic decisions, company-wide directives, and executive correspondence. Adds cross-functional diversity and bridges the legal and trading mailboxes.

buy-r (Email count: 2,429): Energy trading. Selected to add a trading perspective alongside the legal/executive mailboxes and increase topical diversity.

whitt-m (Email count: 807): Operations. Smaller mailbox included to round out the set to five and add operational email coverage.

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
