-- sample_queries.sql
-- Enron Email Pipeline — Sample SQL Queries
-- Run against enron_emails.db (populated by python main.py)


-- Query 1: Top 10 senders by email volume
-- Expected output: 10 rows showing email addresses and their send counts.
-- Top senders are mostly from the selected mailboxes (taylor-m, haedicke-m, skilling-j).

SELECT
    from_address,
    COUNT(*) AS email_count
FROM emails
GROUP BY from_address
ORDER BY email_count DESC
LIMIT 10;


-- Query 2: All emails sent within a date range (Q1 2001)
-- Expected output: all emails sent between Jan 1 and Mar 31 2001, ordered by date.
-- Returns thousands of rows — Q1 2001 was a high-activity period in the dataset.

SELECT
    message_id,
    date,
    from_address,
    subject
FROM emails
WHERE date >= '2001-01-01T00:00:00+00:00'
  AND date <  '2001-04-01T00:00:00+00:00'
ORDER BY date;


-- Query 3: Emails that have at least one CC recipient
-- Expected output: emails that had at least one CC recipient, ordered by most recent.
-- Each row shows the message_id, subject, sender, and date.

SELECT DISTINCT
    e.message_id,
    e.subject,
    e.from_address,
    e.date
FROM emails e
JOIN email_addresses a ON a.message_id = e.message_id
WHERE a.field = 'cc'
ORDER BY e.date DESC;


-- Query 4: Top 5 most emailed-to recipients
-- Expected output: 5 rows showing the most frequently emailed addresses.
-- Reflects who the most central recipients are across all 5 mailboxes.

SELECT
    address,
    COUNT(*) AS times_received
FROM email_addresses
WHERE field = 'to'
GROUP BY address
ORDER BY times_received DESC
LIMIT 5;
