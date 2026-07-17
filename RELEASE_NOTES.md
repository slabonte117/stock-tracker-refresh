\# Release Notes



\## v1.0.0 â€” Initial production release



### Delivered



- Notion-hosted Stock Tracker refresh Worker

- Finnhub quote integration

- Full Stock Tracker coverage for all populated symbols

- Worker-managed `Stock Refresh Runs` history database

- 15-row pagination with Notion-hosted continuation

- Duplicate-symbol grouping

- Finnhub pacing and Notion write pacing

- Bounded Finnhub and Notion retries

- Failure isolation and preservation of last valid values

- Manual dry-run and full-refresh capabilities

- Weekday market-time scheduled refresh

- Windows/PowerShell build and deployment workflow

- Private GitHub source backup and recovery path



### Scheduled windows



- Weekdays 9:00â€“9:04 AM Central / 10:00â€“10:04 AM Eastern

- Weekdays 3:10â€“3:14 PM Central / 4:10â€“4:14 PM Eastern

- Weekend refreshes are blocked



### Validation completed



- Local TypeScript checks and builds passed

- Hosted smoke test passed

- Finnhub provider validation passed

- Read-only Stock Tracker validation passed

- Controlled AAPL update passed

- Two-symbol, ten-symbol, owned-position, and full-tracker refreshes passed

- Retry-safe dry run and live refresh passed

- Scheduled off-hours gate passed

- Full Git-history audit passed: 14 commits scanned, 0 leaks found



### Remaining operational validation



- Observe one naturally scheduled weekday morning refresh

- Observe one naturally scheduled weekday close refresh

- Perform one clone-and-recovery drill from GitHub



### Known limitation



Weekends are blocked. Exchange-holiday handling is not yet implemented; on a weekday market holiday, Finnhub may return the latest available quote.


