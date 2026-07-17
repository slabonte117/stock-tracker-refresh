# Stock Tracker Refresh Worker

A Notion-hosted TypeScript Worker that refreshes market data for every populated row in a Notion Stock Tracker database.

## What it updates

- `Market Price`
- `Day Change %`
- `Snapshot Date`

It preserves ownership, shares, purchase price, accounts, targets, notes, thesis, assessments, and all other user-managed portfolio fields.

## Schedule

The Worker checks every five minutes, but refreshes only on weekdays during these `America/Chicago` windows:

| Snapshot | Central Time | Eastern Time |
| --- | --- | --- |
| Post-open | 9:00–9:04 AM | 10:00–10:04 AM |
| Post-close | 3:10–3:14 PM | 4:10–4:14 PM |

Weekend checks exit with no quote requests or database writes.

## Capabilities

| Capability | Trigger | Purpose |
| --- | --- | --- |
| `stockTrackerFullDryRun` | Manual | Plans updates without changing Stock Tracker |
| `stockTrackerFullRefresh` | Manual | Runs a live full refresh |
| `stockTrackerScheduledRefresh` | Every 5 minutes | Refreshes only inside approved market windows |

## Reliability

- 15-row paginated batches with Notion-hosted continuation
- Duplicate-symbol grouping
- Finnhub pacing: one request every two seconds
- Notion write pacing: one update every 400 milliseconds
- Up to three bounded retry attempts
- Failed quotes preserve the last valid stored market values
- Per-symbol and per-page failure isolation

## Development
