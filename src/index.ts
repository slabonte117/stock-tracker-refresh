import { Worker } from "@notionhq/workers";
import * as Builder from "@notionhq/workers/builder";
import * as Schema from "@notionhq/workers/schema";

const worker = new Worker();
export default worker;

type Quote = {
  symbol: string;
  marketPrice: number;
  dayChangeDecimal: number;
  quotedAt: string;
};

type FinnhubQuoteResponse = {
  c: number;
  dp: number;
  t: number;
};

type RetryCallback = (details: {
  service: "Finnhub" | "Notion";
  attempt: number;
  delayMs: number;
  status?: number;
}) => void;

type StockTrackerRefreshState = {
  cursor?: string;
  runId: string;
  startedAt: string;
  batchNumber: number;
  rowsDiscovered: number;
  quotesRequested: number;
  quotesReceived: number;
  rowsPlanned: number;
  rowsUpdated: number;
  rowsSkipped: number;
  failureCount: number;
  finnhubRetries: number;
  notionRetries: number;
  failureSamples: Array<{
    symbol: string;
    reason: string;
  }>;
  symbolCounts: Record<string, number>;
};

const FINNHUB_MAX_ATTEMPTS = 3;
const NOTION_MAX_ATTEMPTS = 3;
const MAX_RETRY_DELAY_MS = 10_000;

const stockRefreshRuns = worker.database("stockRefreshRuns", {
  type: "managed",
  initialTitle: "Stock Refresh Runs",
  primaryKeyProperty: "Run ID",
  schema: {
    properties: {
      "Run ID": Schema.title(),
      Status: Schema.select([
        { name: "Smoke test", color: "blue" },
        { name: "Success", color: "green" },
        { name: "Failed", color: "red" },
      ]),
      "Executed At": Schema.richText(),
      Summary: Schema.richText(),
    },
  },
});

const scalableFinnhubPacer = worker.pacer("scalableFinnhubPacer", {
  allowedRequests: 1,
  intervalMs: 2000,
});

const scalableNotionWritePacer = worker.pacer(
  "scalableNotionWritePacer",
  {
    allowedRequests: 1,
    intervalMs: 400,
  },
);

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

function parseRetryAfterMs(
  value: string | null | undefined,
): number | undefined {
  if (!value) {
    return undefined;
  }

  const seconds = Number(value);

  if (Number.isFinite(seconds) && seconds >= 0) {
    return seconds * 1000;
  }

  const retryAt = Date.parse(value);

  if (Number.isFinite(retryAt)) {
    return Math.max(0, retryAt - Date.now());
  }

  return undefined;
}

function readHeader(
  container: unknown,
  headerName: string,
): string | undefined {
  if (typeof container !== "object" || container === null) {
    return undefined;
  }

  const possibleHeaders = container as {
    get?: (name: string) => string | null;
    [key: string]: unknown;
  };

  if (typeof possibleHeaders.get === "function") {
    return possibleHeaders.get.call(container, headerName) ?? undefined;
  }

  const targetName = headerName.toLowerCase();

  for (const [key, value] of Object.entries(possibleHeaders)) {
    if (key.toLowerCase() === targetName && typeof value === "string") {
      return value;
    }
  }

  return undefined;
}

function getErrorStatus(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) {
    return undefined;
  }

  const candidate = error as {
    status?: unknown;
    statusCode?: unknown;
    code?: unknown;
    response?: { status?: unknown };
  };

  if (typeof candidate.status === "number") {
    return candidate.status;
  }

  if (typeof candidate.statusCode === "number") {
    return candidate.statusCode;
  }

  if (typeof candidate.response?.status === "number") {
    return candidate.response.status;
  }

  if (candidate.code === "rate_limited") {
    return 429;
  }

  return undefined;
}

function getErrorRetryAfterMs(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) {
    return undefined;
  }

  const candidate = error as {
    headers?: unknown;
    response?: { headers?: unknown };
  };

  return parseRetryAfterMs(
    readHeader(candidate.headers, "Retry-After") ??
      readHeader(candidate.response?.headers, "Retry-After"),
  );
}

function isRetryableStatus(status: number): boolean {
  return (
    status === 408 ||
    status === 409 ||
    status === 425 ||
    status === 429 ||
    status >= 500
  );
}

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

async function withNotionRetry<T>(
  operation: () => Promise<T>,
  operationName: string,
  onRetry?: RetryCallback,
): Promise<T> {
  for (
    let attempt = 1;
    attempt <= NOTION_MAX_ATTEMPTS;
    attempt += 1
  ) {
    try {
      return await operation();
    } catch (error) {
      const status = getErrorStatus(error);
      const retryable =
        status === undefined || isRetryableStatus(status);

      if (!retryable || attempt === NOTION_MAX_ATTEMPTS) {
        throw new Error(
          `${operationName} failed after ${attempt} attempt(s): ${getErrorMessage(
            error,
            "Unknown Notion API error.",
          )}`,
        );
      }

      const delayMs =
        getErrorRetryAfterMs(error) ?? 1000 * 2 ** (attempt - 1);

      if (delayMs > MAX_RETRY_DELAY_MS) {
        throw new Error(
          `${operationName} requested a retry delay of ${delayMs}ms, which exceeds the safe retry window.`,
        );
      }

      onRetry?.({
        service: "Notion",
        attempt,
        delayMs,
        status,
      });

      await sleep(delayMs);
    }
  }

  throw new Error(`${operationName} ended unexpectedly.`);
}

async function fetchFinnhubQuote(
  symbol: string,
  onRetry?: RetryCallback,
): Promise<Quote> {
  const apiKey = process.env.FINNHUB_API_KEY;

  if (!apiKey) {
    throw new Error("FINNHUB_API_KEY is not configured.");
  }

  const url = new URL("https://finnhub.io/api/v1/quote");
  url.searchParams.set("symbol", symbol);

  for (
    let attempt = 1;
    attempt <= FINNHUB_MAX_ATTEMPTS;
    attempt += 1
  ) {
    let response: Response;

    try {
      response = await fetch(url.toString(), {
        headers: {
          "X-Finnhub-Token": apiKey,
        },
      });
    } catch (error) {
      if (attempt === FINNHUB_MAX_ATTEMPTS) {
        throw new Error(
          `Finnhub request for ${symbol} failed after ${attempt} attempts: ${getErrorMessage(
            error,
            "Unknown Finnhub network error.",
          )}`,
        );
      }

      const delayMs = 2000 * 2 ** (attempt - 1);

      onRetry?.({
        service: "Finnhub",
        attempt,
        delayMs,
      });

      await sleep(delayMs);
      continue;
    }

    if (!response.ok) {
      if (
        !isRetryableStatus(response.status) ||
        attempt === FINNHUB_MAX_ATTEMPTS
      ) {
        throw new Error(
          `Finnhub request for ${symbol} failed with HTTP ${response.status} after ${attempt} attempt(s).`,
        );
      }

      const delayMs =
        parseRetryAfterMs(response.headers.get("Retry-After")) ??
        2000 * 2 ** (attempt - 1);

      if (delayMs > MAX_RETRY_DELAY_MS) {
        throw new Error(
          `Finnhub returned HTTP ${response.status} for ${symbol}, but its Retry-After delay of ${delayMs}ms exceeds the safe retry window.`,
        );
      }

      onRetry?.({
        service: "Finnhub",
        attempt,
        delayMs,
        status: response.status,
      });

      await sleep(delayMs);
      continue;
    }

    let raw: Partial<FinnhubQuoteResponse>;

    try {
      raw = (await response.json()) as Partial<FinnhubQuoteResponse>;
    } catch (error) {
      if (attempt === FINNHUB_MAX_ATTEMPTS) {
        throw new Error(
          `Finnhub returned an unreadable response for ${symbol} after ${attempt} attempt(s): ${getErrorMessage(
            error,
            "Invalid JSON response.",
          )}`,
        );
      }

      const delayMs = 2000 * 2 ** (attempt - 1);

      onRetry?.({
        service: "Finnhub",
        attempt,
        delayMs,
        status: response.status,
      });

      await sleep(delayMs);
      continue;
    }

    if (
      typeof raw.c !== "number" ||
      typeof raw.dp !== "number" ||
      typeof raw.t !== "number" ||
      !Number.isFinite(raw.c) ||
      !Number.isFinite(raw.dp) ||
      !Number.isFinite(raw.t) ||
      raw.c <= 0 ||
      raw.t <= 0
    ) {
      throw new Error(`Finnhub returned an invalid quote for ${symbol}.`);
    }

    return {
      symbol,
      marketPrice: raw.c,
      dayChangeDecimal: raw.dp / 100,
      quotedAt: new Date(raw.t * 1000).toISOString(),
    };
  }

  throw new Error(`Finnhub request for ${symbol} ended unexpectedly.`);
}

function extractSymbol(page: unknown): string {
  const properties = (
    page as {
      properties?: Record<string, unknown>;
    }
  ).properties;

  const symbolProperty = properties?.Symbol as
    | {
        type?: string;
        title?: Array<{ plain_text?: string }>;
      }
    | undefined;

  if (
    symbolProperty?.type !== "title" ||
    !Array.isArray(symbolProperty.title)
  ) {
    return "";
  }

  return symbolProperty.title
    .map((item) => item.plain_text ?? "")
    .join("")
    .trim()
    .toUpperCase();
}

function toChicagoDate(isoTimestamp: string): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date(isoTimestamp));

  const values = Object.fromEntries(
    parts.map((part) => [part.type, part.value]),
  );

  if (!values.year || !values.month || !values.day) {
    throw new Error("Could not convert quote timestamp to a Chicago date.");
  }

  return `${values.year}-${values.month}-${values.day}`;
}

/*
 * Native sync schedules are interval-only. This function enforces the
 * business-time rule in America/Chicago:
 *
 * - 10:00 through 10:04 a.m.
 * - 3:10 through 3:14 p.m.
 *
 * A started multi-batch run continues regardless of the clock so it cannot
 * be abandoned halfway through its 75-row refresh.
 */
function isScheduledRefreshWindow(now = new Date()): boolean {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Chicago",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(now);

  const values = Object.fromEntries(
    parts.map((part) => [part.type, part.value]),
  );

  const weekday = values.weekday;
  const hour = Number(values.hour);
  const minute = Number(values.minute);

  const isWeekday = weekday !== "Sat" && weekday !== "Sun";

  const morningWindow = hour === 9 && minute >= 0 && minute <= 4;
  const afternoonWindow = hour === 15 && minute >= 10 && minute <= 14;

  return isWeekday && (morningWindow || afternoonWindow);
}

function registerScalableStockTrackerCapability(
  capabilityKey: string,
  performWrite: boolean,
  schedule: "manual" | "5m",
  onlyRunInScheduledWindow = false,
) {
  worker.sync(capabilityKey, {
    database: stockRefreshRuns,
    mode: "incremental",
    schedule,

    execute: async (rawState, { notion }) => {
      const previous = rawState as StockTrackerRefreshState | undefined;

      /*
       * A fresh scheduled check outside either target window exits without:
       * - Finnhub calls
       * - Stock Tracker reads or writes
       * - a Stock Refresh Runs record
       */
      if (
        onlyRunInScheduledWindow &&
        !previous &&
        !isScheduledRefreshWindow()
      ) {
        return {
          changes: [],
          hasMore: false,
        };
      }

      const mode = performWrite ? "write" : "dry-run";
      const startedAt = previous?.startedAt ?? new Date().toISOString();

      const progress: StockTrackerRefreshState = previous
        ? {
            ...previous,
            finnhubRetries: previous.finnhubRetries ?? 0,
            notionRetries: previous.notionRetries ?? 0,
            failureSamples: [...previous.failureSamples],
            symbolCounts: { ...previous.symbolCounts },
          }
        : {
            runId: `tracker-paginated-${mode}-${startedAt}`,
            startedAt,
            batchNumber: 0,
            rowsDiscovered: 0,
            quotesRequested: 0,
            quotesReceived: 0,
            rowsPlanned: 0,
            rowsUpdated: 0,
            rowsSkipped: 0,
            failureCount: 0,
            finnhubRetries: 0,
            notionRetries: 0,
            failureSamples: [],
            symbolCounts: {},
          };

      const recordRetry: RetryCallback = (details) => {
        if (details.service === "Finnhub") {
          progress.finnhubRetries += 1;
        } else {
          progress.notionRetries += 1;
        }
      };

      const recordFailure = (
        symbol: string,
        reason: string,
        skippedRows: number,
      ) => {
        progress.failureCount += 1;
        progress.rowsSkipped += skippedRows;

        if (progress.failureSamples.length < 25) {
          progress.failureSamples.push({ symbol, reason });
        }
      };

      const buildSummary = (hasMore: boolean, batchRows: number) => {
        const duplicateSymbols = Object.entries(progress.symbolCounts)
          .filter(([, count]) => count > 1)
          .map(([symbol, count]) => ({ symbol, count }));

        return {
          test: "Paginated full Stock Tracker refresh",
          mode: onlyRunInScheduledWindow ? "scheduled-write" : mode,
          schedule: onlyRunInScheduledWindow
            ? "America/Chicago 10:00 and 15:10"
            : "manual",
          batchNumber: progress.batchNumber,
          batchRows,
          hasMore,
          cumulative: {
            rowsDiscovered: progress.rowsDiscovered,
            uniqueSymbols: Object.keys(progress.symbolCounts).length,
            quotesRequested: progress.quotesRequested,
            quotesReceived: progress.quotesReceived,
            rowsPlanned: progress.rowsPlanned,
            rowsUpdated: progress.rowsUpdated,
            rowsSkipped: progress.rowsSkipped,
            failureCount: progress.failureCount,
            finnhubRetries: progress.finnhubRetries,
            notionRetries: progress.notionRetries,
          },
          duplicateSymbols,
          failureSamples: progress.failureSamples,
          propertiesTargeted: [
            "Market Price",
            "Day Change %",
            "Snapshot Date",
          ],
          runtimeMs: Date.now() - Date.parse(progress.startedAt),
        };
      };

      const buildRunChange = (
        status: "Success" | "Failed",
        summary: object,
      ) => ({
        type: "upsert" as const,
        key: progress.runId,
        properties: {
          "Run ID": Builder.title(progress.runId),
          Status: Builder.select(status),
          "Executed At": Builder.richText(progress.startedAt),
          Summary: Builder.richText(JSON.stringify(summary)),
        },
      });

      try {
        const searchResponse = await withNotionRetry(
          () =>
            notion.search({
              filter: {
                property: "object",
                value: "data_source",
              },
              page_size: 10,
            }),
          "Notion data-source search",
          recordRetry,
        );

        if (searchResponse.results.length !== 1) {
          throw new Error(
            `Expected exactly one accessible data source; found ${searchResponse.results.length}.`,
          );
        }

        const response = await withNotionRetry(
          () =>
            notion.dataSources.query({
              data_source_id: searchResponse.results[0].id,
              page_size: 15,
              start_cursor: progress.cursor,
            }),
          "Stock Tracker batch query",
          recordRetry,
        );

        progress.batchNumber += 1;

        const groupedRows = new Map<string, string[]>();

        for (const result of response.results) {
          const page = result as {
            id: string;
            properties: Record<string, unknown>;
          };

          progress.rowsDiscovered += 1;

          const symbol = extractSymbol(page);

          if (!symbol) {
            recordFailure(
              "(blank)",
              `Stock Tracker row ${page.id} has a blank or invalid Symbol.`,
              1,
            );
            continue;
          }

          progress.symbolCounts[symbol] =
            (progress.symbolCounts[symbol] ?? 0) + 1;

          const pageIds = groupedRows.get(symbol) ?? [];
          pageIds.push(page.id);
          groupedRows.set(symbol, pageIds);
        }

        for (const [symbol, pageIds] of groupedRows.entries()) {
          progress.quotesRequested += 1;

          try {
            await scalableFinnhubPacer.wait();

            const quote = await fetchFinnhubQuote(symbol, recordRetry);

            progress.quotesReceived += 1;
            progress.rowsPlanned += pageIds.length;

            if (!performWrite) {
              continue;
            }

            const snapshotDate = toChicagoDate(quote.quotedAt);

            for (const pageId of pageIds) {
              try {
                await scalableNotionWritePacer.wait();

                await withNotionRetry(
                  () =>
                    notion.pages.update({
                      page_id: pageId,
                      properties: {
                        "Market Price": {
                          number: quote.marketPrice,
                        },
                        "Day Change %": {
                          number: quote.dayChangeDecimal,
                        },
                        "Snapshot Date": {
                          date: {
                            start: snapshotDate,
                          },
                        },
                      },
                    }),
                  `Notion update for ${symbol}`,
                  recordRetry,
                );

                progress.rowsUpdated += 1;
              } catch (error) {
                recordFailure(
                  symbol,
                  getErrorMessage(
                    error,
                    "Unknown Notion update error.",
                  ),
                  1,
                );
              }
            }
          } catch (error) {
            /*
             * No write happens for this symbol after a quote failure.
             * Existing market values remain intact.
             */
            recordFailure(
              symbol,
              getErrorMessage(error, "Unknown Finnhub quote error."),
              pageIds.length,
            );
          }
        }

        const hasMore =
          response.has_more && response.next_cursor !== null;

        const summary = buildSummary(hasMore, response.results.length);
        const status =
          progress.failureCount > 0 ? "Failed" : "Success";

        if (hasMore && response.next_cursor) {
          return {
            changes: [buildRunChange(status, summary)],
            hasMore: true,
            nextState: {
              ...progress,
              cursor: response.next_cursor,
            },
          };
        }

        return {
          changes: [buildRunChange(status, summary)],
          hasMore: false,
        };
      } catch (error) {
        recordFailure(
          "(batch)",
          getErrorMessage(error, "Unknown paginated refresh error."),
          0,
        );

        return {
          changes: [
            buildRunChange(
              "Failed",
              buildSummary(false, 0),
            ),
          ],
          hasMore: false,
        };
      }
    },
  });
}

/* Retained manual controls */
registerScalableStockTrackerCapability(
  "stockTrackerFullDryRun",
  false,
  "manual",
);

registerScalableStockTrackerCapability(
  "stockTrackerFullRefresh",
  true,
  "manual",
);

/*
 * Native schedule:
 * - Notion invokes this lightweight capability every five minutes.
 * - It performs the actual full refresh only in the two Chicago-time windows.
 */
registerScalableStockTrackerCapability(
  "stockTrackerScheduledRefresh",
  true,
  "5m",
  true,
);



