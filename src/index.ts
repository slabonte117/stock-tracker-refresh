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

async function fetchFinnhubQuote(symbol: string): Promise<Quote> {
  const apiKey = process.env.FINNHUB_API_KEY;

  if (!apiKey) {
    throw new Error("FINNHUB_API_KEY is not configured.");
  }

  const url =
    `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(symbol)}`;

  const response = await fetch(url, {
    headers: {
      "X-Finnhub-Token": apiKey,
    },
  });

  if (!response.ok) {
    throw new Error(
      `Finnhub request for ${symbol} failed with HTTP ${response.status}.`,
    );
  }

  const raw = (await response.json()) as Partial<FinnhubQuoteResponse>;

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

worker.sync("stockRefreshSmokeTest", {
  database: stockRefreshRuns,
  mode: "incremental",
  schedule: "manual",
  execute: async () => {
    const executedAt = new Date().toISOString();
    const runId = `smoke-${executedAt}`;

    return {
      changes: [
        {
          type: "upsert" as const,
          key: runId,
          properties: {
            "Run ID": Builder.title(runId),
            Status: Builder.select("Smoke test"),
            "Executed At": Builder.richText(executedAt),
            Summary: Builder.richText(
              "Hosted Worker smoke test. No market API or Stock Tracker access.",
            ),
          },
        },
      ],
      hasMore: false,
    };
  },
});

worker.sync("quoteProviderTest", {
  database: stockRefreshRuns,
  mode: "incremental",
  schedule: "manual",
  execute: async () => {
    const executedAt = new Date().toISOString();
    const runId = `provider-test-${executedAt}`;

    try {
      const quotes: Quote[] = [];

      for (const symbol of ["AAPL", "VTI"]) {
        quotes.push(await fetchFinnhubQuote(symbol));
      }

      return {
        changes: [
          {
            type: "upsert" as const,
            key: runId,
            properties: {
              "Run ID": Builder.title(runId),
              Status: Builder.select("Success"),
              "Executed At": Builder.richText(executedAt),
              Summary: Builder.richText(
                JSON.stringify({
                  provider: "Finnhub",
                  symbols: ["AAPL", "VTI"],
                  quotes,
                }),
              ),
            },
          },
        ],
        hasMore: false,
      };
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown provider error.";

      return {
        changes: [
          {
            type: "upsert" as const,
            key: runId,
            properties: {
              "Run ID": Builder.title(runId),
              Status: Builder.select("Failed"),
              "Executed At": Builder.richText(executedAt),
              Summary: Builder.richText(message),
            },
          },
        ],
        hasMore: false,
      };
    }
  },
});

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

worker.sync("stockTrackerReadTest", {
  database: stockRefreshRuns,
  mode: "incremental",
  schedule: "manual",
  execute: async (_state, { notion }) => {
    const executedAt = new Date().toISOString();
    const runId = `tracker-read-${executedAt}`;

    try {
      const searchResponse = await notion.search({
        filter: {
          property: "object",
          value: "data_source",
        },
        page_size: 10,
      });

      if (searchResponse.results.length !== 1) {
        throw new Error(
          `Expected exactly one accessible data source; found ${searchResponse.results.length}.`,
        );
      }

      const dataSourceId = searchResponse.results[0].id;
      const symbols: string[] = [];
      let rowsFound = 0;
      let cursor: string | undefined;

      do {
        const response = await notion.dataSources.query({
          data_source_id: dataSourceId,
          page_size: 100,
          start_cursor: cursor,
        });

        rowsFound += response.results.length;

        for (const page of response.results) {
          const symbol = extractSymbol(page);

          if (symbol) {
            symbols.push(symbol);
          }
        }

        cursor =
          response.has_more && response.next_cursor
            ? response.next_cursor
            : undefined;
      } while (cursor);

      const symbolCounts = new Map<string, number>();

      for (const symbol of symbols) {
        symbolCounts.set(symbol, (symbolCounts.get(symbol) ?? 0) + 1);
      }

      const duplicateSymbols = [...symbolCounts.entries()]
        .filter(([, count]) => count > 1)
        .map(([symbol, count]) => ({ symbol, count }));

      const summary = {
        test: "Stock Tracker read-only access",
        dataSourceId,
        rowsFound,
        populatedSymbols: symbols.length,
        blankSymbols: rowsFound - symbols.length,
        uniqueSymbols: symbolCounts.size,
        duplicateSymbols,
        writesAttempted: 0,
      };

      return {
        changes: [
          {
            type: "upsert" as const,
            key: runId,
            properties: {
              "Run ID": Builder.title(runId),
              Status: Builder.select("Success"),
              "Executed At": Builder.richText(executedAt),
              Summary: Builder.richText(JSON.stringify(summary)),
            },
          },
        ],
        hasMore: false,
      };
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown read-test error.";

      return {
        changes: [
          {
            type: "upsert" as const,
            key: runId,
            properties: {
              "Run ID": Builder.title(runId),
              Status: Builder.select("Failed"),
              "Executed At": Builder.richText(executedAt),
              Summary: Builder.richText(message),
            },
          },
        ],
        hasMore: false,
      };
    }
  },
});
