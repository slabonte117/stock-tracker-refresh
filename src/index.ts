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
