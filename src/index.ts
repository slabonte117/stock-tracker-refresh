import { Worker } from "@notionhq/workers";
import * as Builder from "@notionhq/workers/builder";
import * as Schema from "@notionhq/workers/schema";

const worker = new Worker();
export default worker;

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
