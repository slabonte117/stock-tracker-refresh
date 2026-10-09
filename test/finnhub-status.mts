import { fetchFinnhubQuote, FinnhubUnsupportedSymbolError } from "../src/index.ts";
process.env.FINNHUB_API_KEY = "dummy";
let calls = 0;
const mk = (status: number, body: unknown = {}) => async () => { calls++; return new Response(JSON.stringify(body), { status }); };
const ok = (c: boolean, m: string) => { if (!c) { console.error("FAIL", m); process.exit(1); } console.log("ok  ", m); };

(globalThis as any).fetch = mk(403);
calls = 0;
try { await fetchFinnhubQuote("FXAIX"); ok(false, "403 should throw"); }
catch (e) { ok(e instanceof FinnhubUnsupportedSymbolError, "403 -> FinnhubUnsupportedSymbolError"); ok(calls === 1, "403 not retried (1 request)"); }

(globalThis as any).fetch = mk(404);
calls = 0;
try { await fetchFinnhubQuote("ZZZZ"); ok(false, "404 should throw"); }
catch (e) { ok(!(e instanceof FinnhubUnsupportedSymbolError), "404 stays an ordinary failure"); ok(calls === 1, "404 not retried"); }

(globalThis as any).fetch = mk(200, { c: 123.45, dp: 1.5, t: Math.floor(Date.now() / 1000) });
const q = await fetchFinnhubQuote("AAPL");
ok(q.marketPrice === 123.45, "200 still returns the quote");
console.log("all behaviour checks passed");
process.exit(0);
