import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const templateRoot = new URL("../", import.meta.url);

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(
    new Request("http://localhost/", { headers: { accept: "text/html" } }),
    { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } },
    { waitUntil() {}, passThroughOnException() {} },
  );
}

test("server-renders the operational dashboard shell", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /<title>TradingBot Operations<\/title>/i);
  assert.match(html, /dashboard\.html\?preview=1/);
  assert.match(html, /TradingBot operational dashboard/);
  assert.doesNotMatch(html, /codex-preview|react-loading-skeleton|taking shape/i);
});

test("ships the blinded, localhost dashboard assets", async () => {
  const [page, layout, dashboard, script, packageJson] = await Promise.all([
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/layout.tsx", import.meta.url), "utf8"),
    readFile(new URL("../public/dashboard.html", import.meta.url), "utf8"),
    readFile(new URL("../public/dashboard.js", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  assert.match(page, /dashboard-preview/);
  assert.match(layout, /TradingBot Operations/);
  assert.match(dashboard, /Performance blinded/);
  assert.match(dashboard, /No performance queries exist/);
  assert.match(dashboard, /Localhost only/);
  assert.match(script, /fetch\("\/api\/health"/);
  assert.doesNotMatch(packageJson, /react-loading-skeleton/);
  await assert.rejects(access(new URL("app/_sites-preview", templateRoot)));
});
