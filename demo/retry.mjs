import { existsSync } from 'node:fs';

import { chromium } from 'playwright';

// A retry shows the model its own statement, a busy server is waited out without telling the
// model, and local-model mode asks for a model name the local server has.
const csv = ['posted,merchant,amount', '2024-01-02,SHOP,-1.00', '2024-01-03,SHOP,-2.50', ''].join('\n');

const port = process.argv[2] || '18747';
const modelPort = '18798';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);
const context = await browser.newContext({ locale: 'en-US' });
const page = await context.newPage();
page.on('pageerror', (err) => console.error('pageerror', err.message));

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

const good = {
  sql: 'SELECT sum(amount) AS total FROM t',
  chart: { kind: 'number', y: 'total' },
  narration: 'The total is {r0.total}.',
};
const broken = { ...good, sql: 'SELECT nope_column FROM t' };
const ok = (reply) => ({
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
});
const fail = (status, message) => ({
  status,
  contentType: 'application/json',
  body: JSON.stringify({ type: 'error', error: { type: 'api_error', message } }),
});

// Each request takes the next scripted answer; the last repeats.
let script = [];
const seen = [];
await page.route('https://api.anthropic.com/**', async (route) => {
  seen.push(JSON.parse(route.request().postData()));
  const next = script.length > 1 ? script.shift() : script[0];
  await route.fulfill(next);
});
const localSeen = [];
let localScript = null;
await page.route('http://127.0.0.1:' + modelPort + '/**', async (route) => {
  localSeen.push(JSON.parse(route.request().postData()));
  if (localScript === 'abort') await route.abort();
  else await route.fulfill(ok(good));
});

await page.clock.install();
await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({ name: 'a.csv', mimeType: 'text/csv', buffer: Buffer.from(csv) });
await page.getByText('SHOP').first().waitFor({ timeout: 120000 });
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('What is the total?');
await page.locator('#confirm').uncheck();

const statusText = () => page.locator('#status').innerText();
async function settle(done) {
  for (let i = 0; i < 200; i++) {
    if (await page.evaluate(done)) return true;
    await page.clock.runFor(5000);
    await page.waitForTimeout(50);
  }
  return false;
}
const asked = () => !document.getElementById('ask').disabled;

// A statement that does not plan, two busy answers, then a good one.
script = [ok(broken), fail(529, 'Overloaded'), fail(500, 'Internal server error'), ok(good)];
await page.locator('#ask').click();
if (!(await settle(asked))) bad('the ask never finished');
const text = (req) => req.messages[0].content;
if (seen.length !== 4) bad('expected four requests, got ' + seen.length);
if (seen[0] && text(seen[0]).includes('Previous')) bad('the first request had a previous attempt');
if (seen[1] && !text(seen[1]).includes('Previous statement:\nSELECT nope_column FROM t')) {
  bad('the retry did not carry the statement: ' + (seen[1] && text(seen[1]).slice(-300)));
}
for (const i of [2, 3]) {
  if (seen[i] && text(seen[i]).includes('model http')) bad('request ' + i + ' told the model of a server error');
  if (seen[i] && !text(seen[i]).includes('Previous statement:\nSELECT nope_column FROM t')) {
    bad('request ' + i + ' lost the statement');
  }
}
if (seen[1] && !text(seen[1]).includes('counting rows from 0')) bad('rN is not said to be 0-based');
const narration = await page.locator('#narration').innerText();
if (!narration.includes('-3.50')) bad('narration: ' + narration);

// A server that stays down ends with its own message, not a statement about the model.
seen.length = 0;
script = [fail(503, 'Service Unavailable')];
await page.locator('#ask').click();
if (!(await settle(asked))) bad('the outage ask never finished');
if (seen.length !== 4) bad('outage: expected four requests, got ' + seen.length);
if (seen.some((req) => text(req).includes('Previous'))) bad('the outage reached the model as feedback');
const ended = await statusText();
if (!ended.includes('model http 503: Service Unavailable')) bad('outage status: ' + ended);

// Local model mode: the hosted name is not carried over, and a refused origin is explained.
await page.locator('#mode').selectOption('local-model');
const field = await page.locator('#model').inputValue();
if (field !== '') bad('the model field kept ' + field + ' in local mode');
await page.locator('#port').fill(modelPort);
await page.locator('#ask').click();
await page.waitForFunction(() => (document.getElementById('status').textContent || '').includes('Name the model'));
if (localSeen.length !== 0) bad('a request went with no model name');
await page.locator('#mode').selectOption('schema');
const back = await page.locator('#model').inputValue();
if (back !== 'claude-sonnet-4-5') bad('the hosted name did not come back: ' + back);
await page.locator('#mode').selectOption('local-model');
await page.locator('#model').fill('llama3');
const blurb = await page.locator('#blurb').innerText();
if (!blurb.includes('OLLAMA_ORIGINS')) bad('blurb: ' + blurb);
localScript = 'abort';
await page.locator('#ask').click();
if (!(await settle(asked))) bad('the refused ask never finished');
const hint = await statusText();
if (!hint.includes('does not allow this page')) bad('status after a refused fetch: ' + hint);
localScript = null;
await page.locator('#ask').click();
if (!(await settle(asked))) bad('the local ask never finished');
if (!localSeen.some((req) => req.model === 'llama3')) bad('the local request did not name llama3');

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('retry ok');
