import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';

const require = createRequire('/Users/pepe/Projects/IRMarket/web/package.json');
const { chromium } = require('playwright');

const csv = [
  'posted,merchant,amount',
  '2024-01-02,CANARY_MERCHANT_AAA,-100.00',
  '2024-02-03,CANARY_MERCHANT_BBB,-3.50',
  '',
].join('\n');

const port = process.argv[2] || '18754';
const modelPort = '18799';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);
const context = await browser.newContext({ locale: 'en-US' });
const page = await context.newPage();
page.on('pageerror', (err) => console.error('pageerror', err.message));
page.on('console', (msg) => {
  if (msg.type() === 'error') console.error('console', msg.text());
});
const calls = [];
const hosts = [];

page.on('request', (req) => {
  let parsed;
  try {
    parsed = new URL(req.url());
  } catch {
    hosts.push(req.url());
    return;
  }
  if (parsed.hostname !== '127.0.0.1' && parsed.hostname !== 'localhost') {
    hosts.push(req.url());
  }
  if (parsed.pathname === '/v1/messages' || parsed.hostname === 'api.anthropic.com') {
    calls.push({
      url: req.url(),
      body: req.postData() || '',
      key: req.headers()['x-api-key'] || '',
    });
  }
});

await page.route('http://127.0.0.1:' + modelPort + '/**', async (route) => {
  const reply = {
    sql: 'SELECT sum(amount) AS total FROM t',
    chart: { kind: 'number', y: 'total' },
    narration: 'The total is {r0.total}.',
  };
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});
await page.route('https://api.anthropic.com/**', async (route) => {
  calls.push({ url: route.request().url(), body: route.request().postData() || '', key: 'anthropic' });
  await route.abort();
});

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({
  name: 'canary.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('CANARY_MERCHANT_AAA').waitFor({ timeout: 120000 });
await page.locator('#mode').selectOption('local-model');
await page.locator('#port').fill(modelPort);
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('Total');
await page.locator('#ask').click();
await page.waitForFunction(
  () => (document.getElementById('narration').textContent || '').includes('-103.50'),
  null,
  { timeout: 120000 },
);

const narration = await page.locator('#narration').innerText();
const log = await page.locator('#log').innerText();
const blurb = await page.locator('#blurb').innerText();
const csp = await page.locator('meta[http-equiv="Content-Security-Policy"]').getAttribute('content');
const stored = await page.evaluate(() => ({
  local: window.localStorage.getItem('burrmill-key'),
  session: window.sessionStorage.getItem('burrmill-key'),
}));

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

const expected = 'http://127.0.0.1:' + modelPort + '/v1/messages';
if (calls.length !== 1) bad('expected one model call, got ' + calls.length + ' ' + calls.map((c) => c.url).join(' '));
if (calls[0] && calls[0].url !== expected) bad('url ' + (calls[0] && calls[0].url));
if (calls[0] && !calls[0].body.includes('CANARY_MERCHANT_AAA')) bad('rows were not in the body');
if (calls[0] && calls[0].body.includes('sk-ant-test-key')) bad('key was in the body');
if (calls[0] && calls[0].key) bad('key header was sent');
if (!narration.includes('-103.50')) bad('narration: ' + narration);
if (!log.includes(expected)) bad('log url: ' + log.slice(0, 120));
if (!log.includes('CANARY_MERCHANT_AAA')) bad('log did not show the rows');
if (log.includes('api.anthropic.com')) bad('log named anthropic');
if (!blurb.includes('127.0.0.1')) bad('blurb: ' + blurb);
if (!csp.includes('http://127.0.0.1:*')) bad('csp: ' + csp);
if (csp.includes('api.openai.com')) bad('csp names another provider');
if (stored.local) bad('key landed in localStorage');
if (stored.session) bad('key landed in sessionStorage');
if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('local egress ok, calls ' + calls.length);
