import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';

const require = createRequire('/Users/pepe/Projects/IRMarket/web/package.json');
const { chromium } = require('playwright');

const lines = ['merchant,amount'];
const forbidden = [];
for (let n = 1; n < 10; n++) {
  const merchant = 'CANARY_SMALL_' + String(n).padStart(2, '0');
  const amount = n + '.01';
  const cents = n * (n * 100 + 1);
  const sum = Math.floor(cents / 100) + '.' + String(cents % 100).padStart(2, '0');
  forbidden.push(merchant, amount);
  if (sum !== amount) forbidden.push(sum);
  for (let i = 0; i < n; i++) lines.push(merchant + ',' + amount);
}
for (let i = 0; i < 10; i++) lines.push('KEPT_GROUP,8.00');
const csv = lines.join('\n') + '\n';

const port = process.argv[2] || '18748';
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
const bodies = [];
const hosts = [];

page.on('request', (req) => {
  let host = '';
  try {
    host = new URL(req.url()).hostname;
  } catch {
    host = req.url();
  }
  if (host !== '127.0.0.1' && host !== 'localhost' && host !== 'api.anthropic.com') {
    hosts.push(req.url());
  }
});

await page.route('https://api.anthropic.com/**', async (route) => {
  const body = route.request().postData() || '';
  bodies.push(body);
  const reply = {
    sql: 'SELECT merchant, sum(amount) AS total FROM t GROUP BY merchant',
    chart: { kind: 'table' },
    narration: 'Kept {r0.merchant}.',
  };
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({
  name: 'groups.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('CANARY_SMALL_01').first().waitFor({ timeout: 120000 });
await page.locator('#mode').selectOption('aggregates');
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('Which merchant groups are large?');
await page.locator('#ask').click();
await page.waitForFunction(
  () =>
    !document.getElementById('answer').hidden &&
    (document.getElementById('log').textContent || '').includes('KEPT_GROUP'),
  null,
  { timeout: 120000 },
);

const log = await page.locator('#log').innerText();
const blurb = await page.locator('#blurb').innerText();
const stored = await page.evaluate(() => ({
  local: window.localStorage.getItem('burrmill-key'),
  session: window.sessionStorage.getItem('burrmill-key'),
}));

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

if (bodies.length < 2) bad('expected two requests, got ' + bodies.length);
if (!blurb.includes('Groups of 10')) bad('blurb: ' + blurb);
if (stored.local) bad('key landed in localStorage');
if (stored.session) bad('key landed in sessionStorage');
if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));
if (bodies[0] && (bodies[0].includes('KEPT_GROUP') || bodies[0].includes('CANARY_SMALL'))) {
  bad('first request already had a group value');
}
const second = bodies[1] || '';
if (!second.includes('\\"dropped\\":9') && !second.includes('"dropped":9')) {
  bad('dropped count missing');
}
if (!second.includes('KEPT_GROUP')) bad('kept group missing');
if (!second.includes('80.00')) bad('kept total missing');
if (!second.includes('\\"k\\":10') && !second.includes('"k":10')) bad('k missing');
for (const body of bodies) {
  for (const word of forbidden) {
    if (body.includes(word)) bad('request body contains ' + word);
  }
  if (body.includes('sk-ant-test-key')) bad('key was in the body');
}
for (const word of forbidden) {
  if (log.includes(word)) bad('log contains ' + word);
}

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('aggregates egress ok, bodies ' + bodies.length);
