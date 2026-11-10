import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';

const require = createRequire('/Users/pepe/Projects/IRMarket/web/package.json');
const { chromium } = require('playwright');

const csv = [
  'merchant,email,iban,phone,amount',
  'KEPT_MERCHANT,canary.one@example.com,GB82WEST12345698765432,+359 888 100001,-10.00',
  'OTHER_MERCHANT,canary.two@example.com,DE89370400440532013000,+359 888 100002,-3.50',
  '',
].join('\n');

const forbidden = [
  'canary.one@example.com',
  'canary.two@example.com',
  'GB82WEST12345698765432',
  'DE89370400440532013000',
  '+359 888 100001',
  '+359 888 100002',
];

const port = process.argv[2] || '18753';
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
    sql: 'SELECT merchant, email, iban, phone, sum(amount) AS total FROM t GROUP BY merchant, email, iban, phone',
    chart: { kind: 'table' },
    narration: 'Person CUSTOMER_1.',
  };
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({
  name: 'people.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('KEPT_MERCHANT').first().waitFor({ timeout: 120000 });
await page.locator('#mode').selectOption('pseudonymised');
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('Who is each merchant?');
await page.locator('#ask').click();
await page.waitForFunction(
  () =>
    !document.getElementById('answer').hidden &&
    (document.getElementById('narration').textContent || '').includes('canary.one@example.com'),
  null,
  { timeout: 120000 },
);

const log = await page.locator('#log').innerText();
const narration = await page.locator('#narration').innerText();
const blurb = await page.locator('#blurb').innerText();
const checked = await page.locator('#columns input.identifier:checked').count();
const signOn = await page.locator('#columns input.sign:checked').count();
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
if (checked !== 3) bad('identifier boxes checked ' + checked);
if (signOn !== 0) bad('sign was checked');
if (!blurb.includes('tokens')) bad('blurb: ' + blurb);
if (stored.local) bad('key landed in localStorage');
if (stored.session) bad('key landed in sessionStorage');
if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));
if (!narration.includes('canary.one@example.com')) bad('narration: ' + narration);
const first = bodies[0] || '';
if (first.includes('CUSTOMER_1')) bad('first request already had a token');
for (const word of forbidden) {
  if (first.includes(word)) bad('first request contains ' + word);
}
const second = bodies[1] || '';
if (!second.includes('CUSTOMER_1')) bad('token missing from the second request');
if (!second.includes('KEPT_MERCHANT') || !second.includes('OTHER_MERCHANT')) {
  bad('merchant missing from the second request');
}
for (const body of bodies) {
  for (const word of forbidden) {
    if (body.includes(word) || body.includes(word.replaceAll('@', '\\u0040'))) {
      bad('request body contains ' + word);
    }
  }
  if (body.includes('sk-ant-test-key')) bad('key was in the body');
}
for (const word of forbidden) {
  if (log.includes(word)) bad('log contains ' + word);
}

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('pseudonym egress ok, bodies ' + bodies.length);
