import { existsSync } from 'node:fs';

import { chromium } from 'playwright';

const csv = [
  'posted,merchant,amount',
  '2024-01-02,CANARY_MERCHANT_AAA,-100.00',
  '2024-02-03,CANARY_MERCHANT_BBB,-3.50',
  '2024-03-04,CANARY_MERCHANT_CCC,-90.00',
  '2024-04-05,CANARY_MERCHANT_DDD,-1.00',
  '',
].join('\n');

const forbidden = [
  'CANARY_MERCHANT_AAA',
  'CANARY_MERCHANT_BBB',
  'CANARY_MERCHANT_CCC',
  'CANARY_MERCHANT_DDD',
  '-100.00',
  '-3.50',
  '-90.00',
  '-1.00',
  '100.00',
  '2024-01-02',
  '2024-02-03',
  '2024-03-04',
  '2024-04-05',
];

const port = process.argv[2] || '18747';
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

const total = {
  sql: 'SELECT sum(amount) AS total FROM t',
  chart: { kind: 'number', y: 'total' },
  narration: 'The total is {r0.total}.',
};
// Each ask takes the next reply; once they run out the total answers.
const replies = [];
await page.route('https://api.anthropic.com/**', async (route) => {
  const body = route.request().postData() || '';
  bodies.push(body);
  const reply = replies.shift() || total;
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({
  name: 'canary.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('CANARY_MERCHANT_AAA').waitFor({ timeout: 120000 });
await page.locator('#key').fill('sk-ant-test-key');
await page.getByRole('button', { name: 'Largest single debit' }).click();
await page.locator('#confirm').uncheck();
await page.locator('#ask').click();
await page.waitForFunction(
  () => (document.getElementById('narration').textContent || '').includes('-194.50'),
  null,
  { timeout: 120000 },
);

const narration = await page.locator('#narration').innerText();
const sql = await page.locator('#sql').innerText();

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

// In schema mode only a statement that does not plan earns a second request.
async function askWith(label, queued, sends) {
  replies.push(...queued);
  const before = bodies.length;
  await page.locator('#ask').click();
  await page.waitForFunction(() => !document.getElementById('ask').disabled, null, {
    timeout: 120000,
  });
  const got = bodies.length - before;
  if (got !== sends) bad(label + ': ' + got + ' requests, expected ' + sends);
  return { status: await page.locator('#status').innerText(), last: bodies[bodies.length - 1] };
}

const none = await askWith(
  'zero rows',
  [
    {
      sql: "SELECT merchant FROM t WHERE merchant = 'x@y.com'",
      chart: { kind: 'table' },
      narration: 'Here it is.',
    },
  ],
  1,
);
if (!none.status.includes('No rows')) bad('zero rows status: ' + none.status);
const broke = await askWith(
  'failed while reading',
  [
    {
      sql: 'SELECT CASE WHEN amount < -50 THEN CAST(merchant AS INT) END AS n FROM t',
      chart: { kind: 'table' },
      narration: 'Here it is.',
    },
  ],
  1,
);
if (!broke.status) bad('a failure while reading rows showed nothing');
const unplanned = await askWith(
  'does not plan',
  [{ sql: 'SELECT nosuch FROM t', chart: { kind: 'table' }, narration: 'Here it is.' }],
  2,
);
if (!unplanned.last || !unplanned.last.includes('nosuch')) bad('the planner error did not go back');

const log = await page.locator('#log').innerText();
const stored = await page.evaluate(() => ({
  local: window.localStorage.getItem('burrmill-key'),
  session: window.sessionStorage.getItem('burrmill-key'),
}));

if (!narration.includes('-194.50')) bad('narration missing total: ' + narration);
if (!sql.toLowerCase().includes('sum(amount)')) bad('sql missing: ' + sql);
if (!log.includes('bytes')) bad('log has no byte count');
if (!log.includes('https://api.anthropic.com/v1/messages')) bad('log has no url');
if (bodies.length === 0) bad('no outbound body');
if (stored.local) bad('key landed in localStorage');
if (stored.session) bad('key landed in sessionStorage without the box ticked');
if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));

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
console.log('egress ok, bodies ' + bodies.length);
