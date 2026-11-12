import { existsSync } from 'node:fs';

import { chromium } from 'playwright';

// A statement that will not finish can be stopped, or is stopped at the deadline, and the
// page keeps its file: the next question runs in a new worker.
const rows = ['posted,merchant,amount'];
for (let i = 0; i < 3000; i++) rows.push('2024-01-02,SHOP' + (i % 7) + ',-1.50');
const csv = rows.join('\n') + '\n';

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

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

const reply = (sql) => ({
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify({
    content: [
      {
        type: 'text',
        text: JSON.stringify({
          sql,
          chart: { kind: 'number', y: 'total' },
          narration: 'The total is {r0.total}.',
        }),
      },
    ],
  }),
});
let sql = 'SELECT count(*) AS total FROM t a, t b, t c';
await page.route('https://api.anthropic.com/**', (route) => route.fulfill(reply(sql)));

await page.clock.install();
await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({ name: 'a.csv', mimeType: 'text/csv', buffer: Buffer.from(csv) });
await page.getByText('SHOP0').first().waitFor({ timeout: 120000 });
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('How many triples?');
await page.locator('#confirm').uncheck();
const statusText = () => page.locator('#status').innerText();

// Stop by hand.
await page.locator('#ask').click();
await page.locator('#stop').waitFor({ state: 'visible', timeout: 120000 });
await page.locator('#stop').click();
await page.waitForFunction(() => !document.getElementById('ask').disabled, null, { timeout: 30000 });
if ((await statusText()) !== 'Stopped.') bad('status after Stop: ' + (await statusText()));
if (await page.locator('#stop').isVisible()) bad('Stop is still shown');

// The deadline.
await page.locator('#ask').click();
await page.locator('#stop').waitFor({ state: 'visible', timeout: 120000 });
await page.clock.fastForward(61000);
await page.waitForFunction(() => !document.getElementById('ask').disabled, null, { timeout: 30000 });
if (!(await statusText()).includes('ran past 60 s')) bad('status at the deadline: ' + (await statusText()));

// The held file was opened again in the new worker.
sql = 'SELECT sum(amount) AS total FROM t';
await page.locator('#ask').click();
await page.waitForFunction(
  () => (document.getElementById('narration').textContent || '').includes('-4,500.00'),
  null,
  { timeout: 120000 },
);

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('stop ok');
