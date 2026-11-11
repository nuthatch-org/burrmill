import { existsSync } from 'node:fs';

import { chromium } from 'playwright';

// Declining a send leaves the page usable, and a refused key is reported once, not retried.
const csv = ['posted,merchant,amount', '2024-01-02,SHOP,-1.00', ''].join('\n');

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

let requests = 0;
await page.route('https://api.anthropic.com/**', async (route) => {
  requests += 1;
  await route.fulfill({
    status: 401,
    contentType: 'application/json',
    body: JSON.stringify({ type: 'error', error: { type: 'authentication_error', message: 'invalid x-api-key' } }),
  });
});

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}
const statusText = () => page.locator('#status').innerText();

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({ name: 'a.csv', mimeType: 'text/csv', buffer: Buffer.from(csv) });
await page.getByText('SHOP').waitFor({ timeout: 120000 });
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('What is the total?');

if (!(await page.locator('#confirm').isChecked())) bad('confirm is not on by default');
await page.locator('#ask').click();
await page.locator('#decline').waitFor({ state: 'visible', timeout: 120000 });
await page.locator('#decline').click();
await page.waitForFunction(() => !document.getElementById('ask').disabled, null, { timeout: 10000 });
if ((await statusText()) !== 'Not sent.') bad('status after declining: ' + (await statusText()));
if (requests !== 0) bad('declined, yet ' + requests + ' requests went');
if (await page.locator('#send').isVisible()) bad('send button still shown');

await page.locator('#confirm').uncheck();
await page.locator('#ask').click();
await page.waitForFunction(
  () => (document.getElementById('status').textContent || '').includes('invalid x-api-key'),
  null,
  { timeout: 120000 },
);
await page.waitForTimeout(1000);
if (requests !== 1) bad('a 401 was sent ' + requests + ' times');
if (await page.locator('#ask').isDisabled()) bad('ask stayed disabled after a 401');

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('send ok');
