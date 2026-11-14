import { existsSync } from 'node:fs';

import { chromium } from 'playwright';

// A visitor with no key and no local model can still type a statement and run it in the tab.
const csv = [
  'posted,merchant,amount',
  '2024-01-02,alpha,-100.00',
  '2024-01-03,beta,-25.50',
  '2024-01-04,alpha,-10.00',
  '',
].join('\n');

const port = process.argv[2] || '18763';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);
const context = await browser.newContext({ locale: 'en-US' });
const page = await context.newPage();
page.setDefaultTimeout(120000);
page.on('pageerror', (err) => console.error('pageerror', err.message));

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

const outbound = [];
page.on('request', (req) => {
  const host = new URL(req.url()).hostname;
  if (host !== '127.0.0.1' && host !== 'localhost') outbound.push(req.url());
});

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
if (!(await page.locator('#answer').isHidden())) bad('the SQL box shows before a file is open');

await page.locator('#file-input').setInputFiles({
  name: 'tx.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.locator('#preview').getByText('beta').waitFor();
await page.locator('#run-here').waitFor({ state: 'visible' });
const offered = await page.locator('#sql').inputValue();
if (!/\bFROM t\b/.test(offered)) bad('no starting statement over t: ' + offered);

await page
  .locator('#sql')
  .fill('SELECT merchant, count(*) AS n, sum(amount) AS total FROM t GROUP BY merchant ORDER BY merchant');
await page.locator('#run-here').click();
await page.waitForFunction(() => !document.getElementById('run-here').disabled && document.getElementById('status').textContent);
const said = await page.locator('#status').textContent();
if (said !== 'Ran here. Nothing was sent.') bad('status: ' + said);
const cells = await page.locator('#table td').allTextContents();
if (cells.join('|') !== 'alpha|2|-110.00|beta|1|-25.50') bad('answer: ' + cells.join('|'));

if (outbound.length) bad('requests left the machine: ' + outbound.join(', '));

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('nomodel ok');
