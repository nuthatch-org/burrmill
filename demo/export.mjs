import { existsSync, readFileSync } from 'node:fs';

import { chromium } from 'playwright';

// Wide enough that the table cannot fit 360 px without its own scroll.
const csv = [
  'posted,merchant_name_that_is_long,amount,category_with_a_long_name,note',
  '2024-01-02,CANARY_MERCHANT_AAA_WITH_A_LONG_NAME,-100.00,groceries_and_household,first',
  '2024-02-03,CANARY_MERCHANT_BBB_WITH_A_LONG_NAME,-3.50,"coffee, tea",second',
  '2024-03-04,CANARY_MERCHANT_CCC_WITH_A_LONG_NAME,-90.00,rent_and_utilities,third',
  '',
].join('\n');

const port = process.argv[2] || '18761';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);
const context = await browser.newContext({
  locale: 'en-US',
  viewport: { width: 360, height: 740 },
  acceptDownloads: true,
});
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
await page.route('https://api.anthropic.com/**', async (route) => {
  const reply = {
    sql: 'SELECT * FROM t ORDER BY posted',
    chart: { kind: 'table' },
    narration: 'Every row.',
  };
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});

async function pageScroll(label) {
  const width = await page.evaluate(() => ({
    scroll: document.documentElement.scrollWidth,
    client: document.documentElement.clientWidth,
  }));
  if (width.scroll > width.client) bad(label + ': page scrolls sideways, ' + width.scroll + ' > ' + width.client);
}

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await pageScroll('empty page');
await page.locator('#file-input').setInputFiles({
  name: 'wide.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.locator('#preview').getByText('CANARY_MERCHANT_AAA_WITH_A_LONG_NAME').waitFor();
await pageScroll('file open');

const model = await page.locator('#model').inputValue();
if (!model.startsWith('claude-')) bad('default model: ' + model);

await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('Every row');
await page.locator('#confirm').uncheck();
await page.locator('#ask').click();
await page.locator('#table').getByText('CANARY_MERCHANT_CCC_WITH_A_LONG_NAME').waitFor();
await pageScroll('answer');
const tableScrolls = await page.evaluate(() => {
  const host = document.getElementById('table');
  return host.scrollWidth > host.clientWidth && getComputedStyle(host).overflowX === 'auto';
});
if (!tableScrolls) bad('the result table does not scroll on its own');
if (outbound.length !== 1) bad('expected one model request, got ' + outbound.length);

const [download] = await Promise.all([
  page.waitForEvent('download'),
  page.locator('#export').click(),
]);
const text = readFileSync(await download.path(), 'utf8');
const lines = text.split('\r\n');
if (lines[0] !== 'posted,merchant_name_that_is_long,amount,category_with_a_long_name,note') {
  bad('csv header: ' + lines[0]);
}
if (lines[1] !== '2024-01-02,CANARY_MERCHANT_AAA_WITH_A_LONG_NAME,-100.00,groceries_and_household,first') {
  bad('csv row: ' + lines[1]);
}
if (lines[2] !== '2024-02-03,CANARY_MERCHANT_BBB_WITH_A_LONG_NAME,-3.50,"coffee, tea",second') {
  bad('csv quoting: ' + lines[2]);
}
if (lines.length !== 5 || lines[4] !== '') bad('csv rows: ' + lines.length);

await page
  .locator('#sql')
  .fill("SELECT merchant_name_that_is_long AS m, '=1+1' AS f FROM t WHERE amount < -50 ORDER BY m");
await page.locator('#run-here').click();
await page.locator('#table').getByText('=1+1').first().waitFor();
const rows = await page.locator('#table tbody tr').count();
if (rows !== 2) bad('re-run rows: ' + rows);
if (outbound.length !== 1) bad('running here sent ' + (outbound.length - 1) + ' requests');
const status = await page.locator('#status').innerText();
if (!status.includes('Nothing was sent')) bad('status: ' + status);

const [again] = await Promise.all([page.waitForEvent('download'), page.locator('#export').click()]);
const rerun = readFileSync(await again.path(), 'utf8').split('\r\n');
if (rerun[0] !== 'm,f' || rerun[1] !== "CANARY_MERCHANT_AAA_WITH_A_LONG_NAME,'=1+1") {
  bad('re-run csv: ' + rerun.slice(0, 2).join(' | '));
}
await pageScroll('re-run');

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('export ok');
