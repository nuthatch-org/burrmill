import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';

const require = createRequire('/Users/pepe/Projects/IRMarket/web/package.json');
const { chromium } = require('playwright');

const csv = ['posted,merchant,amount', '2024-01-02,ada@example.com,-100.00', ''].join('\n');
const columnWarn = 'hiv_status looks like an identifier. Rename it before asking.';
const questionWarn = 'The question looks like an identifier. Rename it before asking.';
const bothWarn = 'hiv_status and the question look like identifiers. Rename them before asking.';
const hostile = 'email<img src=x onerror=alert(1)>';
const hostileWarn = hostile + ' looks like an identifier. Rename it before asking.';

const port = process.argv[2] || '18757';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);
const context = await browser.newContext({ locale: 'en-US' });
const page = await context.newPage();
page.setDefaultTimeout(120000);

const pageErrors = [];
page.on('pageerror', (err) => pageErrors.push(err.message));
page.on('dialog', async (dialog) => {
  bad('dialog ' + dialog.message());
  await dialog.dismiss();
});
const hosts = [];
page.on('request', (req) => {
  let host = '';
  try {
    host = new URL(req.url()).hostname;
  } catch {
    host = req.url();
  }
  if (host !== '127.0.0.1' && host !== 'localhost') hosts.push(req.url());
});

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

async function rename(from, to) {
  const inputs = page.locator('#columns input[type="text"]');
  const n = await inputs.count();
  for (let i = 0; i < n; i++) {
    const input = inputs.nth(i);
    if ((await input.inputValue()) === from) {
      await input.fill(to);
      return;
    }
  }
  bad('missing column ' + from);
}

async function warning() {
  const el = page.locator('#name-warn');
  if (!(await el.isVisible())) return '';
  return (await el.innerText()).trim();
}

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await page.locator('#file-input').setInputFiles({
  name: 'canary.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('ada@example.com').waitFor();
if (await warning()) bad('a cell email warned: ' + (await warning()));
if (await page.locator('#ask').isDisabled()) bad('ask stayed disabled');

await rename('merchant', 'hiv_status');
if ((await warning()) !== columnWarn) bad('column warn: ' + (await warning()));
if (await page.locator('#ask').isDisabled()) bad('warning disabled ask');

await rename('hiv_status', 'c7');
if (await warning()) bad('c7 still warned: ' + (await warning()));
await rename('c7', 'microphone');
if (await warning()) bad('microphone warned: ' + (await warning()));
await rename('microphone', 'phone');
if ((await warning()) !== 'phone looks like an identifier. Rename it before asking.') {
  bad('phone warn: ' + (await warning()));
}
await rename('phone', 'c7');

await page.locator('#question').fill('Show hiv_status');
if ((await warning()) !== questionWarn) bad('question warn: ' + (await warning()));
await page.getByRole('button', { name: 'Monthly spend' }).click();
if (await warning()) bad('demo question warned: ' + (await warning()));
await page.getByRole('button', { name: 'Largest single debit' }).click();
if (await warning()) bad('debit question warned: ' + (await warning()));

await rename('c7', 'hiv_status');
await page.locator('#question').fill('Show hiv_status');
if ((await warning()) !== bothWarn) bad('both warn: ' + (await warning()));

await rename('hiv_status', 'c7');
await page.locator('#question').fill('');
await rename('c7', hostile);
if ((await warning()) !== hostileWarn) bad('hostile warn: ' + (await warning()));
const dom = await page.evaluate(() => {
  const el = document.getElementById('name-warn');
  return { elements: el.children.length, text: el.textContent };
});
if (dom.elements !== 0) bad('warning built an element');
if (!dom.text.includes('<img')) bad('warning dropped the name: ' + dom.text);

const stored = await page.evaluate(() => ({
  local: Object.keys(localStorage),
  session: Object.keys(sessionStorage),
}));
if (stored.local.length) bad('localStorage ' + stored.local.join(','));
if (stored.session.length) bad('sessionStorage ' + stored.session.join(','));
if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));
if (pageErrors.length) bad('pageerror ' + pageErrors.join(' | '));

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('warn ok');
