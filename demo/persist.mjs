import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';

const require = createRequire('/Users/pepe/Projects/IRMarket/web/package.json');
const { chromium } = require('playwright');

const csv = ['posted,merchant,amount', '2024-01-02,CANARY_MERCHANT_AAA,-100.00', ''].join('\n');

const port = process.argv[2] || '18755';
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

async function ready() {
  await page.waitForFunction(() => document.documentElement.dataset.store === 'ready');
}

async function dropped() {
  await page.locator('#file-input').setInputFiles({
    name: 'canary.csv',
    mimeType: 'text/csv',
    buffer: Buffer.from(csv),
  });
  await page.getByText('CANARY_MERCHANT_AAA').waitFor();
}

async function preview() {
  return (await page.locator('#preview').textContent()) || '';
}

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await ready();
if (await page.locator('#keep').isChecked()) bad('keep starts checked');
if (await page.locator('#stored').isVisible()) bad('indicator starts visible');

await dropped();
if (await page.locator('#keep').isChecked()) bad('drop opted in');
if (await page.locator('#stored').isVisible()) bad('drop stored the file');

await page.reload({ waitUntil: 'domcontentloaded' });
await ready();
if (await page.locator('#setup').isVisible()) bad('unkept file restored the preview');
if (await page.locator('#stored').isVisible()) bad('unkept file left the indicator');
if ((await preview()).includes('CANARY_MERCHANT_AAA')) bad('reload without opt-in still shows the file');

await dropped();
await page.locator('#keep').check();
await page.waitForFunction(
  () =>
    (document.getElementById('stored').textContent || '') ===
    'canary.csv is stored on this machine.',
);

const saved = await page.evaluate(async () => {
  const root = await navigator.storage.getDirectory();
  const dir = await root.getDirectoryHandle('burrmill');
  const names = [];
  for await (const [name] of dir.entries()) names.push(name);
  const bin = await (await dir.getFileHandle('upload.bin')).getFile();
  const name = await (await dir.getFileHandle('upload.name')).getFile();
  const dump = (store) => Object.keys(store).map((key) => key + '=' + store.getItem(key));
  return {
    names,
    text: await bin.text(),
    name: await name.text(),
    local: dump(window.localStorage),
    session: dump(window.sessionStorage),
  };
});
if (saved.text !== csv) bad('stored bytes differ: ' + JSON.stringify(saved.text));
if (saved.name !== 'canary.csv') bad('stored name: ' + JSON.stringify(saved.name));
if (saved.names.slice().sort().join(',') !== 'upload.bin,upload.name') {
  bad('stored entries: ' + saved.names.join(','));
}
if (saved.local.length) bad('localStorage: ' + saved.local.join(','));
if (saved.session.length) bad('sessionStorage: ' + saved.session.join(','));

await page.reload({ waitUntil: 'domcontentloaded' });
await ready();
await page.getByText('CANARY_MERCHANT_AAA').waitFor();
if (!(await page.locator('#keep').isChecked())) bad('restore left keep unchecked');
const indicator = await page.locator('#stored').innerText();
if (indicator !== 'canary.csv is stored on this machine.') bad('indicator: ' + indicator);

await page.locator('#keep').uncheck();
await page.waitForFunction(() => document.getElementById('stored').hidden);
if (await page.locator('#keep').isChecked()) bad('uncheck left the box ticked');
if (!(await preview()).includes('CANARY_MERCHANT_AAA')) bad('uncheck dropped the open table');

await page.reload({ waitUntil: 'domcontentloaded' });
await ready();
if (await page.locator('#setup').isVisible()) bad('uncheck restored the preview');
if (await page.locator('#stored').isVisible()) bad('uncheck left the indicator');
if ((await preview()).includes('CANARY_MERCHANT_AAA')) bad('reload after uncheck still shows the file');

await dropped();
await page.locator('#keep').check();
await page.waitForFunction(() => !document.getElementById('stored').hidden);
await page.locator('#clear-all').click();
await page.waitForFunction(() => {
  const stored = document.getElementById('stored');
  return stored.hidden && !document.getElementById('keep').checked;
});
if (!(await preview()).includes('CANARY_MERCHANT_AAA')) bad('clear-all dropped the open table');
const opfs = await page.evaluate(async () => {
  const root = await navigator.storage.getDirectory();
  try {
    await root.getDirectoryHandle('burrmill');
    return 'present';
  } catch (err) {
    return err.name;
  }
});
if (opfs !== 'NotFoundError') bad('clear-all left the directory: ' + opfs);

await page.reload({ waitUntil: 'domcontentloaded' });
await ready();
if (await page.locator('#setup').isVisible()) bad('clear-all restored the preview');
if (await page.locator('#keep').isChecked()) bad('clear-all restore checked keep');
if (await page.locator('#stored').isVisible()) bad('clear-all restore showed the indicator');
if ((await preview()).includes('CANARY_MERCHANT_AAA')) bad('reload after clear-all still shows the file');

if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));
if (pageErrors.length) bad('pageerror ' + pageErrors.join(' | '));

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('persist ok');
