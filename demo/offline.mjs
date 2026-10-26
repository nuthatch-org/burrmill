import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';

const require = createRequire('/Users/pepe/Projects/IRMarket/web/package.json');
const { chromium } = require('playwright');

const csv = ['posted,merchant,amount', '2024-01-02,CANARY_MERCHANT_AAA,-100.00', ''].join('\n');

const port = process.argv[2] || '18756';
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
  if (host !== '127.0.0.1' && host !== 'localhost' && host !== 'api.anthropic.com') {
    hosts.push(req.url());
  }
});
const failedModel = [];
page.on('requestfailed', (req) => {
  if (req.url().includes('api.anthropic.com')) failedModel.push(req.url());
});

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

function pngSize(bytes) {
  if (bytes.length < 24 || bytes[0] !== 0x89) return '';
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return view.getUint32(16) + 'x' + view.getUint32(20);
}

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
try {
  await page.waitForFunction(async () => {
    const reg = await navigator.serviceWorker.ready;
    if (!reg.active || !navigator.serviceWorker.controller) return false;
    const cache = await caches.open('burrmill-shell-1');
    const keys = await cache.keys();
    return keys.some((key) => key.url.endsWith('burrmill_wasm_bg.wasm'));
  });
} catch (err) {
  const info = await page.evaluate(async () => {
    const reg = await navigator.serviceWorker.getRegistration();
    return {
      status: document.getElementById('status').textContent,
      controller: !!navigator.serviceWorker.controller,
      state: reg && (reg.active || reg.installing || reg.waiting)
        ? (reg.active || reg.installing || reg.waiting).state
        : 'none',
    };
  });
  console.error(JSON.stringify(info));
  throw err;
}

const shell = await page.evaluate(async () => {
  const link = document.querySelector('link[rel="manifest"]');
  const manifestRes = await fetch(link.href);
  const manifest = await manifestRes.json();
  const icons = [];
  for (const icon of manifest.icons) {
    const res = await fetch(new URL(icon.src, link.href));
    const bytes = new Uint8Array(await res.arrayBuffer());
    icons.push({
      src: icon.src,
      sizes: icon.sizes,
      type: res.headers.get('content-type'),
      bytes: Array.from(bytes.slice(0, 24)),
    });
  }
  return {
    manifestType: manifestRes.headers.get('content-type'),
    manifest,
    icons,
  };
});
if (!shell.manifestType || !shell.manifestType.includes('manifest+json')) {
  bad('manifest type: ' + shell.manifestType);
}
if (shell.manifest.display !== 'standalone') bad('display: ' + shell.manifest.display);
if (shell.manifest.start_url !== './') bad('start_url: ' + shell.manifest.start_url);
const declared = (shell.manifest.icons || []).map((icon) => icon.sizes).sort().join(',');
if (declared !== '192x192,512x512') bad('icon sizes: ' + declared);

for (const icon of shell.icons) {
  const bytes = new Uint8Array(icon.bytes);
  const size = pngSize(bytes);
  if (icon.type !== 'image/png') bad(icon.src + ' type ' + icon.type);
  if (size !== icon.sizes) bad(icon.src + ' decoded ' + size);
}

let installErrors = null;
try {
  const client = await context.newCDPSession(page);
  const result = await client.send('Page.getInstallabilityErrors');
  installErrors = result.installabilityErrors || [];
} catch (err) {
  bad('installability api: ' + err.message);
}
if (installErrors && installErrors.length) {
  bad('not installable: ' + JSON.stringify(installErrors));
}

await context.setOffline(true);
await page.reload({ waitUntil: 'domcontentloaded' });
await page.waitForFunction(() => document.documentElement.dataset.store === 'ready');
const previewBefore = (await page.locator('#preview').textContent()) || '';
if (previewBefore.includes('CANARY_MERCHANT_AAA')) bad('offline reload kept an open file');

await page.locator('#file-input').setInputFiles({
  name: 'canary.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('CANARY_MERCHANT_AAA').waitFor();

const narration = await page.evaluate(async () => {
  return globalThis.burrmillLocal(
    'SELECT sum(amount) AS total FROM t',
    { kind: 'number', y: ['total'] },
    'The total is {r0.total}.',
  );
});
if (narration !== 'The total is -100.00.') bad('narration: ' + narration);
const chart = await page.locator('#chart').innerText();
if (!chart.includes('-100.00')) bad('chart: ' + chart);
const sql = await page.locator('#sql').innerText();
if (!sql.toLowerCase().includes('sum(amount)')) bad('sql: ' + sql);
if ((await page.locator('#chart svg').count()) !== 1) bad('chart did not draw');

await page.locator('#key').fill('sk-ant-test-key');
await page.getByRole('button', { name: 'Largest single debit' }).click();
await page.locator('#ask').click();
await page.waitForFunction(() => {
  const text = document.getElementById('status').textContent || '';
  return /failed|network|offline|disconnected|fetch/i.test(text);
});
const after = await page.locator('#narration').innerText();
if (after !== 'The total is -100.00.') bad('model call replaced the chart narration: ' + after);
const status = await page.locator('#status').innerText();
const log = await page.locator('#log').innerText();
if (!log.includes('https://api.anthropic.com/v1/messages')) bad('model call was not attempted: ' + status);
if (log.includes('CANARY_MERCHANT_AAA') || log.includes('-100.00')) bad('schema log leaked a cell');
if (!failedModel.length) bad('model call did not fail: ' + status);
if (hosts.length) bad('unexpected hosts ' + hosts.join(' '));
if (pageErrors.length) bad('pageerror ' + pageErrors.join(' | '));

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('offline ok, model failed ' + failedModel.length);
