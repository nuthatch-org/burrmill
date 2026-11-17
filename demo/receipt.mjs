import { existsSync, readFileSync } from 'node:fs';

import { chromium } from 'playwright';

// The tab, built for wasm32, makes the receipt a native engine made for the same CSV, byte for
// byte, and verifies it; an edited one it refuses. receipt.json is written by a native test in
// crates/burrmill-wasm (the_demo_receipt_is_what_a_native_engine_makes).
const fixture = JSON.parse(readFileSync(new URL('./receipt.json', import.meta.url)));

const port = process.argv[2] || '18747';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);
const page = await browser.newPage();
page.on('pageerror', (err) => console.error('pageerror', err.message));
await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });

const out = await page.evaluate(async ({ csv, sql, receipt }) => {
  const m = await import('./pkg/burrmill_wasm.js');
  await m.default();
  const s = m.WasmSession.open(new TextEncoder().encode(csv));
  const made = await s.receipt(sql);
  let verified = 'yes';
  try {
    await s.verify_receipt(receipt);
  } catch (e) {
    verified = String(e);
  }
  let edited = 'verified';
  try {
    await s.verify_receipt(receipt.replace('SUM(amount)', 'MAX(amount)'));
  } catch (e) {
    edited = String(e);
  }
  return { made, verified, edited };
}, fixture);
await browser.close();

let failed = false;
if (out.made !== fixture.receipt) {
  failed = true;
  console.error('the tab made another receipt:\n' + out.made + '\nnot\n' + fixture.receipt);
}
if (out.verified !== 'yes') {
  failed = true;
  console.error('the native receipt did not verify in the tab: ' + out.verified);
}
if (!out.edited.includes('receipt does not verify')) {
  failed = true;
  console.error('an edited receipt was not refused: ' + out.edited);
}
if (failed) process.exit(1);
console.log('receipt: the tab and a native engine agree');
