import { existsSync, readFileSync } from 'node:fs';

import { chromium } from 'playwright';

// The static server sends no headers, so the routes add what _headers says a host would.
const csv = ['posted,merchant,amount', '2024-01-02,CANARY_MERCHANT_AAA,-100.00', ''].join('\n');

const port = process.argv[2] || '18762';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

const headers = {};
let inBlock = false;
for (const line of readFileSync(new URL('./_headers', import.meta.url), 'utf8').split('\n')) {
  if (line.startsWith('#') || !line.trim()) continue;
  if (!line.startsWith(' ')) {
    inBlock = line.trim() === '/*';
    continue;
  }
  if (!inBlock) continue;
  const at = line.indexOf(':');
  headers[line.slice(0, at).trim().toLowerCase()] = line.slice(at + 1).trim();
}
const policy = headers['content-security-policy'] || '';
const meta = readFileSync(new URL('./index.html', import.meta.url), 'utf8').match(
  /<meta http-equiv="Content-Security-Policy" content="([^"]*)">/,
);
if (!meta) bad('index.html has no meta policy');
else if (policy !== meta[1] + "; frame-ancestors 'none'") {
  bad('the header policy is not the meta policy and frame-ancestors:\n' + policy + '\n' + meta[1]);
}
for (const want of ["require-trusted-types-for 'script'", "frame-ancestors 'none'"]) {
  if (!policy.includes(want)) bad('header policy lacks ' + want);
}

const browser = await chromium.launch(launch);
// A service worker would answer from its cache and skip the routes.
const context = await browser.newContext({ locale: 'en-US', serviceWorkers: 'block' });
// Not the engine: a hundred megabytes through the route is slow, and a module carries no policy.
await context.route('http://127.0.0.1:' + port + '/**', async (route) => {
  if (route.request().url().endsWith('.wasm')) return route.continue();
  const response = await route.fetch();
  await route.fulfill({ response, headers: { ...response.headers(), ...headers } });
});
await context.route('https://api.anthropic.com/**', async (route) => {
  const reply = {
    sql: 'SELECT sum(amount) AS total FROM t',
    chart: { kind: 'number', y: 'total' },
    narration: 'The total is {r0.total}.',
  };
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});
await context.addInitScript(() => {
  window.violations = [];
  document.addEventListener('securitypolicyviolation', (event) => {
    window.violations.push(event.violatedDirective + ' ' + event.blockedURI);
  });
});

const page = await context.newPage();
page.setDefaultTimeout(120000);
page.on('pageerror', (err) => bad('pageerror ' + err.message));
const main = await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
if (!(main.headers()['content-security-policy'] || '').includes('frame-ancestors')) {
  bad('the route did not add the policy');
}
await page.locator('#file-input').setInputFiles({
  name: 'canary.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('CANARY_MERCHANT_AAA').waitFor();
await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('Total');
await page.locator('#confirm').uncheck();
await page.locator('#ask').click();
await page.waitForFunction(() =>
  (document.getElementById('narration').textContent || '').includes('-100.00'),
);
const violations = await page.evaluate(() => window.violations);
if (violations.length) bad('policy violations: ' + violations.join(' | '));
const refused = await page.evaluate(() => {
  try {
    document.getElementById('narration').innerHTML = '<b>x</b>';
    return false;
  } catch {
    return true;
  }
});
if (!refused) bad('an html sink was not refused');

// Another origin may not frame the page.
const outer = await context.newPage();
await outer.goto('http://localhost:' + port + '/manifest.webmanifest');
await outer.evaluate((src) => {
  const frame = document.createElement('iframe');
  frame.src = src;
  document.body.appendChild(frame);
}, 'http://127.0.0.1:' + port + '/');
await outer.waitForTimeout(2000);
const framed = outer.frames().find((f) => f !== outer.mainFrame());
const framedAsk = framed ? await framed.locator('#ask').count().catch(() => 0) : 0;
if (framedAsk) bad('the page rendered inside a frame from another origin');

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('headers ok');
