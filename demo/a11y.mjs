import { existsSync } from 'node:fs';

import AxeBuilder from '@axe-core/playwright';
import { chromium } from 'playwright';

const csv = [
  'posted,merchant,amount,fee',
  '2024-01-02,Shop A,-100.00,1.00',
  '2024-02-03,Shop B,-3.50,0.50',
  '2024-03-04,Shop C,-90.00,2.00',
  '',
].join('\n');

const port = process.argv[2] || '18760';
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

await page.route('https://api.anthropic.com/**', async (route) => {
  const reply = {
    sql: 'SELECT merchant, -amount AS spent, fee FROM t ORDER BY merchant',
    chart: { kind: 'stacked_bar', x: 'merchant', y: ['spent', 'fee'] },
    narration: 'Spend by merchant.',
  };
  await route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
  });
});

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

async function axe(label) {
  const result = await new AxeBuilder({ page }).analyze();
  for (const v of result.violations) {
    bad(label + ': ' + v.id + ' (' + v.impact + ') ' + v.help);
    for (const node of v.nodes) bad('  ' + node.target.join(' ') + ' ' + node.failureSummary);
  }
}

function luminance(hex) {
  const [r, g, b] = [1, 3, 5].map((i) => {
    const c = parseInt(hex.slice(i, i + 2), 16) / 255;
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function contrast(a, b) {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x);
  return (hi + 0.05) / (lo + 0.05);
}

function hex(rgb) {
  const m = rgb.match(/\d+/g);
  return '#' + m.slice(0, 3).map((n) => Number(n).toString(16).padStart(2, '0')).join('');
}

await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
await axe('empty page');

await page.locator('#file-input').setInputFiles({
  name: 'shops.csv',
  mimeType: 'text/csv',
  buffer: Buffer.from(csv),
});
await page.getByText('Shop A').first().waitFor();
await axe('file open');

await page.locator('#key').fill('sk-ant-test-key');
await page.locator('#question').fill('Spend by merchant');
await page.locator('#confirm').uncheck();
await page.locator('#ask').click();
await page.locator('#chart svg rect').first().waitFor();
await axe('answer');

const live = await page.evaluate(() =>
  ['status', 'log'].map((id) => document.getElementById(id).getAttribute('aria-live')),
);
if (live.some((v) => v !== 'polite')) bad('live regions: ' + live.join(' '));

const chart = await page.evaluate(() => {
  const svg = document.querySelector('#chart svg');
  return {
    title: svg.querySelector('title')?.textContent || '',
    desc: svg.querySelector('desc')?.textContent || '',
    legend: [...svg.querySelectorAll('.legend text')].map((t) => t.textContent),
    fills: [...new Set([...svg.querySelectorAll('rect[data-series]')].map((r) => r.getAttribute('fill')))],
    background: getComputedStyle(svg).backgroundColor,
  };
});
if (!chart.title) bad('chart has no title');
if (!chart.desc.includes('Shop A') || !chart.desc.includes('100.00')) bad('chart values: ' + chart.desc);
if (chart.legend.join(',') !== 'spent,fee') bad('legend: ' + chart.legend.join(','));
if (chart.fills.length !== 2) bad('series fills: ' + chart.fills.join(' '));
for (const fill of chart.fills) {
  const ratio = contrast(fill, hex(chart.background));
  if (ratio < 3) bad('series ' + fill + ' is ' + ratio.toFixed(2) + ':1 on the chart');
}

await context.close();
await browser.close();
if (failed) process.exit(1);
console.log('a11y ok');
