import { existsSync } from 'node:fs';

import { chromium } from 'playwright';

// A statement that asks about one person must send the model the same thing whichever way the
// answer goes. Each case asks twice, differing only in the guess or in that person's row.

const port = process.argv[2] || '18747';
const cached =
  process.env.HOME +
  '/Library/Caches/ms-playwright/chromium_headless_shell-1243/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const launch = { headless: true };
if (existsSync(cached)) launch.executablePath = cached;
const browser = await chromium.launch(launch);

const people = [
  'merchant,email,amount',
  'KEPT_MERCHANT,canary.one@example.com,-10.00',
  'OTHER_MERCHANT,canary.two@example.com,-3.50',
  '',
].join('\n');

function shops(amount) {
  const lines = ['merchant,amount'];
  for (let n = 0; n < 50; n++) lines.push('SHOP_0' + (n % 4) + ',5.00');
  lines.push('CANARY_X,' + amount);
  return lines.join('\n') + '\n';
}

async function ask(csv, mode, sql) {
  const context = await browser.newContext({ locale: 'en-US' });
  const page = await context.newPage();
  page.on('pageerror', (err) => console.error('pageerror', err.message));
  const bodies = [];
  await page.route('https://api.anthropic.com/**', async (route) => {
    bodies.push(route.request().postData() || '');
    const reply = { sql, chart: { kind: 'table' }, narration: 'Done.' };
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ content: [{ type: 'text', text: JSON.stringify(reply) }] }),
    });
  });
  await page.goto('http://127.0.0.1:' + port + '/', { waitUntil: 'domcontentloaded' });
  await page.locator('#file-input').setInputFiles({
    name: 'file.csv',
    mimeType: 'text/csv',
    buffer: Buffer.from(csv),
  });
  await page.getByText(csv.includes('SHOP_00') ? 'SHOP_00' : 'KEPT_MERCHANT').first().waitFor({ timeout: 120000 });
  await page.locator('#mode').selectOption(mode);
  await page.locator('#key').fill('sk-ant-test-key');
  await page.locator('#question').fill('A question.');
  await page.locator('#confirm').uncheck();
  await page.locator('#ask').click();
  for (let waited = 0; !bodies.length && waited < 120000; waited += 50) await page.waitForTimeout(50);
  await page.waitForFunction(() => !document.getElementById('ask').disabled, null, { timeout: 120000 });
  await context.close();
  return bodies;
}

let failed = false;
function bad(message) {
  failed = true;
  console.error(message);
}

function same(label, a, b, swap = []) {
  const plain = (bodies) =>
    bodies.map((body) => swap.reduce((text, word) => text.split(word).join('GUESS'), body));
  const left = plain(a);
  const right = plain(b);
  if (left.length < 2) bad(label + ': expected a second request, got ' + left.length);
  if (JSON.stringify(left) !== JSON.stringify(right)) {
    bad(label + ': the two answers sent different requests\n' + left.join('\n') + '\n---\n' + right.join('\n'));
  }
}

const right = 'canary.one@example.com';
const wrong = 'nobody.at.all@example.com';

const fails = (guess) => "SELECT CASE WHEN email = '" + guess + "' THEN error('overflow') END AS a FROM t";
same(
  'a refused statement that fails on one person',
  await ask(people, 'pseudonymised', fails(right)),
  await ask(people, 'pseudonymised', fails(wrong)),
  [right, wrong],
);

const literal = (guess) => "SELECT merchant, '" + guess + "' AS x FROM t";
const tokened = await ask(people, 'pseudonymised', literal(right));
same('a literal that guesses a value', tokened, await ask(people, 'pseudonymised', literal(wrong)), [right, wrong]);
if (tokened.some((body) => /CUSTOMER_\d/.test(body))) bad('a literal came back as a token');

await browser.close();
if (failed) process.exit(1);
console.log('guesses ok');
