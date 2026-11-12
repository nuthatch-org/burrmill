const QUESTIONS = [
  'Monthly spend',
  'Subscriptions',
  'Top merchants',
  'Largest single debit',
  'Year-over-year change',
];

const DEFAULT_MODEL = 'claude-sonnet-4-5';

// For the prompt and the blurb. The filter is AGGREGATE_K in crates/burrmill-wasm/src/aggregates.rs.
const AGGREGATE_K = 10;

// Each ask in aggregates mode is a set of groups, and differencing two of them can isolate a
// small one. A fixed number per page load bounds that; it does not prevent it.
const AGGREGATE_ASKS = 20;
let aggregateAsks = 0;

// Set by the first local-model ask, after which this document cannot reach another host.
let heldLocal = false;

// A label check only, and a warning: it does not stop a send (docs/browser.md).
const IDENTIFIER_LABELS = new Set(['email', 'phone', 'iban', 'ssn', 'patient', 'hiv', 'id']);

const SVG_NS = 'http://www.w3.org/2000/svg';
// Each at least 3:1 on the chart's #12100e.
const FILLS = ['#d46a30', '#6a8ca3', '#a77a42', '#7d8f5e'];

if (window.trustedTypes && window.trustedTypes.createPolicy) {
  window.trustedTypes.createPolicy('default', {
    createHTML() {
      throw new Error('html refused');
    },
    createScript() {
      throw new Error('script refused');
    },
    createScriptURL(url) {
      const resolved = new URL(url, window.location.href);
      const own = resolved.origin === window.location.origin;
      const path = resolved.pathname;
      if (own && (path.endsWith('.wasm') || path.endsWith('/worker.js') || path.endsWith('/sw.js'))) {
        return resolved.href;
      }
      throw new Error('script url refused');
    },
  });
}

let seq = 0;
const pending = new Map();
const fields = [];
let fileOpen = false;
// The buffer posted to the worker is detached. This copy is what a later tick can store.
let held = null;
let heldName = '';

// A statement that runs past this is stopped. A join does not yield, so stopping one means
// ending the worker and opening the held file in a new one.
const QUERY_MS = 60000;
const QUERY_OPS = new Set(['query', 'aggregates', 'pseudonym']);
let worker = spawn();
let running = 0;
let renames = [];
let restoring = Promise.resolve();

function spawn() {
  const next = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
  next.onmessage = (event) => {
    const msg = event.data;
    const wait = pending.get(msg.id);
    if (!wait) return;
    pending.delete(msg.id);
    if (msg.ok) wait.resolve(msg.result);
    else wait.reject(new Error(msg.error || 'worker failed'));
  };
  return next;
}

function dispatch(op, extra, transfer) {
  const id = ++seq;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage(Object.assign({ id, op }, extra || {}), transfer || []);
  });
}

async function restore() {
  if (!held) return;
  const bytes = await held.arrayBuffer();
  await dispatch('open', { bytes }, [bytes]);
  for (const step of renames) await dispatch('rename', step);
}

function restart(reason) {
  worker.terminate();
  for (const wait of pending.values()) wait.reject(new Error(reason));
  pending.clear();
  worker = spawn();
  restoring = restore().catch((err) => status(err.message || String(err)));
}

async function call(op, extra, transfer) {
  await restoring;
  if (!QUERY_OPS.has(op)) return dispatch(op, extra, transfer);
  running++;
  document.getElementById('stop').hidden = false;
  const timer = setTimeout(
    () => restart('The query ran past ' + QUERY_MS / 1000 + ' s and was stopped. Ask for less.'),
    QUERY_MS,
  );
  try {
    return await dispatch(op, extra, transfer);
  } finally {
    clearTimeout(timer);
    if (--running === 0) document.getElementById('stop').hidden = true;
  }
}

function status(text) {
  document.getElementById('status').textContent = text || '';
}

function separators() {
  const parts = new Intl.NumberFormat(navigator.language).formatToParts(10000.1);
  return {
    group: parts.find((part) => part.type === 'group')?.value ?? ',',
    decimal: parts.find((part) => part.type === 'decimal')?.value ?? '.',
  };
}

function promptText(question, card, grammar, feedback, previousSql, mode, identifiers, rows) {
  const lines = [
    'Return one JSON object and no other text.',
    'Keys are sql, chart, and narration. No other keys.',
    'sql is one SELECT or WITH. The only uploaded table is t.',
    'chart.kind is number, bar, line, stacked_bar, or table.',
    'chart.y is a column name or an array of column names. chart.x is a column name.',
    'narration is one sentence. A slot is {rN.column}, N counting rows from 0, or {column} for the first row.',
    'A digit outside a slot is refused. Do not write a number except inside a slot.',
    'The schema below has no cell values. Do not invent values.',
  ];
  if (mode === 'aggregates') {
    lines.push(
      'Aggregates mode, k = ' + AGGREGATE_K + '.',
      'Write one SELECT that reads table t and has a GROUP BY.',
      'Each output is a grouping key, or count, sum or avg of a column, with arithmetic, CAST or round over those.',
      'min, max, string_agg, CASE inside an aggregate, FILTER and a CTE named t are not sent.',
      'The request after this one lists the groups your statement made of size at least ' +
        AGGREGATE_K +
        ', and how many groups were dropped.',
      'A smaller group is not in that list. Rows that are not groups are not sent.',
      'group_n is the measured size of a group.',
      'Every count, group_n, the number dropped and the row count of t come back as a range such as 10-99.',
      'Every sum and avg, and arithmetic over them, comes back to two significant figures.',
      'A count leaves only as itself or as what a sum or avg is divided by. HAVING over a count, and a limit after ORDER BY a count, are not applied to what is sent.',
    );
  }
  if (mode === 'pseudonymised') {
    const list = identifiers.length ? identifiers.join(', ') : '(none)';
    lines.push(
      'Pseudonymised mode.',
      'Identifier columns: ' + list + '.',
      'Those columns come back as tokens such as CUSTOMER_ and a number. The same value keeps the same token.',
      'Select an identifier column by name. A function around it is not sent.',
      'Use the token or a slot when you mean that entity.',
    );
  }
  if (mode === 'local-model') {
    lines.push('Local model on this machine.', 'The rows are included below.');
  }
  lines.push('', grammar, '', card, '', 'Question: ' + question);
  if (rows) {
    lines.push('', 'Rows:', rows);
  }
  if (feedback) {
    if (previousSql) lines.push('', 'Previous statement:', previousSql);
    lines.push('', 'Previous attempt:', feedback);
  }
  return lines.join('\n');
}

function describeMode() {
  const mode = document.getElementById('mode').value;
  const blurb = document.getElementById('blurb');
  if (mode === 'aggregates') {
    blurb.textContent = 'Groups of ' + AGGREGATE_K + ' or more can leave. Smaller groups stay.';
  } else if (mode === 'pseudonymised') {
    blurb.textContent = 'Identifier columns leave as tokens. The values stay here.';
  } else if (mode === 'local-model') {
    blurb.textContent =
      'Rows go to 127.0.0.1. Nothing leaves this machine. The first ask holds this tab to ' +
      '127.0.0.1 until it is reloaded. Name a model your server has, and let the server allow ' +
      'this page\'s origin (for Ollama, set OLLAMA_ORIGINS).';
  } else {
    blurb.textContent =
      'The schema leaves this browser. The rows stay. A statement that does not plan goes back ' +
      'for another try; one that finds no rows or fails while reading them stays on this page. ' +
      'What the model can still learn is that its statement planned, which the schema decides.';
  }
  document.getElementById('port-label').hidden = mode !== 'local-model';
  swapModel(mode);
  showBudget();
}

// The hosted default is not a name a local server has, so each side keeps its own.
let modelSide = 'hosted';
const modelKept = { hosted: DEFAULT_MODEL, local: '' };

function swapModel(mode) {
  const side = mode === 'local-model' ? 'local' : 'hosted';
  if (side === modelSide) return;
  const field = document.getElementById('model');
  modelKept[modelSide] = field.value;
  field.value = modelKept[side];
  field.placeholder = side === 'local' ? 'the model your server has' : '';
  modelSide = side;
}

function showBudget() {
  const el = document.getElementById('budget');
  el.hidden = document.getElementById('mode').value !== 'aggregates';
  el.textContent =
    AGGREGATE_ASKS - aggregateAsks + ' of ' + AGGREGATE_ASKS + ' aggregate asks left in this page load.';
}

// A policy added to the document only narrows it, and stays until the page is reloaded.
function holdLocal() {
  if (heldLocal) return;
  const meta = document.createElement('meta');
  meta.httpEquiv = 'Content-Security-Policy';
  meta.content = "connect-src 'self' http://127.0.0.1:*";
  document.head.appendChild(meta);
  heldLocal = true;
}

function dropStatus(text) {
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    return '';
  }
  if (typeof payload.dropped === 'string') {
    return payload.dropped + ' groups under ' + payload.k + ' were left out of the request.';
  }
  if (payload.omitted) return 'No groups were sent.';
  return '';
}

function tokenStatus(text) {
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    return '';
  }
  if (payload.omitted) return 'No identifier rows were sent.';
  return 'Identifier columns left as tokens.';
}

function requestBody(model, text) {
  return JSON.stringify({
    model,
    max_tokens: 2048,
    messages: [{ role: 'user', content: text }],
  });
}

function modelUrl(mode, port) {
  if (mode === 'local-model') return 'http://127.0.0.1:' + port + '/v1/messages';
  return 'https://api.anthropic.com/v1/messages';
}

// Whole for an ordinary request; a local-model dump of thousands of rows is cut.
const LOG_SHOWN = 65536;
const LOG_KEPT = 8;

// Each request is its own entry, cut to its start until asked; only the last few are kept.
function logBody(url, body) {
  const bytes = new TextEncoder().encode(body).length;
  const log = document.getElementById('log');
  const entry = document.createElement('div');
  const text = document.createTextNode(
    'POST ' + url + '\n' + bytes + ' bytes\n' + body.slice(0, LOG_SHOWN) + '\n',
  );
  entry.appendChild(text);
  if (body.length > LOG_SHOWN) {
    const more = document.createElement('button');
    more.type = 'button';
    more.textContent = 'Show all ' + bytes + ' bytes';
    more.addEventListener('click', () => {
      text.textContent = 'POST ' + url + '\n' + bytes + ' bytes\n' + body + '\n';
      more.remove();
    });
    entry.appendChild(more);
  }
  entry.appendChild(document.createTextNode('\n'));
  log.appendChild(entry);
  while (log.childElementCount > LOG_KEPT) log.firstElementChild.remove();
}

class NotSent extends Error {}

// Resolves when the request may go; rejects with NotSent when it may not.
function waitForSend() {
  const send = document.getElementById('send');
  const decline = document.getElementById('decline');
  send.hidden = false;
  decline.hidden = false;
  return new Promise((resolve, reject) => {
    const done = (go) => () => {
      send.hidden = true;
      decline.hidden = true;
      send.removeEventListener('click', yes);
      decline.removeEventListener('click', no);
      if (go) resolve();
      else reject(new NotSent('Not sent.'));
    };
    const yes = done(true);
    const no = done(false);
    send.addEventListener('click', yes);
    decline.addEventListener('click', no);
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function post(url, body, key, mode, attempt) {
  const headers = { 'content-type': 'application/json' };
  if (mode !== 'local-model') {
    headers['x-api-key'] = key;
    headers['anthropic-version'] = '2023-06-01';
    headers['anthropic-dangerous-direct-browser-access'] = 'true';
  }
  const res = await fetch(url, {
    method: 'POST',
    redirect: 'error',
    credentials: 'omit',
    headers,
    body,
  });
  if (!res.ok) {
    let message = '';
    try {
      message = JSON.parse(await res.text())?.error?.message || '';
    } catch {}
    const error = 'model http ' + res.status + (message ? ': ' + message : '');
    // A rate limit or an overloaded server is waited out, and the model is not told: it did not
    // write the failure. Any other refusal is the key, the model name or the request, and asking
    // the model to fix it would only send the request again.
    if (res.status === 429 || res.status === 529 || res.status >= 500) {
      const after = Number(res.headers.get('retry-after'));
      const base = res.status === 429 ? 5000 : 2000 * 2 ** attempt;
      return { error, wait: Number.isFinite(after) && after > 0 ? Math.min(after, 60) * 1000 : base };
    }
    return { error, fatal: res.status >= 400 && res.status < 500 };
  }
  let data;
  try {
    data = JSON.parse(await res.text());
  } catch {
    return { error: 'model http body is not json' };
  }
  const text = (data.content || []).map((block) => block.text || '').join('');
  return { text };
}

function renderTable(host, columns, display) {
  host.replaceChildren();
  const table = document.createElement('table');
  const head = document.createElement('tr');
  for (const column of columns) {
    const th = document.createElement('th');
    th.textContent = column.name;
    head.appendChild(th);
  }
  const thead = document.createElement('thead');
  thead.appendChild(head);
  table.appendChild(thead);
  const body = document.createElement('tbody');
  for (const row of display) {
    const tr = document.createElement('tr');
    for (const cell of row) {
      const td = document.createElement('td');
      td.textContent = cell == null ? '' : String(cell);
      tr.appendChild(td);
    }
    body.appendChild(tr);
  }
  table.appendChild(body);
  host.appendChild(table);
}

function svgEl(name) {
  return document.createElementNS(SVG_NS, name);
}

function colIndex(columns, name) {
  return columns.findIndex((column) => column.name === name);
}

function mag(raw) {
  if (raw == null) return 0;
  const n = Number(raw);
  return Number.isFinite(n) ? Math.abs(n) : 0;
}

function cellText(display, r, idx) {
  const raw = idx >= 0 && display[r] ? display[r][idx] : '';
  return raw == null ? '' : String(raw);
}

// The name a screen reader gives the chart, and the values it reads under it.
function describe(svg, chart, columns, display, limit) {
  const title = svgEl('title');
  title.id = 'chart-title';
  const series = chart.y.join(', ');
  title.textContent =
    chart.kind === 'number'
      ? series
      : chart.kind.replace('_', ' ') + ' chart of ' + series + (chart.x ? ' by ' + chart.x : '');
  const desc = svgEl('desc');
  desc.id = 'chart-desc';
  const xIdx = colIndex(columns, chart.x);
  const yIdx = chart.y.map((name) => colIndex(columns, name));
  const lines = [];
  for (let r = 0; r < limit; r++) {
    const values = chart.y.map((name, i) => name + ' ' + cellText(display, r, yIdx[i]));
    lines.push((xIdx >= 0 ? cellText(display, r, xIdx) + ': ' : '') + values.join(', '));
  }
  desc.textContent = lines.join('; ');
  svg.setAttribute('role', 'img');
  svg.setAttribute('aria-labelledby', 'chart-title chart-desc');
  svg.appendChild(title);
  svg.appendChild(desc);
}

function legend(svg, names) {
  const group = svgEl('g');
  group.setAttribute('class', 'legend');
  let x = 20;
  names.forEach((name, series) => {
    const swatch = svgEl('rect');
    swatch.setAttribute('x', String(x));
    swatch.setAttribute('y', '8');
    swatch.setAttribute('width', '10');
    swatch.setAttribute('height', '10');
    swatch.setAttribute('fill', FILLS[series % FILLS.length]);
    group.appendChild(swatch);
    const text = svgEl('text');
    text.setAttribute('x', String(x + 14));
    text.setAttribute('y', '17');
    text.setAttribute('fill', '#b7ab9a');
    text.setAttribute('font-size', '11');
    text.textContent = name;
    group.appendChild(text);
    x += 30 + name.length * 7;
  });
  svg.appendChild(group);
}

function draw(host, chart, columns, rows, display) {
  host.replaceChildren();
  if (!chart || chart.kind === 'table') return;
  const svg = svgEl('svg');
  svg.setAttribute('viewBox', '0 0 640 280');
  describe(svg, chart, columns, display, chart.kind === 'number' ? 1 : Math.min(rows.length, 24));
  if (chart.kind === 'number') {
    const idx = colIndex(columns, chart.y[0]);
    const text = svgEl('text');
    text.setAttribute('x', '320');
    text.setAttribute('y', '156');
    text.setAttribute('text-anchor', 'middle');
    text.setAttribute('fill', '#f3ecdf');
    text.setAttribute('font-size', '48');
    text.textContent = idx >= 0 && display[0] ? display[0][idx] || '' : '';
    svg.appendChild(text);
    host.appendChild(svg);
    return;
  }
  const limit = Math.min(rows.length, 24);
  const yIdx = chart.y.map((name) => colIndex(columns, name));
  const xIdx = colIndex(columns, chart.x);
  let peak = 0;
  for (let r = 0; r < limit; r++) {
    let total = 0;
    for (const idx of yIdx) {
      if (idx >= 0) total += mag(rows[r][idx]);
    }
    if (total > peak) peak = total;
  }
  if (peak === 0) peak = 1;
  const slot = 600 / limit;
  if (chart.kind === 'line') {
    const points = [];
    for (let r = 0; r < limit; r++) {
      const idx = yIdx[0];
      const h = idx >= 0 ? mag(rows[r][idx]) / peak : 0;
      points.push(20 + r * slot + slot / 2 + ',' + (240 - h * 200));
    }
    const line = svgEl('polyline');
    line.setAttribute('points', points.join(' '));
    line.setAttribute('fill', 'none');
    line.setAttribute('stroke', FILLS[0]);
    line.setAttribute('stroke-width', '2');
    line.dataset.series = chart.y[0];
    svg.appendChild(line);
  } else {
    for (let r = 0; r < limit; r++) {
      let stacked = 0;
      yIdx.forEach((idx, series) => {
        const h = idx >= 0 ? (mag(rows[r][idx]) / peak) * 200 : 0;
        const rect = svgEl('rect');
        rect.setAttribute('x', String(20 + r * slot + 2));
        rect.setAttribute('y', String(240 - stacked - h));
        rect.setAttribute('width', String(Math.max(slot - 4, 1)));
        rect.setAttribute('height', String(h));
        rect.setAttribute('fill', FILLS[series % FILLS.length]);
        rect.dataset.series = chart.y[series];
        svg.appendChild(rect);
        stacked += h;
      });
    }
  }
  for (let r = 0; r < limit; r++) {
    const label = svgEl('text');
    label.setAttribute('x', String(20 + r * slot + slot / 2));
    label.setAttribute('y', '268');
    label.setAttribute('text-anchor', 'middle');
    label.setAttribute('fill', '#b7ab9a');
    label.setAttribute('font-size', '10');
    label.textContent = cellText(display, r, xIdx).slice(0, 12);
    svg.appendChild(label);
  }
  legend(svg, chart.kind === 'line' ? chart.y.slice(0, 1) : chart.y);
  host.appendChild(svg);
}

// Queries and charts stay in the tab. The model call is the one that leaves.
async function localChart(sql, chart, narration) {
  const queried = await call('query', { sql });
  if (queried.error) throw new Error(queried.error);
  const locale = separators();
  const shown = await call('present', {
    answer: queried.answer,
    chart,
    narration,
    group: locale.group,
    decimal: locale.decimal,
  });
  if (shown.narration_error) throw new Error(shown.narration_error);
  render(shown, sql, queried.answer);
  return shown.narration || '';
}

// What is on the page, for the CSV and for running the statement again here.
let shownAnswer = null;
let shownChart = null;

function render(shown, sql, answer) {
  shownAnswer = answer;
  shownChart = shown.chart;
  document.getElementById('answer').hidden = false;
  document.getElementById('narration').textContent = shown.narration || '';
  document.getElementById('narration-error').textContent = shown.narration_error || '';
  const editor = document.getElementById('sql');
  editor.defaultValue = sql;
  editor.value = sql;
  draw(
    document.getElementById('chart'),
    shown.chart,
    answer.columns,
    answer.rows,
    shown.display,
  );
  renderTable(document.getElementById('table'), answer.columns, shown.display);
  if (answer.truncated) status('Result truncated at 5000 rows.');
}

// A spreadsheet runs a cell that starts like a formula, and the model wrote the statement.
function csvCell(cell) {
  if (cell == null) return '';
  let text = String(cell);
  if (/^[=+@\t\r]/.test(text) || (text.startsWith('-') && !Number.isFinite(Number(text)))) {
    text = "'" + text;
  }
  return /[",\r\n]/.test(text) ? '"' + text.replace(/"/g, '""') + '"' : text;
}

function toCsv(answer) {
  const lines = [answer.columns.map((column) => csvCell(column.name)).join(',')];
  for (const row of answer.rows) lines.push(row.map(csvCell).join(','));
  return lines.join('\r\n') + '\r\n';
}

function exportCsv() {
  if (!shownAnswer) return;
  const url = URL.createObjectURL(new Blob([toCsv(shownAnswer)], { type: 'text/csv' }));
  const link = document.createElement('a');
  link.href = url;
  link.download = 'burrmill-result.csv';
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

// The edited statement runs in the tab only. Its narration was written for the model's
// statement, so it is not carried over.
async function runHere() {
  const sql = document.getElementById('sql').value.trim();
  if (!sql || !shownChart) return;
  const button = document.getElementById('run-here');
  button.disabled = true;
  try {
    const queried = await call('query', { sql });
    if (queried.error) {
      status(queried.error);
      return;
    }
    const locale = separators();
    const shown = await call('present', {
      answer: queried.answer,
      chart: shownChart,
      narration: '',
      group: locale.group,
      decimal: locale.decimal,
    });
    render(shown, sql, queried.answer);
    status(queried.answer.rows.length ? 'Ran here. Nothing was sent.' : 'No rows. Nothing was sent.');
  } catch (err) {
    status(err.message || String(err));
  } finally {
    button.disabled = false;
  }
}

function identifierHits(text) {
  const raw = String(text || '');
  const hits = [];
  const email = raw.match(/[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/i);
  if (email) hits.push(email[0]);
  const phone = raw.match(/\+?\d[\d\s().-]{8,}\d/);
  if (phone && phone[0].replace(/\D/g, '').length >= 10) hits.push(phone[0]);
  for (const token of raw.toLowerCase().split(/[^a-z0-9]+/)) {
    if (!token) continue;
    const stem = token.endsWith('s') ? token.slice(0, -1) : token;
    if (IDENTIFIER_LABELS.has(token) || IDENTIFIER_LABELS.has(stem)) hits.push(token);
  }
  return hits;
}

function warnNames() {
  const el = document.getElementById('name-warn');
  const names = [];
  for (const field of fields) {
    const name = field.input.value.trim();
    if (name && identifierHits(name).length) names.push(name);
  }
  const questionHit = identifierHits(document.getElementById('question').value).length > 0;
  if (!names.length && !questionHit) {
    el.hidden = true;
    el.textContent = '';
    return;
  }
  const many = names.length + (questionHit ? 1 : 0) > 1;
  let who = names.join(', ');
  if (names.length && questionHit) who += ' and the question';
  else if (questionHit) who = 'The question';
  el.hidden = false;
  el.textContent =
    who +
    (many ? ' look like identifiers. Rename them before asking.' : ' looks like an identifier. Rename it before asking.');
}

function showColumns(columns, proposals) {
  const host = document.getElementById('columns');
  host.replaceChildren();
  fields.length = 0;
  const proposed = new Map((proposals || []).map((item) => [item.name, item.kind]));
  for (const column of columns) {
    const row = document.createElement('div');
    row.className = 'column';
    const input = document.createElement('input');
    input.type = 'text';
    input.value = column.name;
    input.dataset.name = column.name;
    input.setAttribute('aria-label', 'Name for column ' + column.name);
    input.addEventListener('input', warnNames);
    row.appendChild(input);
    const field = { input, sign: null, date: null, identifier: null };
    if (column.numeric) {
      field.sign = tick(row, 'Sign', 'sign');
    }
    if (column.temporal) {
      field.date = tick(row, 'Date range (sends the real min and max)', '');
    }
    const kind = proposed.get(column.name);
    field.identifier = tick(row, kind ? 'Identifier, ' + kind : 'Identifier', 'identifier');
    if (kind) field.identifier.checked = true;
    host.appendChild(row);
    fields.push(field);
  }
  warnNames();
}

function tick(row, label, className) {
  const box = document.createElement('input');
  box.type = 'checkbox';
  if (className) box.className = className;
  const text = document.createElement('label');
  text.appendChild(box);
  text.appendChild(document.createTextNode(' ' + label));
  row.appendChild(text);
  return box;
}

function showStored(name) {
  const el = document.getElementById('stored');
  if (name == null) {
    el.hidden = true;
    el.textContent = '';
    return;
  }
  el.hidden = false;
  el.textContent = (name || 'A file') + ' is stored on this machine.';
}

async function storeDir(create) {
  const root = await navigator.storage.getDirectory();
  return root.getDirectoryHandle('burrmill', create ? { create: true } : undefined);
}

async function storeFile(name, bytes) {
  const dir = await storeDir(true);
  const bin = await dir.getFileHandle('upload.bin', { create: true });
  const binOut = await bin.createWritable({ keepExistingData: false });
  await binOut.write(new Blob([bytes]));
  await binOut.close();
  const nameHandle = await dir.getFileHandle('upload.name', { create: true });
  const nameOut = await nameHandle.createWritable({ keepExistingData: false });
  await nameOut.write(new Blob([new TextEncoder().encode(name)]));
  await nameOut.close();
}

async function loadStore() {
  try {
    const dir = await storeDir(false);
    const bin = await (await dir.getFileHandle('upload.bin')).getFile();
    const nameFile = await (await dir.getFileHandle('upload.name')).getFile();
    const bytes = await bin.arrayBuffer();
    if (!bytes.byteLength) return null;
    return { name: await nameFile.text(), bytes };
  } catch (err) {
    if (err && err.name === 'NotFoundError') return null;
    throw err;
  }
}

async function clearStore() {
  const root = await navigator.storage.getDirectory();
  try {
    await root.removeEntry('burrmill', { recursive: true });
  } catch (err) {
    if (err && err.name === 'NotFoundError') return;
    throw err;
  }
}

// Ingest refuses past this too; checking first saves reading a file it will not take.
const MAX_FILE_BYTES = 200 * 1024 * 1024;

function megabytes(text) {
  return text.replace(/\b\d{7,}\b/g, (n) => (Number(n) / 1048576).toFixed(1) + ' MB');
}

async function openFile(file, persist) {
  if (file.size > MAX_FILE_BYTES) {
    status(
      file.name + ' is ' + megabytes(String(file.size)) + '; the limit is ' +
        megabytes(String(MAX_FILE_BYTES)) + '.',
    );
    return;
  }
  status('Reading ' + file.name);
  const bytes = await file.arrayBuffer();
  let info;
  try {
    // Transferred, not copied. The File stays the handle for storing it.
    info = await call('open', { bytes }, [bytes]);
  } catch (err) {
    status(megabytes(err.message || String(err)));
    return;
  }
  held = file;
  heldName = file.name;
  renames = [];
  let storageNote = '';
  if (persist) {
    try {
      await storeFile(heldName, held);
      showStored(heldName);
    } catch (err) {
      document.getElementById('keep').checked = false;
      showStored(null);
      await clearStore();
      storageNote = err.message || String(err);
    }
  }
  fileOpen = true;
  document.getElementById('setup').hidden = false;
  document.getElementById('ask').disabled = false;
  showColumns(info.columns, info.proposals);
  renderTable(document.getElementById('preview'), info.columns, info.preview);
  status(storageNote);
}

async function keepHeld() {
  const keep = document.getElementById('keep');
  if (!held) {
    keep.checked = false;
    status('Drop a file before keeping it.');
    return;
  }
  try {
    await storeFile(heldName, held);
  } catch (err) {
    keep.checked = false;
    showStored(null);
    await clearStore();
    throw err;
  }
  showStored(heldName);
  status('');
}

async function forgetHeld() {
  await clearStore();
  showStored(null);
}

function bindStore() {
  const keep = document.getElementById('keep');
  keep.addEventListener('change', () => {
    const job = keep.checked ? keepHeld() : forgetHeld();
    job.catch((err) => status(err.message || String(err)));
  });
  document.getElementById('clear-all').addEventListener('click', () => {
    clearStore()
      .then(() => {
        keep.checked = false;
        showStored(null);
      })
      .catch((err) => status(err.message || String(err)));
  });
}

async function restoreStore() {
  try {
    const stored = await loadStore();
    if (!stored) return;
    showStored(stored.name);
    if (fileOpen) return;
    document.getElementById('keep').checked = true;
    await openFile(new File([stored.bytes], stored.name), false);
  } catch (err) {
    status(err.message || String(err));
  } finally {
    document.documentElement.dataset.store = 'ready';
  }
}

async function applyRenames() {
  for (const field of fields) {
    const next = field.input.value.trim();
    const prev = field.input.dataset.name;
    if (next === prev) continue;
    await call('rename', { from: prev, to: next });
    renames.push({ from: prev, to: next });
    field.input.dataset.name = next;
  }
}

function ticked(kind) {
  const names = [];
  for (const field of fields) {
    const box = field[kind];
    if (box && box.checked) names.push(field.input.dataset.name);
  }
  return names;
}

async function ask() {
  if (!fileOpen) return;
  const question = document.getElementById('question').value.trim();
  if (!question) {
    status('Pick a question.');
    return;
  }
  document.getElementById('ask').disabled = true;
  status('');
  try {
    await applyRenames();
    const mode = document.getElementById('mode').value;
    const key = document.getElementById('key').value;
    const modelName = document.getElementById('model').value.trim();
    const model = modelName || DEFAULT_MODEL;
    let port = '';
    if (mode === 'local-model') {
      port = document.getElementById('port').value.trim();
      const n = Number(port);
      if (!/^[0-9]+$/.test(port) || n < 1 || n > 65535) {
        status('The local model is a port on 127.0.0.1.');
        return;
      }
      if (!modelName) {
        status('Name the model your local server has.');
        return;
      }
    } else if (!key) {
      status('The key stays in this tab. Paste one to ask.');
      return;
    }
    if (heldLocal && mode !== 'local-model') {
      status('This tab sent rows to 127.0.0.1 and is held there. Reload to use another mode.');
      return;
    }
    if (mode === 'aggregates') {
      if (aggregateAsks >= AGGREGATE_ASKS) {
        status('The ' + AGGREGATE_ASKS + ' aggregate asks for this page load are spent.');
        return;
      }
      aggregateAsks++;
      showBudget();
    }
    if (mode === 'local-model') holdLocal();
    const identifiers = mode === 'pseudonymised' ? ticked('identifier') : [];
    await call('bind', { identifiers });
    const packed = await call('card', {
      signs: ticked('sign'),
      dates: ticked('date'),
      coarse: mode === 'aggregates',
    });
    let rows = '';
    if (mode === 'local-model') {
      const dumped = await call('query', { sql: 'SELECT * FROM t' });
      if (dumped.error) throw new Error(dumped.error);
      rows = JSON.stringify({
        columns: dumped.answer.columns,
        rows: dumped.answer.rows,
        truncated: dumped.answer.truncated,
      });
    }
    const locale = separators();
    const url = modelUrl(mode, port);
    let feedback = '';
    let previousSql = '';
    let lastError = '';
    let fed = false;
    for (let attempt = 0; attempt < 4; attempt++) {
      const body = requestBody(
        model,
        promptText(question, packed.card, packed.grammar, feedback, previousSql, mode, identifiers, rows),
      );
      logBody(url, body);
      if (document.getElementById('confirm').checked) await waitForSend();
      const sent = await post(url, body, key, mode, attempt);
      if (sent.fatal) {
        status(sent.error);
        return;
      }
      if (sent.wait) {
        lastError = sent.error;
        status(sent.error);
        if (attempt < 3) {
          status(sent.error + '. Waiting ' + Math.round(sent.wait / 1000) + ' s.');
          await sleep(sent.wait);
        }
        continue;
      }
      if (sent.error) {
        feedback = await call('error', { message: sent.error });
        status(sent.error);
        continue;
      }
      lastError = '';
      let reply;
      previousSql = '';
      try {
        reply = await call('parse', { text: sent.text });
      } catch (err) {
        feedback = await call('error', { message: err.message || String(err) });
        status(feedback);
        continue;
      }
      previousSql = reply.sql;
      const queried = await call('query', { sql: reply.sql });
      // Whether a statement reads rows and then fails, or finds none, is about the data. In
      // schema mode only a failure the schema decides goes back.
      if (queried.error && mode === 'schema' && !queried.before_rows) {
        status(queried.error);
        return;
      }
      if (queried.error && (queried.before_rows || mode === 'local-model')) {
        feedback = queried.feedback;
        status(queried.error);
        continue;
      }
      if (mode === 'aggregates' || mode === 'pseudonymised') {
        const op = mode === 'aggregates' ? 'aggregates' : 'pseudonym';
        // From here the model hears only the mode's answer, and what the run did only when the
        // mode passes the statement: a refused one is answered alike whether it failed or not.
        const admitted = await call('admits', { mode: op, sql: reply.sql });
        if (queried.error) status(queried.error);
        if (!fed && attempt < 3) {
          feedback = await call(op, { sql: reply.sql });
          if (!(queried.error && admitted)) fed = true;
          if (!queried.error) status(op === 'aggregates' ? dropStatus(feedback) : tokenStatus(feedback));
          continue;
        }
        // An aggregate's failure or empty answer can turn on one member, so it is not asked about.
        const told = admitted && mode === 'pseudonymised' && attempt < 3;
        if (queried.error) {
          if (told) {
            feedback = await call(op, { sql: reply.sql });
            continue;
          }
          return;
        }
        if (queried.answer.rows.length === 0 && told) {
          feedback = queried.feedback;
          status('No rows. Asking again.');
          continue;
        }
      } else if (queried.answer.rows.length === 0 && attempt < 3 && mode !== 'schema') {
        feedback = queried.feedback;
        status('No rows. Asking again.');
        continue;
      }
      const shown = await call('present', {
        answer: queried.answer,
        chart: reply.chart,
        narration: reply.narration,
        group: locale.group,
        decimal: locale.decimal,
      });
      render(shown, reply.sql, queried.answer);
      status(queried.answer.rows.length ? '' : 'No rows.');
      return;
    }
    status(lastError || 'No answer after 4 attempts.');
  } catch (err) {
    const message = err.message || String(err);
    const local = document.getElementById('mode').value === 'local-model';
    status(
      local && err instanceof TypeError
        ? message + '. The local server is not running on that port, or does not allow this page\'s origin.'
        : message,
    );
  } finally {
    document.getElementById('ask').disabled = false;
    document.getElementById('send').hidden = true;
    document.getElementById('decline').hidden = true;
  }
}

function bindKey() {
  const input = document.getElementById('key');
  const remember = document.getElementById('remember');
  const stored = sessionStorage.getItem('burrmill-key');
  if (stored) {
    input.value = stored;
    remember.checked = true;
  }
  const keep = () => {
    if (remember.checked) sessionStorage.setItem('burrmill-key', input.value);
    else sessionStorage.removeItem('burrmill-key');
  };
  input.addEventListener('input', keep);
  remember.addEventListener('change', keep);
}

function bindQuestions() {
  const host = document.getElementById('questions');
  for (const question of QUESTIONS) {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = question;
    button.addEventListener('click', () => {
      document.getElementById('question').value = question;
      warnNames();
    });
    host.appendChild(button);
  }
}

function openDropped(file) {
  const persist = document.getElementById('keep').checked;
  openFile(file, persist).catch((err) => status(err.message || String(err)));
}

document.getElementById('file-input').addEventListener('change', (event) => {
  const file = event.target.files && event.target.files[0];
  if (file) openDropped(file);
});

const drop = document.getElementById('drop');
drop.addEventListener('dragover', (event) => event.preventDefault());
drop.addEventListener('drop', (event) => {
  event.preventDefault();
  const file = event.dataTransfer && event.dataTransfer.files && event.dataTransfer.files[0];
  if (file) openDropped(file);
});

document.getElementById('question').addEventListener('input', warnNames);
document.getElementById('ask').addEventListener('click', () => {
  ask();
});
document.getElementById('mode').addEventListener('change', describeMode);
document.getElementById('stop').addEventListener('click', () => restart('Stopped.'));
document.getElementById('export').addEventListener('click', exportCsv);
document.getElementById('run-here').addEventListener('click', () => {
  runHere();
});
document.getElementById('model').value = DEFAULT_MODEL;

function bindOffline() {
  if (!('serviceWorker' in navigator)) return;
  navigator.serviceWorker.register('./sw.js').catch((err) => status(err.message || String(err)));
}

bindKey();
bindQuestions();
describeMode();
bindStore();
bindOffline();
restoreStore();
globalThis.burrmillLocal = localChart;
