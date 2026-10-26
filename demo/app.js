const QUESTIONS = [
  'Monthly spend',
  'Subscriptions',
  'Top merchants',
  'Largest single debit',
  'Year-over-year change',
];

// RFC-0047's stated default. Whether k should differ, or be per column, is open.
const AGGREGATE_K = 10;

const SVG_NS = 'http://www.w3.org/2000/svg';
const FILLS = ['#c45c26', '#3d5a6c', '#8a6232', '#5c6b45'];

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
      if (resolved.origin === window.location.origin && resolved.pathname.endsWith('.wasm')) {
        return resolved.href;
      }
      throw new Error('script url refused');
    },
  });
}

const worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
let seq = 0;
const pending = new Map();
const fields = [];
let fileOpen = false;
// The buffer posted to the worker is detached. This copy is what a later tick can store.
let held = null;
let heldName = '';

worker.onmessage = (event) => {
  const msg = event.data;
  const wait = pending.get(msg.id);
  if (!wait) return;
  pending.delete(msg.id);
  if (msg.ok) wait.resolve(msg.result);
  else wait.reject(new Error(msg.error || 'worker failed'));
};

function call(op, extra, transfer) {
  const id = ++seq;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage(Object.assign({ id, op }, extra || {}), transfer || []);
  });
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

function promptText(question, card, grammar, feedback, mode, identifiers, rows) {
  const lines = [
    'Return one JSON object and no other text.',
    'Keys are sql, chart, and narration. No other keys.',
    'sql is one SELECT or WITH. The only uploaded table is t.',
    'chart.kind is number, bar, line, stacked_bar, or table.',
    'chart.y is a column name or an array of column names. chart.x is a column name.',
    'narration is one sentence. A slot is {rN.column} or {column} for the first row.',
    'A digit outside a slot is refused. Do not write a number except inside a slot.',
    'The schema below has no cell values. Do not invent values.',
  ];
  if (mode === 'aggregates') {
    lines.push(
      'Aggregates mode, k = ' + AGGREGATE_K + '.',
      'Write one SELECT that reads table t and has a GROUP BY.',
      'The next message lists groups of size at least ' +
        AGGREGATE_K +
        ', and how many groups were dropped.',
      'A smaller group is not in that list. Rows that are not groups are not sent.',
      'group_n is the measured size of a group.',
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
    blurb.textContent = 'Rows go to 127.0.0.1. Nothing leaves this machine.';
  } else {
    blurb.textContent = 'The schema leaves this browser. The rows stay.';
  }
  document.getElementById('port-label').hidden = mode !== 'local-model';
}

function dropStatus(text) {
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    return '';
  }
  if (typeof payload.dropped === 'number') {
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

function logBody(url, body) {
  const bytes = new TextEncoder().encode(body).length;
  document.getElementById('log').appendChild(
    document.createTextNode('POST ' + url + '\n' + bytes + ' bytes\n' + body + '\n\n'),
  );
}

function waitForSend() {
  const send = document.getElementById('send');
  send.hidden = false;
  return new Promise((resolve) => {
    send.addEventListener(
      'click',
      () => {
        send.hidden = true;
        resolve();
      },
      { once: true },
    );
  });
}

async function post(url, body, key, mode) {
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
    return { error: 'model http ' + res.status };
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

function draw(host, chart, columns, rows, display) {
  host.replaceChildren();
  if (!chart || chart.kind === 'table') return;
  const svg = svgEl('svg');
  svg.setAttribute('viewBox', '0 0 640 280');
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
    const raw = xIdx >= 0 && display[r] ? display[r][xIdx] : '';
    label.textContent = raw == null ? '' : String(raw).slice(0, 12);
    svg.appendChild(label);
  }
  host.appendChild(svg);
}

function render(shown, sql, answer) {
  document.getElementById('answer').hidden = false;
  document.getElementById('narration').textContent = shown.narration || '';
  document.getElementById('narration-error').textContent = shown.narration_error || '';
  document.getElementById('sql').textContent = sql;
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
    row.appendChild(input);
    const field = { input, sign: null, date: null, identifier: null };
    if (column.numeric) {
      field.sign = tick(row, 'Sign', 'sign');
    }
    if (column.temporal) {
      field.date = tick(row, 'Date range', '');
    }
    const kind = proposed.get(column.name);
    field.identifier = tick(row, kind ? 'Identifier, ' + kind : 'Identifier', 'identifier');
    if (kind) field.identifier.checked = true;
    host.appendChild(row);
    fields.push(field);
  }
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

async function openFile(file, persist) {
  status('Reading ' + file.name);
  const bytes = await file.arrayBuffer();
  held = bytes.slice(0);
  heldName = file.name;
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
  const info = await call('open', { bytes }, [bytes]);
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
    let port = '';
    if (mode === 'local-model') {
      port = document.getElementById('port').value.trim();
      const n = Number(port);
      if (!/^[0-9]+$/.test(port) || n < 1 || n > 65535) {
        status('The local model is a port on 127.0.0.1.');
        return;
      }
    } else if (!key) {
      status('The key stays in this tab. Paste one to ask.');
      return;
    }
    const identifiers = mode === 'pseudonymised' ? ticked('identifier') : [];
    await call('bind', { identifiers });
    const packed = await call('card', { signs: ticked('sign'), dates: ticked('date') });
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
    const model = document.getElementById('model').value.trim() || 'claude-sonnet-4-5';
    const locale = separators();
    const url = modelUrl(mode, port);
    let feedback = '';
    let fedGroups = false;
    let fedTokens = false;
    for (let attempt = 0; attempt < 4; attempt++) {
      const body = requestBody(
        model,
        promptText(question, packed.card, packed.grammar, feedback, mode, identifiers, rows),
      );
      logBody(url, body);
      if (document.getElementById('confirm').checked) await waitForSend();
      const sent = await post(url, body, key, mode);
      if (sent.error) {
        feedback = await call('error', { message: sent.error });
        status(sent.error);
        continue;
      }
      let reply;
      try {
        reply = await call('parse', { text: sent.text });
      } catch (err) {
        feedback = await call('error', { message: err.message || String(err) });
        status(feedback);
        continue;
      }
      const queried = await call('query', { sql: reply.sql });
      if (queried.error) {
        feedback = queried.feedback;
        status(queried.error);
        continue;
      }
      if (mode === 'aggregates' && !fedGroups && attempt < 3) {
        feedback = await call('aggregates', { sql: reply.sql });
        fedGroups = true;
        status(dropStatus(feedback));
        continue;
      }
      if (mode === 'pseudonymised' && !fedTokens && attempt < 3) {
        feedback = await call('pseudonym', { sql: reply.sql });
        fedTokens = true;
        status(tokenStatus(feedback));
        continue;
      }
      if (queried.answer.rows.length === 0 && attempt < 3) {
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
      status('');
      return;
    }
    status('No answer after 4 attempts.');
  } catch (err) {
    status(err.message || String(err));
  } finally {
    document.getElementById('ask').disabled = false;
    document.getElementById('send').hidden = true;
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

document.getElementById('ask').addEventListener('click', () => {
  ask();
});
document.getElementById('mode').addEventListener('change', describeMode);

bindKey();
bindQuestions();
describeMode();
bindStore();
restoreStore();
