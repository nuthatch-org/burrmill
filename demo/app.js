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

function promptText(question, card, grammar, feedback, mode) {
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
  lines.push('', grammar, '', card, '', 'Question: ' + question);
  if (feedback) {
    lines.push('', 'Previous attempt:', feedback);
  }
  return lines.join('\n');
}

function describeMode() {
  const mode = document.getElementById('mode').value;
  document.getElementById('blurb').textContent =
    mode === 'aggregates'
      ? 'Groups of ' + AGGREGATE_K + ' or more can leave. Smaller groups stay.'
      : 'The schema leaves this browser. The rows stay.';
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

function requestBody(model, text) {
  return JSON.stringify({
    model,
    max_tokens: 2048,
    messages: [{ role: 'user', content: text }],
  });
}

function logBody(body) {
  const bytes = new TextEncoder().encode(body).length;
  document.getElementById('log').appendChild(
    document.createTextNode(
      'POST https://api.anthropic.com/v1/messages\n' + bytes + ' bytes\n' + body + '\n\n',
    ),
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

async function post(body, key) {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    redirect: 'error',
    credentials: 'omit',
    headers: {
      'content-type': 'application/json',
      'x-api-key': key,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
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

function showColumns(columns) {
  const host = document.getElementById('columns');
  host.replaceChildren();
  fields.length = 0;
  for (const column of columns) {
    const row = document.createElement('div');
    row.className = 'column';
    const input = document.createElement('input');
    input.type = 'text';
    input.value = column.name;
    input.dataset.name = column.name;
    row.appendChild(input);
    const field = { input, sign: null, date: null };
    if (column.numeric) {
      field.sign = tick(row, 'Sign');
    }
    if (column.temporal) {
      field.date = tick(row, 'Date range');
    }
    host.appendChild(row);
    fields.push(field);
  }
}

function tick(row, label) {
  const box = document.createElement('input');
  box.type = 'checkbox';
  const text = document.createElement('label');
  text.appendChild(box);
  text.appendChild(document.createTextNode(' ' + label));
  row.appendChild(text);
  return box;
}

async function openFile(file) {
  status('Reading ' + file.name);
  const bytes = await file.arrayBuffer();
  const info = await call('open', { bytes }, [bytes]);
  fileOpen = true;
  document.getElementById('setup').hidden = false;
  document.getElementById('ask').disabled = false;
  showColumns(info.columns);
  renderTable(document.getElementById('preview'), info.columns, info.preview);
  status('');
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
  const key = document.getElementById('key').value;
  if (!key) {
    status('The key stays in this tab. Paste one to ask.');
    return;
  }
  const question = document.getElementById('question').value.trim();
  if (!question) {
    status('Pick a question.');
    return;
  }
  document.getElementById('ask').disabled = true;
  status('');
  try {
    await applyRenames();
    const packed = await call('card', { signs: ticked('sign'), dates: ticked('date') });
    const model = document.getElementById('model').value.trim() || 'claude-sonnet-4-5';
    const locale = separators();
    const mode = document.getElementById('mode').value;
    let feedback = '';
    let fedGroups = false;
    for (let attempt = 0; attempt < 4; attempt++) {
      const body = requestBody(
        model,
        promptText(question, packed.card, packed.grammar, feedback, mode),
      );
      logBody(body);
      if (document.getElementById('confirm').checked) await waitForSend();
      const sent = await post(body, key);
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

document.getElementById('file-input').addEventListener('change', (event) => {
  const file = event.target.files && event.target.files[0];
  if (file) openFile(file).catch((err) => status(err.message || String(err)));
});

const drop = document.getElementById('drop');
drop.addEventListener('dragover', (event) => event.preventDefault());
drop.addEventListener('drop', (event) => {
  event.preventDefault();
  const file = event.dataTransfer && event.dataTransfer.files && event.dataTransfer.files[0];
  if (file) openFile(file).catch((err) => status(err.message || String(err)));
});

document.getElementById('ask').addEventListener('click', () => {
  ask();
});
document.getElementById('mode').addEventListener('change', describeMode);

bindKey();
bindQuestions();
describeMode();
