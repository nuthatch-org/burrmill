import init, {
  WasmSession,
  parse_model,
  shape_feedback,
  error_feedback,
  present_answer,
} from './pkg/burrmill_wasm.js';

const ready = init();
let session = null;

self.onmessage = async (event) => {
  const msg = event.data;
  try {
    await ready;
    const result = await handle(msg);
    self.postMessage({ id: msg.id, ok: true, result });
  } catch (err) {
    const error = err && err.message ? err.message : String(err);
    self.postMessage({ id: msg.id, ok: false, error });
  }
};

async function handle(msg) {
  if (msg.op === 'open') {
    session = WasmSession.open(new Uint8Array(msg.bytes));
    return {
      columns: JSON.parse(session.columns()),
      preview: JSON.parse(session.preview(20)),
    };
  }
  if (!session) {
    throw new Error('no file');
  }
  if (msg.op === 'rename') {
    session.rename(msg.from, msg.to);
    return {};
  }
  if (msg.op === 'card') {
    return {
      card: session.card(JSON.stringify(msg.signs || []), JSON.stringify(msg.dates || [])),
      grammar: session.grammar(),
    };
  }
  if (msg.op === 'parse') {
    return JSON.parse(parse_model(msg.text));
  }
  if (msg.op === 'query') {
    try {
      const answer = JSON.parse(await session.query(msg.sql));
      return { answer, feedback: shape_feedback(JSON.stringify(answer)) };
    } catch (err) {
      const message = err && err.message ? err.message : String(err);
      return { error: message, feedback: error_feedback(message) };
    }
  }
  if (msg.op === 'aggregates') {
    return session.aggregate_feedback(msg.sql);
  }
  if (msg.op === 'present') {
    return JSON.parse(
      present_answer(
        JSON.stringify(msg.answer),
        JSON.stringify(msg.chart),
        msg.narration,
        msg.group,
        msg.decimal,
      ),
    );
  }
  if (msg.op === 'error') {
    return error_feedback(msg.message || '');
  }
  throw new Error('unknown op');
}
