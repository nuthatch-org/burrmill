// A new name drops what the earlier worker kept beyond the shell.
const CACHE = 'burrmill-shell-2';

const SHELL = [
  './',
  './index.html',
  './app.js',
  './style.css',
  './worker.js',
  './manifest.webmanifest',
  './icon-192.png',
  './icon-512.png',
  './pkg/burrmill_wasm.js',
  './pkg/burrmill_wasm_bg.wasm',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE).then((cache) => cache.addAll(SHELL)).then(() => self.skipWaiting()),
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((key) => key !== CACHE).map((key) => caches.delete(key))))
      .then(() => self.clients.claim()),
  );
});

const SHELL_URLS = new Set(SHELL.map((path) => new URL(path, self.location.href).href));

// The shell only. The model call stays a network request, so airplane mode fails it, and
// anything else on this origin is left to the network rather than kept.
self.addEventListener('fetch', (event) => {
  if (event.request.method !== 'GET' || !SHELL_URLS.has(event.request.url)) return;
  event.respondWith(shell(event.request));
});

async function shell(request) {
  const cache = await caches.open(CACHE);
  try {
    const fresh = await fetch(request);
    if (fresh.ok) await cache.put(request, fresh.clone());
    return fresh;
  } catch (err) {
    const hit = await cache.match(request);
    if (hit) return hit;
    throw err;
  }
}
