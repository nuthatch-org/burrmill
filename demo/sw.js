const CACHE = 'burrmill-shell-1';

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

// Same-origin only. The model call stays a network request, so airplane mode fails it.
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin || event.request.method !== 'GET') return;
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
