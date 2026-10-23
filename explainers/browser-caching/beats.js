// Script for the browser caching explainer. One beat = one line of narration.
// id: stable key the scenes are timed against. ch: chapter. lab: step name in the step bar.
// v: what is on screen. hold: extra seconds to look after the line is read.

const chapters = [
  { id: 'intro', title: 'Intro', short: 'Intro' },
  { id: 'r1', title: 'Request 1 · First visit', short: '1 · First visit' },
  { id: 'r2', title: 'Request 2 · Cache hit', short: '2 · Cache hit' },
  { id: 'r3', title: 'Request 3 · Expired', short: '3 · Expired' },
  { id: 'recap', title: 'Recap', short: 'Recap' },
];

const beats = [
  // Intro
  { id: 'i1', ch: 'intro', lab: 'Cache first',
    text: 'When a page needs a file, the browser checks its own cache first.',
    v: 'A browser with an unstyled page that needs style.css. An arrow goes into the HTTP cache box before the server.' },
  { id: 'i2', ch: 'intro', lab: 'One question',
    text: 'One question decides what happens next. Do I have a fresh copy?',
    v: 'The question card: "Do I have a fresh copy?"' },
  { id: 'i3', ch: 'intro', lab: 'Three requests', hold: 1.0,
    text: "Let's follow one file, style.css, through three requests.",
    v: 'Three cards: 1 First visit (no copy), 2 Cache hit (fresh copy), 3 Expired (stale copy).' },

  // Request 1
  { id: 'r1a', ch: 'r1', lab: 'Cache miss',
    text: 'First visit. The cache is empty, so this is a cache miss.',
    v: 'Main diagram: browser and cache on the left, network in the middle, server on the right. The cache drawer is empty. MISS stamp.' },
  { id: 'r1b', ch: 'r1', lab: 'Request sent',
    text: 'So the browser sends GET /style.css to the server.',
    v: 'The request card "GET /style.css" travels to the server.' },
  { id: 'r1c', ch: 'r1', lab: '200 OK', hold: 0.4,
    text: 'The server replies 200 OK with the whole file and two caching headers.',
    v: 'The response card comes back: 200 OK, Cache-Control: max-age=3600, ETag: "a3f9", and a 48.2 kB body.' },
  { id: 'r1d', ch: 'r1', lab: 'max-age', hold: 0.6,
    text: "Cache-Control: max-age=3600 says the copy stays fresh for 3600 seconds. That's one hour.",
    v: 'The max-age line lights up. The freshness timeline appears and draws a fresh window from 0 to 1 hour.' },
  { id: 'r1e', ch: 'r1', lab: 'ETag',
    text: 'The ETag is a version tag for this exact file, like a fingerprint.',
    v: 'The ETag line lights up. Callout: "a3f9" names this version of style.css.' },
  { id: 'r1f', ch: 'r1', lab: 'Copy saved', hold: 0.4,
    text: 'The browser uses the file and saves a copy, headers and all. Its age starts at zero.',
    v: 'The page gets its styles. The response shrinks into the cache as a stored copy: ETag "a3f9", max-age=3600, age 0 s, FRESH.' },
  { id: 'r1g', ch: 'r1', lab: 'DevTools: 200',
    text: 'DevTools logs it as status 200, with the full size.',
    v: 'DevTools row 1: style.css, 200, stylesheet, 48.2 kB.' },

  // Request 2
  { id: 'r2a', ch: 'r2', lab: '10 minutes later',
    text: 'Ten minutes later, you open another page. It needs style.css too.',
    v: '"+10 min" chip. The address bar changes to /about. The age counts up from 0 to 600 s. The timeline cursor moves to 10 min.' },
  { id: 'r2b', ch: 'r2', lab: 'Still fresh', hold: 0.4,
    text: "The copy is 600 seconds old. That's under 3600, so it's still fresh.",
    v: 'Comparison: age 600 s < max-age 3600 s. FRESH stamp.' },
  { id: 'r2c', ch: 'r2', lab: 'Use the copy',
    text: 'So the browser uses its copy right away. Nothing goes over the network.',
    v: 'The copy jumps from the cache to the page. The network lane shows "no request". The server stays dim.' },
  { id: 'r2d', ch: 'r2', lab: 'Cache hit',
    text: "That's a cache hit. No network wait, and no data used.",
    v: 'HIT stamp. Chips: 0 round trips, 0 bytes downloaded.' },
  { id: 'r2e', ch: 'r2', lab: 'DevTools: disk cache', hold: 0.6,
    text: 'DevTools still lists it, but the Size column says disk cache or memory cache.',
    v: 'DevTools row 2: style.css, 200 (dimmed), stylesheet, (disk cache).' },

  // Request 3
  { id: 'r3a', ch: 'r3', lab: '2 hours later',
    text: 'Later, you come back. The copy is now two hours old.',
    v: '"+2 h" chip. The address bar goes back to /. The age counts up to 7200 s. The timeline cursor moves past the fresh window.' },
  { id: 'r3b', ch: 'r3', lab: 'Stale, still stored', hold: 0.4,
    text: "That's past max-age, so the copy is stale. But stale does not mean deleted.",
    v: 'Comparison: age 7200 s > max-age 3600 s. STALE stamp. The copy stays in the cache, tagged "still stored".' },
  { id: 'r3c', ch: 'r3', lab: 'If-None-Match',
    text: 'So the browser checks with the server. It sends the stored ETag in an If-None-Match header.',
    v: 'The request card "GET /style.css" with If-None-Match: "a3f9" travels to the server.' },
  { id: 'r3d', ch: 'r3', lab: 'Changed?',
    text: 'In plain words: has the file changed since version a3f9?',
    v: 'A question bubble at the server: changed since "a3f9"? The server looks up its current ETag.' },
  { id: 'r3e', ch: 'r3', lab: '304 Not Modified',
    text: "The server's tag still matches. So it replies 304 Not Modified.",
    v: 'Comparison "a3f9" = "a3f9". The 304 card comes back.' },
  { id: 'r3f', ch: 'r3', lab: 'No body', hold: 0.6,
    text: "A 304 has no body. It's only headers, so it's tiny.",
    v: 'The 304 card shows an empty "no body" slot. Two bars to scale: 200 = 48.2 kB, 304 = 0.3 kB.' },
  { id: 'r3g', ch: 'r3', lab: 'Fresh again', hold: 0.4,
    text: "The browser reuses its copy and updates its headers. It's fresh for another hour.",
    v: 'The 304 merges into the stored copy. Age resets to 0 s, FRESH again. The timeline draws a new fresh window from 2 to 3 hours.' },
  { id: 'r3h', ch: 'r3', lab: 'DevTools: 304',
    text: 'DevTools shows status 304 and a small size.',
    v: 'DevTools row 3: style.css, 304, stylesheet, 0.3 kB.' },
  { id: 'r3i', ch: 'r3', lab: 'If the file changed', hold: 0.8,
    text: 'If the file had changed, the tags would differ. Then the server sends 200 with the new file and a new ETag.',
    v: 'Inset "What if it changed?": "a3f9" does not match "b71c". 200 OK with ETag "b71c" and a new body.' },

  // Recap
  { id: 'c1', ch: 'recap', lab: 'Three outcomes', hold: 1.6,
    text: 'Three requests. Three outcomes.',
    v: 'Three columns: 1 no copy, 200 with the whole file. 2 fresh copy, no request. 3 stale copy, 304 with headers only.' },
  { id: 'c2', ch: 'recap', lab: 'max-age picks', hold: 0.4,
    text: 'Your Cache-Control header picks the path. max-age=3600 means path two for an hour, then path three.',
    v: 'Chip "max-age=3600" with arrows to column 2 (first hour) and column 3 (after that).' },
  { id: 'c3', ch: 'recap', lab: 'no-cache',
    text: "With no-cache, the copy is stored but checked first, every time. That's path three.",
    v: 'Chip "no-cache" points to column 3. Note: no-cache still stores the copy.' },
  { id: 'c4', ch: 'recap', lab: 'no-store',
    text: 'With no-store, nothing is saved. Every request is a first visit.',
    v: 'Chip "no-store" points to column 1.' },
  { id: 'c5', ch: 'recap', lab: 'Disable cache',
    text: 'To replay a first visit yourself, tick Disable cache in DevTools.',
    v: 'DevTools toolbar: the "Disable cache" checkbox gets ticked. The row shows 200 and the full size again.' },
  { id: 'c6', ch: 'recap', lab: 'What each costs', hold: 2.0,
    text: 'A fresh hit uses no network. A 304 costs one small round trip. A 200 sends the whole file.',
    v: 'Final card with three costs: fresh hit = no network, 304 = one small round trip, 200 = the whole file.' },
];

// Reading-pace timing (no voice): words / 2.6 per second, at least 1.6 s, plus hold, 0.4 s gaps.
function time(list, voice) {
  let t = 0.4;
  for (const b of list) {
    const w = b.text.split(/\s+/).length;
    b.words = w;
    b.dur = voice && voice[b.id] ? voice[b.id] : Math.max(1.6, w / 2.6);
    b.start = t;
    b.end = t + b.dur + (b.hold || 0);
    t = b.end + 0.4;
  }
  return t;
}

module.exports = { chapters, beats, time };
