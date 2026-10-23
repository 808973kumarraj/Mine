#!/usr/bin/env node
// Builds index.html (one page: markup, CSS, fonts and script inline; GSAP from cdnjs),
// plus captions.srt and chapters.txt, from beats.js and src/.
//   node build.js
const fs = require('fs');
const path = require('path');
const { chapters, beats, time } = require('./beats.js');
const { stageHTML } = require('./src/stage.js');

const ROOT = __dirname;
const read = (p) => fs.readFileSync(path.join(ROOT, p), 'utf8');
const b64 = (p) => fs.readFileSync(path.join(ROOT, p)).toString('base64');
const r3 = (n) => Math.round(n * 1000) / 1000;
const GSAP_URL = 'https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js';

const TOTAL = r3(time(beats));
const fmtClock = (s) => Math.floor(s / 60) + ':' + String(Math.floor(s % 60)).padStart(2, '0');

// Captions: about seven words per chunk, cut at punctuation or before a joining word.
const JOIN = new Set(['with', 'and', 'so', 'but', 'then', 'because', 'or', 'for', 'since', 'while', 'when', 'if']);
function chunk(words, max = 9) {
  if (words.length <= max) return [words.join(' ')];
  const cuts = [];
  words.forEach((w, i) => {
    if (i === 0 || i >= words.length) return;
    if (/[,:;]$/.test(words[i - 1])) cuts.push({ i, punct: true });
    else if (JOIN.has(w.toLowerCase())) cuts.push({ i, punct: false });
  });
  if (!cuts.length) cuts.push({ i: Math.ceil(words.length / 2), punct: false });
  cuts.sort((a, b) => {
    const la = Math.max(a.i, words.length - a.i), lb = Math.max(b.i, words.length - b.i);
    return la - lb || Number(b.punct) - Number(a.punct);
  });
  const i = cuts[0].i;
  return [...chunk(words.slice(0, i), max), ...chunk(words.slice(i), max)];
}
function chunks(text) {
  return text.split(/(?<=[.?!])\s+/).flatMap((s) => chunk(s.trim().split(/\s+/)));
}
const CAPS = [];
for (const b of beats) {
  const parts = chunks(b.text);
  const counts = parts.map((p) => p.split(/\s+/).length);
  const sum = counts.reduce((a, c) => a + c, 0);
  let t = b.start;
  parts.forEach((p, i) => {
    const t1 = i === parts.length - 1 ? b.end : t + b.dur * (counts[i] / sum);
    CAPS.push({ t0: r3(t), t1: r3(t1), text: p });
    t = t1;
  });
}

const fonts = `
@font-face { font-family: 'Bricolage Grotesque'; font-style: normal; font-weight: 400 800; font-display: block; src: url(data:font/woff2;base64,${b64('assets/fonts/bricolage-latin.woff2')}) format('woff2'); }
@font-face { font-family: 'IBM Plex Mono'; font-style: normal; font-weight: 400; font-display: block; src: url(data:font/woff2;base64,${b64('assets/fonts/plexmono-400-latin.woff2')}) format('woff2'); }
@font-face { font-family: 'IBM Plex Mono'; font-style: normal; font-weight: 600; font-display: block; src: url(data:font/woff2;base64,${b64('assets/fonts/plexmono-600-latin.woff2')}) format('woff2'); }
@font-face { font-family: 'Instrument Serif'; font-style: italic; font-weight: 400; font-display: block; src: url(data:font/woff2;base64,${b64('assets/fonts/instrument-serif-italic-latin.woff2')}) format('woff2'); }`;

const ICON = {
  play: '<svg class="i-play" viewBox="0 0 20 20" aria-hidden="true"><path d="M6 3.5v13l10.5-6.5z" fill="currentColor"/></svg>',
  pause: '<svg class="i-pause" viewBox="0 0 20 20" aria-hidden="true" hidden><path d="M5 3.5h3.5v13H5zM11.5 3.5H15v13h-3.5z" fill="currentColor"/></svg>',
  replay: '<svg class="i-replay" viewBox="0 0 20 20" aria-hidden="true" hidden><path d="M4.6 10.4a5.6 5.6 0 1 0 1.9-4.6" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"/><path d="M3.2 3v4.6h4.6" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  prev: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M12.5 4L6.5 10l6 6" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  next: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M7.5 4l6 6-6 6" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  fs: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M3 8V3h5M17 8V3h-5M3 12v5h5M17 12v5h-5" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>',
};

const DATA = {
  BEATS: beats.map((b) => ({ id: b.id, ch: b.ch, lab: b.lab, text: b.text, start: r3(b.start), dur: r3(b.dur), end: r3(b.end) })),
  CHAPTERS: chapters.map((c) => ({ id: c.id, title: c.title, short: c.short })),
  CAPS,
};

const sources = `
  <details class="sources">
    <summary>Sources and example values</summary>
    <p>The site, the file, max-age=3600, the ETags "a3f9" and "b71c", the sizes 48.2 kB and 0.3 kB, and the visit times are example values.</p>
    <ul>
      <li><a href="https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Caching" target="_blank" rel="noopener">MDN: HTTP caching</a></li>
      <li><a href="https://web.dev/articles/http-cache" target="_blank" rel="noopener">web.dev: Prevent unnecessary network requests with the HTTP Cache</a></li>
      <li><a href="https://www.rfc-editor.org/rfc/rfc9111.html" target="_blank" rel="noopener">RFC 9111: HTTP Caching</a> (fresh and stale, validation, max-age, no-cache, no-store)</li>
      <li><a href="https://www.rfc-editor.org/rfc/rfc9110.html#section-15.4.5" target="_blank" rel="noopener">RFC 9110: 304 Not Modified</a>, <a href="https://www.rfc-editor.org/rfc/rfc9110.html#section-13.1.2" target="_blank" rel="noopener">If-None-Match</a>, <a href="https://www.rfc-editor.org/rfc/rfc9110.html#section-8.8.3" target="_blank" rel="noopener">ETag</a></li>
      <li><a href="https://developer.chrome.com/docs/devtools/network/reference#disable-cache" target="_blank" rel="noopener">Chrome DevTools: Disable cache</a></li>
      <li><a href="https://github.com/ChromeDevTools/devtools-frontend/blob/main/front_end/panels/network/NetworkDataGridNode.ts" target="_blank" rel="noopener">Chrome DevTools source: the (disk cache) and (memory cache) labels</a></li>
    </ul>
  </details>`;

const html = `<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>How Browser Caching Works</title>
<meta name="claude:viewport-fit-sides" content="cover">
<meta name="description" content="An animated explainer for junior developers: one file, three requests (a first visit, a cache hit, and an expired copy revalidated with a 304).">
<style>${fonts}
${read('src/page.css')}
${read('src/stage.css')}</style>
<main class="wrap">
  <div class="deck" id="deck">
    <div class="stage" id="stage" role="region" aria-label="Animated explainer: how browser caching works">
      <div class="canvas" id="canvas">
${stageHTML({ chapters, totalLabel: fmtClock(TOTAL) })}
      </div>
    </div>
    <div class="player" role="group" aria-label="Player">
      <button id="btn-play" class="pbtn primary" type="button" aria-label="Play" title="Play or pause (Space)">${ICON.play}${ICON.pause}${ICON.replay}</button>
      <span class="time" id="time">0:00 / ${fmtClock(TOTAL)}</span>
      <div class="scrub"><input type="range" id="scrub" min="0" max="1000" step="1" value="0" aria-label="Seek"><div class="marks" id="marks"></div></div>
      <button id="btn-cc" class="pbtn" type="button" aria-pressed="true" title="Captions (C)">CC</button>
      <label class="speed"><span class="sr">Playback speed</span><select id="speed"><option value="0.75">0.75x</option><option value="1" selected>1x</option><option value="1.25">1.25x</option><option value="1.5">1.5x</option></select></label>
      <button id="btn-fs" class="pbtn" type="button" aria-label="Full screen" title="Full screen">${ICON.fs}</button>
    </div>
    <div class="stepbar" role="group" aria-label="Step through each request">
      <button id="btn-prev" class="pbtn" type="button" title="Previous step (Left arrow)">${ICON.prev}<span>Prev step</span></button>
      <div class="tabs" id="tabs"></div>
      <button id="btn-next" class="pbtn primary" type="button" title="Next step (Right arrow)"><span>Next step</span>${ICON.next}</button>
      <div class="steprow">
        <span class="steplabel" id="steplabel"></span>
        <label class="toggle"><input type="checkbox" id="stepmode"> Pause after each step</label>
      </div>
    </div>
  </div>
  <p class="sr" id="live" aria-live="polite"></p>
${sources}
</main>
<script src="${GSAP_URL}"></script>
<script>
const BEATS = ${JSON.stringify(DATA.BEATS)};
const CHAPTERS = ${JSON.stringify(DATA.CHAPTERS)};
const CAPS = ${JSON.stringify(DATA.CAPS)};
const TOTAL = ${TOTAL};
if (!window.gsap) {
  document.getElementById('poster').hidden = true;
  document.getElementById('stage').insertAdjacentHTML('beforeend', '<p class="nogsap">The animation library did not load. Check your connection, then reload the page.</p>');
} else {
${read('src/timeline.js')}
${read('src/player.js')}
}
</script>
`;

fs.writeFileSync(path.join(ROOT, 'index.html'), html);

const srtTime = (s) => {
  const ms = Math.round(s * 1000);
  const h = Math.floor(ms / 3600000), m = Math.floor((ms % 3600000) / 60000), sec = Math.floor((ms % 60000) / 1000);
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')},${String(ms % 1000).padStart(3, '0')}`;
};
fs.writeFileSync(
  path.join(ROOT, 'captions.srt'),
  CAPS.map((c, i) => `${i + 1}\n${srtTime(c.t0)} --> ${srtTime(c.t1)}\n${c.text}\n`).join('\n'),
);
fs.writeFileSync(
  path.join(ROOT, 'chapters.txt'),
  chapters
    .map((c, i) => {
      const first = beats.find((b) => b.ch === c.id);
      return `${i === 0 ? '0:00' : fmtClock(first.start)} ${c.title}`;
    })
    .join('\n') + '\n',
);

const kb = (n) => (n / 1024).toFixed(0) + ' KB';
console.log(`index.html ${kb(Buffer.byteLength(html))}, ${beats.length} beats, ${CAPS.length} caption chunks, total ${fmtClock(TOTAL)} (${TOTAL}s)`);
