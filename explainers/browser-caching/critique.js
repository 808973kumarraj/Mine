#!/usr/bin/env node
// Headless check for index.html. Seeks to 0.35 s after each beat starts and 0.45 s before it
// ends, screenshots the stage, and lists page errors. GSAP is served from a local copy of the
// exact cdnjs file the page loads.
//   NODE_PATH=$(npm root -g) node critique.js --gsap <gsap.min.js> --out <dir> [--only r1a,r2b] [--reverse]
const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');

const args = process.argv.slice(2);
const opt = (name, def) => {
  const i = args.indexOf('--' + name);
  return i >= 0 ? args[i + 1] : def;
};
const flag = (name) => args.includes('--' + name);
const GSAP_URL = 'https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js';
const gsapPath = opt('gsap');
const out = opt('out', path.join(__dirname, 'shots'));
const only = opt('only') ? opt('only').split(',') : null;

(async () => {
  if (!gsapPath || !fs.existsSync(gsapPath)) throw new Error('pass --gsap <path to gsap 3.12.5 gsap.min.js>');
  fs.mkdirSync(out, { recursive: true });
  const browser = await chromium.launch();
  const context = await browser.newContext({ viewport: { width: 1632, height: 1120 }, deviceScaleFactor: 1 });
  await context.route('https://**/*', (route) =>
    route.request().url() === GSAP_URL
      ? route.fulfill({ path: gsapPath, contentType: 'application/javascript' })
      : route.abort(),
  );
  const page = await context.newPage();
  const errors = [];
  page.on('pageerror', (e) => errors.push('pageerror: ' + e.message));
  page.on('console', (m) => {
    if (m.type() === 'error' || m.type() === 'warning') errors.push(m.type() + ': ' + m.text());
  });
  await page.goto('file://' + path.join(__dirname, 'index.html'));
  await page.waitForFunction(() => window.__explainer, null, { timeout: 10000 });
  await page.evaluate(() => document.fonts.ready);
  await page.locator('#stage').screenshot({ path: path.join(out, '00-poster.png') });
  await page.evaluate(() => { document.getElementById('poster').hidden = true; });

  const beats = await page.evaluate(() => window.__explainer.beats);
  const frames = [];
  beats.forEach((b, i) => {
    if (only && !only.includes(b.id)) return;
    const n = String(i + 1).padStart(2, '0');
    frames.push({ file: `${n}-${b.id}-a.png`, t: b.start + 0.35 });
    frames.push({ file: `${n}-${b.id}-b.png`, t: b.end - 0.45 });
  });
  if (flag('reverse')) {
    await page.evaluate((t) => window.__explainer.seek(t), 1e6);
    frames.reverse().forEach((f) => { f.file = f.file.replace('.png', '-rev.png'); });
  }
  for (const f of frames) {
    await page.evaluate((t) => window.__explainer.seek(t), f.t);
    await page.locator('#stage').screenshot({ path: path.join(out, f.file) });
  }
  await browser.close();
  console.log(`${frames.length} frames -> ${out}`);
  console.log(errors.length ? errors.join('\n') : 'no page errors');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
