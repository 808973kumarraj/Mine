// Inlines the woff2 fonts into index.html so the page works offline.
// Usage: node tools/inline-fonts.mjs <dir-with-fontsource-woff2-files>
//   e.g. npm pack @fontsource/space-grotesk @fontsource/jetbrains-mono, untar,
//   then point this at a folder holding the */files/*.woff2 files.
import { readFileSync, writeFileSync, readdirSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const page = join(here, '..', 'index.html');
const root = process.argv[2];
if (!root) { console.error('usage: node tools/inline-fonts.mjs <font-dir>'); process.exit(1); }

const want = [
  ['Space Grotesk', 'space-grotesk', [500, 600, 700]],
  ['JetBrains Mono', 'jetbrains-mono', [400, 500, 600, 700]],
];
const files = [];
(function walk(d) {
  for (const f of readdirSync(d)) {
    const p = join(d, f);
    if (statSync(p).isDirectory()) walk(p); else if (f.endsWith('.woff2')) files.push(p);
  }
})(root);

const faces = [];
for (const [family, slug, weights] of want) {
  for (const w of weights) {
    const name = `${slug}-latin-${w}-normal.woff2`;
    const file = files.find(p => p.endsWith('/' + name));
    if (!file) { console.error('missing ' + name); process.exit(1); }
    const b64 = readFileSync(file).toString('base64');
    faces.push(`@font-face{font-family:"${family}";font-style:normal;font-weight:${w};font-display:block;src:url(data:font/woff2;base64,${b64}) format("woff2")}`);
  }
}

const html = readFileSync(page, 'utf8');
const out = html.replace(/<style id="fonts">[\s\S]*?<\/style>/, `<style id="fonts">\n/* Space Grotesk and JetBrains Mono, SIL Open Font License 1.1 */\n${faces.join('\n')}\n</style>`);
writeFileSync(page, out);
console.log(`inlined ${faces.length} font faces, index.html is ${(out.length / 1024).toFixed(0)} KB`);
