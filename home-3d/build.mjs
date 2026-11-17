// Readable single-file build:
//  1) the open-source libraries (three.js, three-gpu-pathtracer, three-mesh-bvh) are bundled — unminified —
//     into one script that exposes them as HOME3D_LIBS;
//  2) the app code (src/main.js) is inserted verbatim, with all of its comments, after that script.
import { build } from 'esbuild';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';

const OUT = 'dist/Home_3D_Minimalist_Concrete.html';

const main = readFileSync('src/main.js', 'utf8');
const importRe = /^import\s+(\*\s+as\s+(\w+)|\{([^}]*)\})\s+from\s+'([^']+)';\s*$/gm;
const vendorLines = [], names = [];
const app = main.replace(importRe, (m, what, star, list, from) => {
  vendorLines.push(m);
  if (star) { names.push(star); return `const ${star} = HOME3D_LIBS.${star};`; }
  const ids = list.split(',').map((x) => x.trim()).filter(Boolean);
  names.push(...ids);
  return `const { ${ids.join(', ')} } = HOME3D_LIBS;`;
});
// expose only the parts of three.js the app actually uses (keeps tree-shaking → smaller file)
const used = [...new Set([...main.matchAll(/\bTHREE\.(\w+)/g)].map((x) => x[1]))].sort();
const libs = names.map((n) => (n === 'THREE' ? `THREE: { ${used.map((u) => `${u}: THREE.${u}`).join(', ')} }` : n));
const vendorSrc = vendorLines.join('\n') + `\nwindow.HOME3D_LIBS = { ${libs.join(', ')} };\n`;
const r = await build({
  stdin: { contents: vendorSrc, resolveDir: process.cwd(), sourcefile: 'libraries.js', loader: 'js' },
  bundle: true, minify: false, format: 'iife', target: ['es2020'], write: false, legalComments: 'inline', charset: 'utf8',
});
let vendor = r.outputFiles[0].text;
// turn long one-line shader strings back into readable multi-line template literals
vendor = vendor.replace(/^(\s*(?:var|const|let) [\w$]+ = )("(?:[^"\\]|\\.)*\\n(?:[^"\\]|\\.)*");$/gm, (m, head, lit) => {
  const text = new Function('return ' + lit)();
  return head + '`' + text.replace(/\\/g, '\\\\').replace(/`/g, '\\`').replace(/\$\{/g, '\\${') + '`;';
});
const js = '\n/* ---------- open-source libraries (readable, unminified) ---------- */\n' + vendor +
  '\n/* ---------- the 3D home app ---------- */\n(() => {\n\'use strict\';\n' + app + '\n})();\n';
const html = readFileSync('src/template.html', 'utf8').replace('/*__BUNDLE__*/', () => js.replace(/<\/script/gi, '<\\/script'));
mkdirSync('dist', { recursive: true });
writeFileSync(OUT, html);
let m = 0; for (const l of html.split('\n')) m = Math.max(m, l.length);
console.log(OUT + ':', (html.length / 1024).toFixed(0) + ' KB, ' + html.split('\n').length + ' lines, longest line ' + m);
