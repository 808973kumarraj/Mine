#!/usr/bin/env node
// Bundles real Motion Canvas scenes (.tsx) into ONE standalone HTML file.
// Usage: node build.mjs scene1.tsx [scene2.tsx ...] --out out.html [--title T] [--width 1920] [--height 1080] [--fps 60] [--background "#141414"]
import { build } from 'esbuild';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2), scenes = [], opt = { out: 'scene.html', title: 'Motion Canvas', width: 1920, height: 1080, fps: 60, background: '#141414' };
for (let i = 0; i < args.length; i++) args[i].startsWith('--') ? (opt[args[i].slice(2)] = args[++i]) : scenes.push(resolve(args[i]));
if (!scenes.length) { console.error('Give at least one scene .tsx file'); process.exit(1); }

const entry = `
import { bootstrap, makeProject, MetaFile, ValueDispatcher, Player, Stage, Vector2 } from '@motion-canvas/core';
${scenes.map((s, i) => `import s${i} from ${JSON.stringify(s)};`).join('\n')}
const scenes = [${scenes.map((_, i) => `s${i}`).join(',')}];
scenes.forEach((d, i) => { d.name = 'scene' + i; new MetaFile(d.name, false).attach(d.meta); d.onReplaced ??= new ValueDispatcher(d.config); });
const project = bootstrap('project', { core: '3', two: '3', ui: null, vitePlugin: null }, [], makeProject({ scenes }), new MetaFile('project', false), new MetaFile('settings', false));
window.__mc = { project, Player, Stage, Vector2, opt: ${JSON.stringify(opt)} };
`;
const res = await build({
  stdin: { contents: entry, resolveDir: here, loader: 'ts' },
  bundle: true, minify: true, format: 'iife', write: false, target: 'es2020',
  jsx: 'automatic', jsxImportSource: '@motion-canvas/2d/lib',
  nodePaths: [join(here, 'node_modules')], logLevel: 'error',
});
const js = res.outputFiles[0].text.replace(/<\/script/gi, '<\\/script');
const html = readFileSync(join(here, '../templates/player.html'), 'utf8')
  .replaceAll('{{TITLE}}', opt.title.replace(/[<&]/g, c => c === '<' ? '&lt;' : '&amp;'))
  .replace('/*{{BUNDLE}}*/', () => js);
writeFileSync(opt.out, html);
console.log(`Wrote ${opt.out} (${(html.length / 1024).toFixed(0)} KB)`);
