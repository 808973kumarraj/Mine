/* The Energy Ledger: an interactive pop-up notebook about energy stores.
   three.js r147 and GSAP 3 are inlined by build.py; this file is the notebook itself.
   URL options: ?p=N opens at spread N, ?shot=1 stops the idle camera sway (for screenshots). */
(async () => {
'use strict';
const T = THREE;
const Q = new URLSearchParams(location.search);
const REDUCED = matchMedia('(prefers-reduced-motion: reduce)').matches;
const SHOT = Q.has('shot');
const $ = s => document.querySelector(s);
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const easeIO = t => t < 0.5 ? 2 * t * t : 1 - (-2 * t + 2) ** 2 / 2;
function bounceOut(t) {
  const n = 7.5625, d = 2.75;
  if (t < 1 / d) return n * t * t;
  if (t < 2 / d) return n * (t -= 1.5 / d) * t + 0.75;
  if (t < 2.5 / d) return n * (t -= 2.25 / d) * t + 0.9375;
  return n * (t -= 2.625 / d) * t + 0.984375;
}
function rng(seed) {
  return () => {
    seed = seed + 0x6D2B79F5 | 0;
    let t = Math.imul(seed ^ seed >>> 15, 1 | seed);
    t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
const hex = c => '#' + c.toString(16).padStart(6, '0');

const INK = 0x2b2721, INKC = '#2b2721', RED = '#a8492f', GREY = '#5b5347';
const PW = 1.4, PD = 1.9, BT = 0.03, KT = 0.05, PY = BT + KT + 0.0006;   // page size, board and page-block thickness
const GRAV = 10;                                                         // N/kg
const ST = {
  G: { name: 'Gravitational', col: 0x86b46f, css: '#4f7d3c' },
  K: { name: 'Kinetic', col: 0x6f9fd6, css: '#3d6ea5' },
  T: { name: 'Thermal', col: 0xe27d52, css: '#b5532c' },
  C: { name: 'Chemical', col: 0xa184cf, css: '#6f4fa6' },
  L: { name: 'Light', col: 0xf0c64a, css: '#9a7412' },
  B: { name: 'Hot bulb', col: 0xe27d52, css: '#b5532c' },
  W: { name: 'Warm wall', col: 0xf0a24a, css: '#a8662a' },
  R: { name: 'Whole room', col: 0xd9a483, css: '#9a6046' },
};
const SP = [
  { id: 'cover' },
  { id: 'swing', title: 'The swing', stores: ['G', 'K', 'T'] },
  { id: 'slide', title: 'Slide and stop', stores: ['G', 'K', 'T'] },
  { id: 'torch', title: 'Light and heat', stores: ['C', 'L', 'T'] },
  { id: 'room', title: 'Spread thin', stores: ['B', 'W', 'R'] },
  { id: 'end', title: 'The books balance', stores: [] },
];

// ---------- renderer ----------
const canvas = $('#gl');
let renderer;
try {
  renderer = new T.WebGLRenderer({ canvas, antialias: true, powerPreference: 'high-performance' });
} catch (e) { document.body.classList.add('nogl'); return; }
const DPR = Math.min(devicePixelRatio || 1, 2);
renderer.setPixelRatio(DPR);
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = T.PCFSoftShadowMap;
renderer.setClearColor(0xcdb28c);
const ANISO = Math.min(8, renderer.capabilities.getMaxAnisotropy());
const scene = new T.Scene();
scene.fog = new T.Fog(0xcdb28c, 7, 15);
const camera = new T.PerspectiveCamera(30, 1, 0.05, 60);
scene.add(new T.HemisphereLight(0xffffff, 0x8f8576, 0.56));
const sun = new T.DirectionalLight(0xffffff, 0.6);
sun.position.set(-1.8, 4.3, 2.9);
sun.target.position.set(0.2, 0, -0.1);
sun.castShadow = true;
sun.shadow.mapSize.set(2048, 2048);
Object.assign(sun.shadow.camera, { left: -2.3, right: 2.3, top: 2.3, bottom: -2.3, near: 1, far: 10 });
sun.shadow.bias = -0.0005;
sun.shadow.normalBias = 0.01;
scene.add(sun, sun.target);

// ---------- pencil-hatch material: lit tone → layers of world-space hatching ----------
const HATCH = `
uniform vec3 uInk; uniform float uInkAmt; uniform float uSp; uniform float uWash; uniform float uGlow; uniform vec3 uGlowCol;
varying vec3 vWPos; varying vec3 vWN;
float hR(float n){ return fract(sin(n * 91.3458) * 47453.5453); }
float hLine(vec2 p, vec2 dpx, vec2 dpy, float ang, float sp, float wid){
  vec2 d = vec2(cos(ang), sin(ang)), nn = vec2(-d.y, d.x);
  float along = dot(p, d) / sp;
  float u = dot(p, nn) / sp;
  u += 0.1 * sin(along * 0.31 + u * 1.7);
  float w = max((abs(dot(dpx, nn)) + abs(dot(dpy, nn))) / sp, 1e-4);
  float l = 1.0 - smoothstep(wid - w, wid + w, abs(fract(u) - 0.5));
  float k = floor(u);
  float gap = step(0.1, hR(k * 7.13 + floor(along / 11.0 + hR(k) * 11.0) * 1.37));
  return l * mix(gap, 1.0, smoothstep(0.15, 0.35, w));
}
float hStack(vec2 p, vec2 dpx, vec2 dpy, float s){
  if (s > 0.76) return 0.0;
  float wid = mix(0.07, 0.16, clamp(1.0 - s, 0.0, 1.0));
  float l = hLine(p, dpx, dpy, 0.7854, uSp, wid) * (1.0 - smoothstep(0.62, 0.76, s));
  if (s < 0.49) l = max(l, hLine(p, dpx, dpy, -0.7854, uSp, wid) * (1.0 - smoothstep(0.40, 0.49, s)));
  if (s < 0.33) l = max(l, hLine(p, dpx, dpy, 0.0, uSp * 0.8, wid) * (1.0 - smoothstep(0.25, 0.33, s)));
  return l;
}
float hatchAll(vec3 p, vec3 n, float s){
  vec3 dx = HDX(p), dy = HDY(p);
  vec3 w = pow(abs(n), vec3(4.0)); w /= (w.x + w.y + w.z + 1e-5);
  float h = 0.0;
  if (w.x > 0.01) h += w.x * hStack(p.zy, dx.zy, dy.zy, s);
  if (w.y > 0.01) h += w.y * hStack(p.xz, dx.xz, dy.xz, s);
  if (w.z > 0.01) h += w.z * hStack(p.xy, dx.xy, dy.xy, s);
  return h;
}
float hLum(vec3 c){ return dot(c, vec3(0.299, 0.587, 0.114)); }
`;
const inkU = { uInk: { value: new T.Color(INK) } };
function inkMat(color, o = {}) {
  const m = new T.MeshPhongMaterial({
    color, shininess: 0, specular: 0x000000, map: o.map || null, side: o.side ?? T.FrontSide,
    polygonOffset: true, polygonOffsetFactor: 1, polygonOffsetUnits: 1,
  });
  m.userData.u = {
    uInkAmt: { value: o.ink ?? 0.82 }, uSp: { value: o.sp ?? 0.017 }, uWash: { value: o.wash ?? 1 },
    uGlow: { value: 0 }, uGlowCol: { value: new T.Color(o.glowCol ?? 0xf08a4a) },
  };
  m.onBeforeCompile = sh => {
    Object.assign(sh.uniforms, inkU, m.userData.u);
    sh.vertexShader = sh.vertexShader
      .replace('#include <common>', '#include <common>\nvarying vec3 vWPos;\nvarying vec3 vWN;')
      .replace('#include <worldpos_vertex>', `#include <worldpos_vertex>
  {
    vec4 hp = vec4(transformed, 1.0);
    vec3 hn = objectNormal;
    #ifdef USE_INSTANCING
      hp = instanceMatrix * hp; hn = mat3(instanceMatrix) * hn;
    #endif
    vWPos = (modelMatrix * hp).xyz;
    vWN = normalize(mat3(modelMatrix) * hn);
  }`);
    sh.fragmentShader = sh.fragmentShader
      .replace('#include <common>', '#include <common>\n' + (renderer.capabilities.isWebGL2
        ? '#define HDX(a) dFdx(a)\n#define HDY(a) dFdy(a)\n' : '#define HDX(a) vec3(0.0015)\n#define HDY(a) vec3(0.0015)\n') + HATCH)
      .replace('#include <output_fragment>', `
  {
    float sL = hLum(outgoingLight) / max(hLum(diffuseColor.rgb), 0.03);
    float inkA = hatchAll(vWPos, normalize(vWN), sL);
    vec3 wash = mix(vec3(1.0), diffuseColor.rgb, uWash) * mix(0.86, 1.02, smoothstep(0.35, 1.0, sL));
    outgoingLight = mix(wash, uInk, inkA * uInkAmt);
    outgoingLight = mix(outgoingLight, uGlowCol, uGlow);
    float gr = fract(sin(dot(gl_FragCoord.xy, vec2(12.9898, 78.233))) * 43758.5453);
    outgoingLight *= 0.976 + 0.04 * gr;
  }
  #include <output_fragment>`);
  };
  m.customProgramCacheKey = () => 'ink1';
  return m;
}

// ink outlines: hard edges as screen-space lines, round shapes as an inverted hull
const LMs = [];
function lineMat(px, color = INK, opacity = 0.92) {
  const m = new T.LineMaterial({ color, linewidth: px, transparent: true, opacity, depthWrite: false });
  m.userData.px = px; LMs.push(m); return m;
}
const LM = lineMat(1.5), LMthin = lineMat(1.1, INK, 0.8);
function edgeLines(geo, thr = 28) {
  const e = new T.EdgesGeometry(geo, thr);
  return new T.LineSegmentsGeometry().setPositions(e.attributes.position.array);
}
function addEdges(mesh, thr = 28, mat = LM) {
  const l = new T.LineSegments2(edgeLines(mesh.geometry, thr), mat);
  l.renderOrder = 1; mesh.add(l); return l;
}
const hullMats = {};
function hullMat(t) {
  if (hullMats[t]) return hullMats[t];
  const m = new T.MeshBasicMaterial({ color: INK, side: T.BackSide });
  m.onBeforeCompile = sh => {
    sh.vertexShader = sh.vertexShader.replace('#include <begin_vertex>', `#include <begin_vertex>\ntransformed += normalize(normal) * ${t.toFixed(4)};`);
  };
  m.customProgramCacheKey = () => 'hull' + t;
  return (hullMats[t] = m);
}
function addHull(mesh, t = 0.004) {
  const h = new T.Mesh(mesh.geometry, hullMat(t)); h.castShadow = h.receiveShadow = false; mesh.add(h); return h;
}
function mk(geo, mat, o = {}) {
  const m = new T.Mesh(geo, mat);
  m.castShadow = o.cast ?? true; m.receiveShadow = o.recv ?? true;
  if (o.edges !== false) addEdges(m, o.thr ?? 28, o.lm ?? LM);
  if (o.hull) addHull(m, o.hull);
  if (o.pos) m.position.set(...o.pos);
  if (o.rot) m.rotation.set(...o.rot);
  return m;
}
const box = (w, h, d) => new T.BoxGeometry(w, h, d);
const alongX = g => g.rotateZ(-Math.PI / 2);   // cylinders point along +x
const MAT = {
  paper: inkMat(0xf6f0e2), wall: inkMat(0xe7e1d3), wood: inkMat(0xdcbb8e), woodDark: inkMat(0xa9805a),
  red: inkMat(0xc9573f), teal: inkMat(0x5f9c95), grey: inkMat(0xbdbcb5), dark: inkMat(0x57524b, { ink: 0.6 }),
  yellow: inkMat(0xf0c64a), purple: inkMat(0xa184cf), cloth: inkMat(0xb4583f, { ink: 0.75 }), pink: inkMat(0xe39a95),
};
MAT.shell = inkMat(0x5f9c95, { side: T.DoubleSide });

// ---------- canvas textures ----------
await Promise.all(['700 60px Caveat', '500 60px Caveat', '600 60px Newsreader', '500 40px Newsreader', 'italic 400 40px Newsreader']
  .map(f => document.fonts.load(f).catch(() => null)));
function makeCanvas(w, h) { const c = document.createElement('canvas'); c.width = w; c.height = h; return c; }
function canvasTex(w, h, draw) {
  const c = makeCanvas(w, h); draw(c.getContext('2d'), w, h);
  const t = new T.CanvasTexture(c); t.anisotropy = ANISO; return t;
}
const CW = 1024, CH = Math.round(CW * PD / PW);
const pxR = x => (x - 0.01) / PW * CW, pxL = x => (x + 1.41) / PW * CW, pz = z => (z + PD / 2) / PD * CH;

const paperBase = makeCanvas(CW, CH);
{
  const x = paperBase.getContext('2d'), r = rng(7);
  x.fillStyle = '#f7f1e3'; x.fillRect(0, 0, CW, CH);
  for (let i = 0; i < 3200; i++) {
    x.fillStyle = `rgba(110,90,60,${(r() * 0.07).toFixed(3)})`;
    x.fillRect(r() * CW, r() * CH, 1 + r() * 2.2, 1 + r() * 0.8);
  }
  x.fillStyle = 'rgba(95,82,60,0.22)';
  for (let gy = 50; gy < CH - 30; gy += 41) for (let gx = 36; gx < CW - 24; gx += 41) { x.beginPath(); x.arc(gx, gy, 1.7, 0, 6.283); x.fill(); }
}
function pen(x, col = INKC, lw = 3.2) { x.strokeStyle = col; x.fillStyle = col; x.lineWidth = lw; x.lineCap = 'round'; x.lineJoin = 'round'; }
function sk(x, pts, r, j = 1.6, passes = 2) {
  for (let p = 0; p < passes; p++) {
    x.beginPath();
    pts.forEach(([a, b], i) => { const ja = (r() - 0.5) * j, jb = (r() - 0.5) * j; i ? x.lineTo(a + ja, b + jb) : x.moveTo(a + ja, b + jb); });
    x.stroke();
  }
}
function skLine(x, a, b, c, d, r, j = 1.6) {
  const n = Math.max(2, Math.round(Math.hypot(c - a, d - b) / 30)), pts = [];
  for (let i = 0; i <= n; i++) pts.push([a + (c - a) * i / n, b + (d - b) * i / n]);
  sk(x, pts, r, j);
}
function skCircle(x, cx, cy, rad, r, j = 1.6) {
  const n = Math.max(16, Math.round(rad / 4)), a0 = r() * 6.28, pts = [];
  for (let i = 0; i <= n + 1; i++) { const a = a0 + i / n * 6.283; pts.push([cx + Math.cos(a) * rad, cy + Math.sin(a) * rad]); }
  sk(x, pts, r, j);
}
function skArrow(x, a, b, c, d, r, head = 18) {
  skLine(x, a, b, c, d, r);
  const ang = Math.atan2(d - b, c - a);
  for (const s of [-1, 1]) skLine(x, c, d, c - Math.cos(ang + s * 0.45) * head, d - Math.sin(ang + s * 0.45) * head, r, 1);
}
function skWave(x, x0, y0, len, r, amp = 9) {
  const pts = []; for (let i = 0; i <= 16; i++) pts.push([x0 + Math.sin(i / 16 * Math.PI * 2.4) * amp, y0 - i / 16 * len]);
  sk(x, pts, r, 1.2);
}
function hand(x, t, px, py, size, o = {}) {
  x.font = `${o.w || 700} ${size}px Caveat`; x.fillStyle = o.col || INKC;
  x.textAlign = o.al || 'left'; x.textBaseline = o.bl || 'alphabetic'; x.fillText(t, px, py);
  return x.measureText(t).width;
}
function serif(x, t, px, py, size, o = {}) {
  x.font = `${o.it ? 'italic ' : ''}${o.w || 500} ${size}px Newsreader`; x.fillStyle = o.col || INKC;
  x.textAlign = o.al || 'left'; x.textBaseline = o.bl || 'alphabetic';
  if ('ls' in o) x.letterSpacing = o.ls + 'px';
  x.fillText(t, px, py); x.letterSpacing = '0px';
}
function gutter(x, side) {
  const g = side === 'R' ? x.createLinearGradient(0, 0, 110, 0) : x.createLinearGradient(CW, 0, CW - 110, 0);
  g.addColorStop(0, 'rgba(80,60,35,0.22)'); g.addColorStop(1, 'rgba(80,60,35,0)');
  x.fillStyle = g; side === 'R' ? x.fillRect(0, 0, 110, CH) : x.fillRect(CW - 110, 0, 110, CH);
}
function heading(x, num, title) {
  const r = rng(31 + num), x0 = num ? 176 : 84;
  if (num) { pen(x, RED, 3); skCircle(x, 114, 150, 40, r, 2); hand(x, String(num), 114, 170, 62, { al: 'center', col: RED }); }
  const w = hand(x, title, x0, 176, 92);
  pen(x, 'rgba(43,39,33,0.7)', 3); skLine(x, x0, 200, Math.min(CW - 70, x0 + w + 12), 197, r, 2.4);
}
function pageNo(x, n, side) { serif(x, String(n), side === 'R' ? CW - 64 : 64, CH - 44, 26, { col: '#8a7d68', al: 'center' }); }

// left pages: the notebook's own pencil notes for each spread
const LEFT = {
  swing(x, r) {
    heading(x, 1, 'The swing');
    const cx = 512, cy = 280, R = 330;
    pen(x, 'rgba(43,39,33,0.5)', 2.4); x.setLineDash([10, 12]);
    x.beginPath(); x.arc(cx, cy, R, Math.PI / 2 - 0.95, Math.PI / 2 + 0.95); x.stroke(); x.setLineDash([]);
    pen(x, INKC, 4); skLine(x, cx - 130, cy, cx + 130, cy, r, 1.5);
    for (const [a, t, s] of [[-0.95, 'all G', 'G'], [0, 'all K', 'K'], [0.95, 'all G', 'G']]) {
      const bx = cx + Math.sin(a) * R, by = cy + Math.cos(a) * R;
      pen(x, 'rgba(43,39,33,0.75)', 2.4); skLine(x, cx, cy, bx, by, r, 1.2);
      x.fillStyle = hex(ST[s].col) + '88'; x.beginPath(); x.arc(bx, by, 28, 0, 6.283); x.fill();
      pen(x, INKC, 3); skCircle(x, bx, by, 30, r, 1.6);
      hand(x, t, bx, by + 80, 46, { al: 'center', col: ST[s].css });
    }
    hand(x, 'E = m × g × h', 110, 800, 60);
    hand(x, '= 2 kg × 10 N/kg × 1 m', 150, 872, 50, { w: 500 });
    hand(x, '= 20 J', 150, 944, 60);
    pen(x, RED, 3); skLine(x, 150, 962, 300, 959, r, 2);
    hand(x, 'G + K = 20 J', 110, 1086, 66, { col: RED });
    hand(x, 'the whole way through the swing', 112, 1140, 40, { w: 500, col: GREY });
    hand(x, 'with air: G + K + thermal = 20 J', 112, 1222, 42, { w: 500, col: GREY });
  },
  slide(x, r) {
    heading(x, 2, 'Slide and stop');
    pen(x, INKC, 3.2);
    sk(x, [[110, 560], [110, 330], [430, 560], [110, 560]], r, 1.5);
    skLine(x, 430, 560, 940, 560, r, 1.5);
    pen(x, 'rgba(43,39,33,0.6)', 2); for (let k = 0; k < 27; k++) skLine(x, 452 + k * 18, 574, 442 + k * 18, 592, r, 0.8);
    pen(x, INKC, 3); sk(x, [[132, 318], [196, 360], [186, 376], [122, 334], [132, 318]], r, 1);
    hand(x, 'G', 140, 290, 60, { col: ST.G.css });
    pen(x, ST.K.css, 3.2); skArrow(x, 480, 520, 650, 520, r);
    hand(x, 'K', 565, 500, 54, { col: ST.K.css, al: 'center' });
    pen(x, ST.T.css, 3); for (const sx of [770, 815, 860]) skWave(x, sx, 548, 70, r);
    hand(x, 'thermal', 815, 455, 46, { col: ST.T.css, al: 'center' });
    hand(x, 'work done = force × distance', 100, 770, 54);
    [['plastic', '5 N × 4 m = 20 J'], ['carpet', '10 N × 2 m = 20 J'], ['sandpaper', '20 N × 1 m = 20 J']].forEach(([a, b], k) => {
      hand(x, a, 130, 860 + k * 72, 46, { w: 500, col: GREY }); hand(x, b, 420, 860 + k * 72, 50);
    });
    hand(x, 'same 20 J, different distance', 100, 1130, 54, { col: RED });
  },
  torch(x, r) {
    heading(x, 3, 'Light and heat');
    x.fillStyle = hex(ST.C.col) + '66'; x.fillRect(110, 300, 380, 200);
    x.fillStyle = hex(ST.L.col) + 'aa'; x.fillRect(490, 300, 400, 22);
    x.fillStyle = hex(ST.T.col) + '77';
    x.beginPath(); x.moveTo(490, 322); x.bezierCurveTo(690, 322, 840, 420, 840, 700); x.lineTo(660, 700);
    x.bezierCurveTo(660, 560, 600, 500, 490, 500); x.closePath(); x.fill();
    pen(x, INKC, 3);
    sk(x, [[110, 300], [890, 300]], r); sk(x, [[890, 300], [912, 311], [890, 322]], r, 1);
    sk(x, [[110, 300], [110, 500], [490, 500]], r);
    x.beginPath(); x.moveTo(490, 322); x.lineTo(890, 322); x.stroke();
    x.beginPath(); x.moveTo(490, 322); x.bezierCurveTo(690, 322, 840, 420, 840, 700); x.stroke();
    x.beginPath(); x.moveTo(490, 500); x.bezierCurveTo(600, 500, 660, 560, 660, 700); x.stroke();
    sk(x, [[640, 700], [750, 750], [860, 700]], r, 1);
    hand(x, 'chemical 20 J', 300, 420, 52, { al: 'center', col: ST.C.css });
    hand(x, 'light 2 J', 900, 290, 44, { al: 'right', col: ST.L.css });
    hand(x, 'heat 18 J', 750, 810, 50, { al: 'center', col: ST.T.css });
    hand(x, 'filament bulb', 110, 560, 38, { w: 500, col: GREY });
    hand(x, 'efficiency =', 100, 940, 54);
    hand(x, 'useful energy out', 400, 912, 44, { w: 500 });
    pen(x, INKC, 2.6); skLine(x, 396, 930, 700, 930, r, 1.2);
    hand(x, 'total energy in', 420, 975, 44, { w: 500 });
    hand(x, 'filament: 2 ÷ 20 = 10%', 100, 1090, 52);
    hand(x, 'LED: 6 ÷ 20 = 30%', 100, 1160, 52, { col: RED });
  },
  room(x, r) {
    heading(x, 4, 'Spread thin');
    pen(x, INKC, 3);
    sk(x, [[150, 640], [150, 300], [700, 300], [700, 640]], r);
    sk(x, [[150, 640], [80, 720]], r); sk(x, [[700, 640], [880, 720]], r); sk(x, [[700, 300], [880, 260], [880, 720]], r);
    x.fillStyle = hex(ST.T.col) + 'aa'; x.beginPath(); x.arc(300, 560, 34, 0, 6.283); x.fill();
    skCircle(x, 300, 560, 36, r);
    pen(x, ST.T.css, 2.6);
    for (let k = 0; k < 7; k++) { const a = -2.6 + k * 0.62; skArrow(x, 300 + Math.cos(a) * 52, 560 + Math.sin(a) * 52, 300 + Math.cos(a) * 120, 560 + Math.sin(a) * 120, r, 12); }
    const rr = rng(4);
    x.fillStyle = hex(ST.T.col) + 'cc';
    for (let k = 0; k < 70; k++) { const px = 180 + rr() * 680, py = 320 + rr() * 380; if (Math.hypot(px - 300, py - 560) < 140) continue; x.beginPath(); x.arc(px, py, 4, 0, 6.283); x.fill(); }
    hand(x, 'start: 20 J in one hot bulb', 100, 820, 50);
    hand(x, 'end: 20 J all over the room', 100, 892, 50);
    hand(x, 'room: under 0.001 °C warmer', 100, 990, 50, { col: ST.T.css });
    hand(x, 'still there, just too spread out to use', 100, 1090, 44, { w: 500, col: GREY });
    hand(x, '= dissipation', 100, 1172, 60, { col: RED });
  },
  end(x, r) {
    heading(x, 0, 'The books balance');
    hand(x, 'energy in = energy out', 96, 380, 72, { col: RED });
    ['the swing', 'slide and stop', 'light and heat', 'spread thin'].forEach((t, k) => {
      const y = 520 + k * 96;
      pen(x, INKC, 3); sk(x, [[110, y - 34], [150, y - 34], [150, y + 6], [110, y + 6], [110, y - 34]], r, 1.4);
      pen(x, ST.G.css, 4); sk(x, [[116, y - 14], [128, y], [160, y - 44]], r, 1);
      hand(x, t, 180, y, 52);
    });
    hand(x, 'Energy is never made or destroyed.', 96, 1000, 48, { w: 600 });
    hand(x, 'It moves from store to store,', 96, 1062, 46, { w: 500 });
    hand(x, 'and some always spreads out as heat.', 96, 1124, 46, { w: 500 });
  },
};
const COLS = [0.32, 0.72, 1.12], STACK_Z = 0.6;
const RIGHT = {
  swing(x) { serif(x, 'fig. 1   a 2 kg ball on a 2 m string', pxR(0.1), pz(-0.86), 25, { it: true, col: GREY }); },
  slide(x, r) {
    serif(x, 'fig. 2   a polished 1 m slope at 40°, then a mat', pxR(0.1), pz(-0.86), 25, { it: true, col: GREY });
    pen(x, 'rgba(43,39,33,0.7)', 2.4);
    for (let m = 0; m <= 4; m++) {
      const X = pxR(SL.xb + m * SL.SC), Y = pz(SL.Z + 0.175);
      skLine(x, X, Y, X, Y + 22, r, 0.8);
      hand(x, m + ' m', X, Y + 58, 34, { al: 'center', col: GREY, w: 600 });
    }
    skLine(x, pxR(SL.xb), pz(SL.Z + 0.175), pxR(SL.xb + 4 * SL.SC), pz(SL.Z + 0.175), r, 0.8);
  },
  torch(x) { serif(x, 'fig. 3   torch with the battery cut away', pxR(0.1), pz(-0.86), 25, { it: true, col: GREY }); },
  room(x, r) {
    serif(x, 'fig. 4   the room, just after switching off', pxR(0.1), pz(-0.9), 25, { it: true, col: GREY });
    pen(x, 'rgba(120,90,55,0.28)', 2);
    for (let z = -0.84; z < 0.26; z += 0.11) skLine(x, pxR(0.06), pz(z), pxR(1.37), pz(z), r, 0.6);
  },
  end() {},
};
function drawLedger(x, stores, id, r) {
  const y0 = pz(0.36), y1 = pz(0.9);
  pen(x, 'rgba(43,39,33,0.6)', 2);
  skLine(x, pxR(0.08), y0, pxR(1.34), y0, r, 0.8); skLine(x, pxR(0.08), y0 + 7, pxR(1.34), y0 + 7, r, 0.8);
  serif(x, 'LEDGER', pxR(0.09), y0 - 14, 25, { w: 600, col: GREY, ls: 5 });
  serif(x, '1 coin = 1 joule', pxR(1.33), y0 - 14, 25, { it: true, col: GREY, al: 'right' });
  if (id === 'end') {
    hand(x, 'before = after', CW / 2, pz(0.66), 92, { al: 'center', col: RED });
    hand(x, '20 J                    20 J', CW / 2, pz(0.81), 56, { al: 'center', col: GREY });
  }
  stores.forEach((s, k) => {
    const cx = pxR(COLS[k]), cy = pz(STACK_Z), rad = 0.062 / PW * CW;
    pen(x, 'rgba(43,39,33,0.42)', 2); x.setLineDash([7, 8]);
    x.beginPath(); x.ellipse(cx, cy, rad, rad, 0, 0, 6.283); x.stroke(); x.setLineDash([]);
    if (k) { pen(x, 'rgba(43,39,33,0.18)', 2); skLine(x, (pxR(COLS[k - 1]) + cx) / 2, y0 + 30, (pxR(COLS[k - 1]) + cx) / 2, y1 - 20, r, 0.6); }
    if (s === 'R') hand(x, 'spread around the room ↑', cx, cy + 8, 30, { al: 'center', col: ST.R.css });
  });
  pen(x, 'rgba(43,39,33,0.6)', 2); skLine(x, pxR(0.08), y1, pxR(1.34), y1, r, 0.8);
}
function drawPage(P, side, i) {
  const x = P.ctx, sp = SP[i], r = rng(500 + i * 7 + (side === 'R' ? 3 : 0));
  x.setTransform(1, 0, 0, 1, 0, 0); x.drawImage(paperBase, 0, 0); gutter(x, side);
  if (i > 0) {
    if (side === 'L') LEFT[sp.id](x, r);
    else { RIGHT[sp.id](x, r); if (sp.stores.length || sp.id === 'end') drawLedger(x, sp.stores, sp.id, r); }
    pageNo(x, i * 2 + (side === 'R' ? 1 : 0), side);
  }
  P.tex.needsUpdate = true;
}
function pageCanvas(mirror) {
  const c = makeCanvas(CW, CH), tex = new T.CanvasTexture(c);
  tex.anisotropy = ANISO;
  if (mirror) { tex.repeat.x = -1; tex.offset.x = 1; }
  return { c, ctx: c.getContext('2d'), tex };
}
const pL = pageCanvas(), pR = pageCanvas(), pF = pageCanvas(), pB = pageCanvas(true);

const coverTex = canvasTex(CW, CH, (x, w, h) => {
  const r = rng(3);
  x.fillStyle = '#b4583f'; x.fillRect(0, 0, w, h);
  for (let i = 0; i < 900; i++) { x.fillStyle = `rgba(60,25,15,${(r() * 0.08).toFixed(3)})`; x.fillRect(0, r() * h, w, 1); x.fillRect(r() * w, 0, 1, h); }
  const px = 140, py = 270, pw = w - 280, ph = 560;
  x.fillStyle = '#f4ecd9'; x.fillRect(px, py, pw, ph);
  pen(x, INKC, 3); x.strokeRect(px + 16, py + 16, pw - 32, ph - 32); pen(x, INKC, 1.5); x.strokeRect(px + 26, py + 26, pw - 52, ph - 52);
  serif(x, 'The Energy', w / 2, py + 205, 104, { w: 600, al: 'center' });
  serif(x, 'Ledger', w / 2, py + 318, 104, { w: 600, al: 'center' });
  pen(x, RED, 2.5); x.beginPath(); x.moveTo(w / 2 - 120, py + 368); x.lineTo(w / 2 + 120, py + 368); x.stroke();
  serif(x, 'a pop-up notebook', w / 2, py + 432, 42, { it: true, w: 400, al: 'center' });
  serif(x, 'about where energy goes', w / 2, py + 488, 36, { it: true, w: 400, al: 'center', col: GREY });
  const cy = py + ph + 240;
  x.fillStyle = '#e9c35a'; x.beginPath(); x.arc(w / 2, cy, 104, 0, 6.283); x.fill();
  pen(x, INKC, 4); x.beginPath(); x.arc(w / 2, cy, 104, 0, 6.283); x.stroke();
  pen(x, INKC, 2); x.beginPath(); x.arc(w / 2, cy, 86, 0, 6.283); x.stroke();
  hand(x, '1 J', w / 2, cy + 30, 92, { al: 'center' });
  serif(x, 'NOTES & EXPERIMENTS', w / 2, h - 150, 30, { w: 600, al: 'center', col: '#f4ecd9', ls: 6 });
});
coverTex.center.set(0.5, 0.5); coverTex.rotation = Math.PI;   // the cover face is read with the book closed

const deskTex = canvasTex(1024, 1024, (x, w, h) => {
  const r = rng(11);
  x.fillStyle = '#d2b58f'; x.fillRect(0, 0, w, h);
  for (let i = 0; i < 90; i++) {
    const y = r() * h, a = 2 + r() * 7, f = 0.004 + r() * 0.01, ph = r() * 6;
    x.strokeStyle = `rgba(120,85,50,${(0.06 + r() * 0.12).toFixed(3)})`; x.lineWidth = 1 + r() * 2.5;
    x.beginPath(); for (let X = 0; X <= w; X += 16) { const Y = y + Math.sin(X * f + ph) * a; X ? x.lineTo(X, Y) : x.moveTo(X, Y); } x.stroke();
  }
});
deskTex.wrapS = deskTex.wrapT = T.RepeatWrapping; deskTex.repeat.set(3, 3);
const edgeTex = canvasTex(256, 64, (x, w, h) => {
  x.fillStyle = '#f3ecdd'; x.fillRect(0, 0, w, h);
  x.strokeStyle = 'rgba(120,105,80,0.35)'; x.lineWidth = 1;
  for (let y = 3; y < h; y += 4) { x.beginPath(); x.moveTo(0, y); x.lineTo(w, y + 0.5); x.stroke(); }
});
const matTex = [
  canvasTex(512, 160, (x, w, h) => {      // plastic
    const r = rng(21); x.fillStyle = '#cdd8db'; x.fillRect(0, 0, w, h);
    x.strokeStyle = 'rgba(255,255,255,0.7)'; x.lineWidth = 3;
    for (let i = 0; i < 7; i++) { const X = r() * w; x.beginPath(); x.moveTo(X, 10); x.lineTo(X + 40, h - 10); x.stroke(); }
  }),
  canvasTex(512, 160, (x, w, h) => {      // carpet
    const r = rng(22); x.fillStyle = '#a95a4c'; x.fillRect(0, 0, w, h);
    for (let yy = 4; yy < h; yy += 7) for (let xx = 4; xx < w; xx += 7) { x.fillStyle = `rgba(70,25,20,${(0.2 + r() * 0.3).toFixed(2)})`; x.fillRect(xx + r() * 2, yy + r() * 2, 3, 3); }
  }),
  canvasTex(512, 160, (x, w, h) => {      // sandpaper
    const r = rng(23); x.fillStyle = '#cfb37b'; x.fillRect(0, 0, w, h);
    for (let i = 0; i < 9000; i++) { x.fillStyle = r() < 0.5 ? 'rgba(70,50,20,0.5)' : 'rgba(255,245,215,0.6)'; x.fillRect(r() * w, r() * h, 1.6, 1.6); }
  }),
];
const fxTexs = {
  squig: canvasTex(64, 128, (x, w, h) => {
    x.strokeStyle = '#c4532a'; x.lineWidth = 5; x.lineCap = 'round'; x.beginPath();
    for (let i = 0; i <= 20; i++) { const y = h - 8 - i * (h - 16) / 20, xx = w / 2 + Math.sin(i / 20 * Math.PI * 2.2) * w * 0.22; i ? x.lineTo(xx, y) : x.moveTo(xx, y); }
    x.stroke();
  }),
  dot: canvasTex(64, 64, (x, w, h) => {
    const g = x.createRadialGradient(w / 2, h / 2, 0, w / 2, h / 2, w / 2);
    g.addColorStop(0, 'rgba(255,245,190,1)'); g.addColorStop(0.35, 'rgba(255,214,90,0.85)'); g.addColorStop(1, 'rgba(255,200,60,0)');
    x.fillStyle = g; x.fillRect(0, 0, w, h);
  }),
  glow: canvasTex(64, 64, (x, w, h) => {
    const g = x.createRadialGradient(w / 2, h / 2, 0, w / 2, h / 2, w / 2);
    g.addColorStop(0, 'rgba(255,170,90,0.9)'); g.addColorStop(1, 'rgba(255,140,60,0)');
    x.fillStyle = g; x.fillRect(0, 0, w, h);
  }),
  curl: canvasTex(64, 64, (x, w, h) => {
    x.strokeStyle = 'rgba(70,64,56,0.85)'; x.lineWidth = 3.5; x.lineCap = 'round'; x.beginPath();
    for (let i = 0; i <= 40; i++) { const a = i / 40 * Math.PI * 3, rr = 3 + i * 0.5; const xx = 8 + i * 1.0 + Math.cos(a) * rr * 0.6, yy = h / 2 + Math.sin(a) * rr * 0.6; i ? x.lineTo(xx, yy) : x.moveTo(xx, yy); }
    x.stroke();
  }),
};

// ---------- desk, book, props ----------
const desk = new T.Mesh(new T.PlaneGeometry(16, 16), inkMat(0xffffff, { map: deskTex, ink: 0.55, sp: 0.022 }));
desk.rotation.x = -Math.PI / 2; desk.receiveShadow = true; scene.add(desk);

const book = new T.Group(); scene.add(book);
const BW = PW + 0.06, BD = PD + 0.08;
const blockMat = inkMat(0xffffff, { map: edgeTex, ink: 0.6 });
book.add(mk(box(BW, BT, BD), MAT.cloth, { pos: [BW / 2, BT / 2, 0] }));
book.add(mk(box(PW, KT, PD), blockMat, { pos: [0.01 + PW / 2, BT + KT / 2, 0] }));
const leftHalf = new T.Group(); leftHalf.position.set(0, PY, 0); book.add(leftHalf);
const coverFace = inkMat(0xffffff, { map: coverTex, ink: 0.6 });
leftHalf.add(mk(box(BW, BT, BD), [MAT.cloth, MAT.cloth, MAT.cloth, coverFace, MAT.cloth, MAT.cloth], { pos: [-BW / 2, BT / 2 - PY, 0] }));
leftHalf.add(mk(box(PW, KT, PD), blockMat, { pos: [-0.01 - PW / 2, BT + KT / 2 - PY, 0] }));
const pageGeo = dx => new T.PlaneGeometry(PW, PD).rotateX(-Math.PI / 2).translate(dx, 0, 0);
const leftPage = new T.Mesh(pageGeo(-0.01 - PW / 2), inkMat(0xffffff, { map: pL.tex, ink: 0.7 }));
leftPage.position.y = 0.0004; leftPage.receiveShadow = true; leftHalf.add(leftPage);
const rightPage = new T.Mesh(pageGeo(0.01 + PW / 2), inkMat(0xffffff, { map: pR.tex, ink: 0.7 }));
rightPage.position.y = PY + 0.0004; rightPage.receiveShadow = true; book.add(rightPage);
const spine = mk(box(0.014, 1, BD), MAT.cloth, { edges: false }); spine.position.x = -0.007; book.add(spine);

// the turning sheet: a strip of columns that bends as it rotates about the spine
const SN = 36, sheetPos = new Float32Array((SN + 1) * 6), sheetUV = new Float32Array((SN + 1) * 4), sheetIdx = [];
for (let i = 0; i <= SN; i++) { sheetUV.set([i / SN, 1, i / SN, 0], i * 4); }
for (let i = 0; i < SN; i++) { const a = i * 2, b = a + 1, c = a + 2, d = a + 3; sheetIdx.push(a, b, c, b, d, c); }
const sheetGeo = new T.BufferGeometry();
sheetGeo.setAttribute('position', new T.BufferAttribute(sheetPos, 3));
sheetGeo.setAttribute('uv', new T.BufferAttribute(sheetUV, 2));
sheetGeo.setIndex(sheetIdx);
const sheet = new T.Group();
const sheetFront = new T.Mesh(sheetGeo, inkMat(0xffffff, { map: pF.tex, ink: 0.7 }));
const sheetBack = new T.Mesh(sheetGeo, inkMat(0xffffff, { map: pB.tex, ink: 0.7, side: T.BackSide }));
sheetFront.castShadow = true; sheetFront.receiveShadow = sheetBack.receiveShadow = true;
sheet.add(sheetFront, sheetBack); sheet.visible = false; book.add(sheet);
function bendSheet(th) {
  let x = 0, y = 0; const du = PW / SN;
  for (let i = 0; i <= SN; i++) {
    sheetPos.set([x, y, -PD / 2, x, y, PD / 2], i * 6);
    const ph = th - 0.55 * Math.sin(th) * ((i + 0.5) / SN);
    x += du * Math.cos(ph); y += du * Math.sin(ph);
  }
  sheetGeo.attributes.position.needsUpdate = true;
  sheetGeo.computeVertexNormals(); sheetGeo.computeBoundingSphere();
  sheet.position.set(0.01 - 0.02 * th / Math.PI, PY + 0.0012, 0);
}
bendSheet(0);

// a pencil lying on the desk
{
  const p = new T.Group();
  p.add(mk(alongX(new T.CylinderGeometry(0.019, 0.019, 0.62, 6)), MAT.yellow, { thr: 20 }));
  p.add(mk(alongX(new T.CylinderGeometry(0.0195, 0.0195, 0.05, 16)), MAT.grey, { thr: 40, pos: [-0.335, 0, 0] }));
  p.add(mk(alongX(new T.CylinderGeometry(0.018, 0.018, 0.05, 16)), MAT.pink, { thr: 40, pos: [-0.385, 0, 0] }));
  p.add(mk(alongX(new T.CylinderGeometry(0.003, 0.019, 0.09, 6)), MAT.wood, { thr: 20, pos: [0.355, 0, 0] }));
  p.add(mk(alongX(new T.CylinderGeometry(0.001, 0.0055, 0.026, 8)), MAT.dark, { edges: false, pos: [0.39, 0, 0] }));
  p.position.set(1.86, 0.019, 0.25); p.rotation.y = 1.25; scene.add(p);
}

// ---------- pop-up models (one per spread) ----------
function piece(x, z, ...kids) { const g = new T.Group(); g.position.set(x, PY, z); kids.forEach(k => g.add(k)); return g; }
const DIO = {};
function diorama(id, pieces, extra) {
  const g = new T.Group(); pieces.forEach(p => g.add(p)); g.visible = false; book.add(g);
  return (DIO[id] = Object.assign({ g, pieces }, extra));
}
function cylBetween(a, b, rad, mat) {
  const len = a.distanceTo(b), m = mk(new T.CylinderGeometry(rad, rad, len, 6), mat, { edges: false });
  m.position.copy(a).add(b).multiplyScalar(0.5);
  m.quaternion.setFromUnitVectors(new T.Vector3(0, 1, 0), b.clone().sub(a).normalize());
  return m;
}

// 1. the swing: a ball on a string, hung from a wooden frame
const SW = (() => {
  const Z = -0.3, X0 = 0.71, TOP = 0.74, L = 0.5;
  const posts = [-0.6, 0.6].map(dx => {
    const p = piece(X0 + dx, Z, mk(box(0.1, 0.022, 0.34), MAT.woodDark, { pos: [0, 0.011, 0] }),
      mk(box(0.044, TOP + 0.03, 0.044), MAT.wood, { pos: [0, (TOP + 0.03) / 2, 0] }));
    for (const s of [-1, 1]) p.add(mk(box(0.026, 0.355, 0.026), MAT.wood, { pos: [0, 0.172, s * 0.075], rot: [-s * 0.44, 0, 0] }));
    return p;
  });
  const bar = piece(X0, Z, mk(box(1.25, 0.038, 0.038), MAT.woodDark, { pos: [0, TOP + 0.012, 0] }));
  const arm = new T.Group(); arm.position.y = TOP - 0.006;
  const hook = mk(new T.TorusGeometry(0.011, 0.0035, 6, 16), MAT.dark, { edges: false }); hook.rotation.y = Math.PI / 2;
  const str = mk(new T.CylinderGeometry(0.0026, 0.0026, L - 0.055, 6), MAT.dark, { edges: false }); str.position.y = -(L - 0.055) / 2;
  const bob = mk(new T.SphereGeometry(0.058, 32, 20), MAT.red, { edges: false, hull: 0.0045 }); bob.position.y = -L;
  arm.add(hook, str, bob);
  const guide = new T.LineSegments2(new T.LineSegmentsGeometry(), lineMat(1.3, INK, 0.45)); guide.position.y = TOP - 0.006;
  const armP = piece(X0, Z, arm, guide);
  return diorama('swing', [...posts, bar, armP], { arm, bob, guide, Z, X0, TOP, L });
})();
function setGuide(th0) {
  const a = Math.abs(th0), n = 44, pts = [];
  for (let i = 0; i < n; i += 2) {
    const a1 = -a + 2 * a * i / n, a2 = -a + 2 * a * (i + 1) / n;
    pts.push(SW.L * Math.sin(a1), -SW.L * Math.cos(a1), 0, SW.L * Math.sin(a2), -SW.L * Math.cos(a2), 0);
  }
  SW.guide.geometry.dispose(); SW.guide.geometry = new T.LineSegmentsGeometry().setPositions(pts);
}

// 2. slide and stop: a polished slope, a choice of mats, a toy sledge
const SL = (() => {
  const Z = -0.24, SC = 0.248, xa = 0.045, xb = 0.045 + 1.1918 * 0.248, xe = 1.4, H = 0.248;
  const sh = new T.Shape(); sh.moveTo(0, 0); sh.lineTo(xb - xa, 0); sh.lineTo(0, H); sh.closePath();
  const wg = new T.ExtrudeGeometry(sh, { depth: 0.3, bevelEnabled: false }); wg.translate(0, 0, -0.15);
  const ramp = piece(xa, Z, mk(wg, MAT.wood, { thr: 20 }), mk(box(0.04, H, 0.3), MAT.woodDark, { pos: [-0.02, H / 2, 0] }));
  const flagShape = new T.Shape(); flagShape.moveTo(0, 0); flagShape.lineTo(0.075, 0.024); flagShape.lineTo(0, 0.048); flagShape.closePath();
  const flag = piece(xa - 0.02, Z - 0.12,
    mk(new T.CylinderGeometry(0.004, 0.004, 0.17, 6), MAT.dark, { pos: [0, H + 0.085, 0], edges: false }),
    mk(new T.ShapeGeometry(flagShape), inkMat(0xc9573f, { side: T.DoubleSide }), { pos: [0.004, H + 0.12, 0] }));
  const matMesh = mk(box(xe - xb, 0.006, 0.3), inkMat(0xffffff, { map: matTex[0], ink: 0.7 }), { pos: [(xe - xb) / 2, 0.003, 0] });
  const warmGeo = new T.PlaneGeometry(1, 0.22).rotateX(-Math.PI / 2).translate(0.5, 0, 0);
  const warm = new T.Mesh(warmGeo, new T.MeshBasicMaterial({ color: 0xf07a3a, transparent: true, opacity: 0, depthWrite: false }));
  warm.position.y = 0.0068; warm.scale.x = 0.001;
  const matP = piece(xb, Z, matMesh, warm);
  const sled = new T.Group();
  for (const zz of [-0.03, 0.03]) {
    sled.add(mk(box(0.12, 0.007, 0.008), MAT.dark, { pos: [0, 0.0035, zz] }));
    const curl = mk(new T.TorusGeometry(0.017, 0.0035, 6, 12, Math.PI), MAT.dark, { edges: false });
    curl.position.set(0.06, 0.0175, zz); curl.rotation.z = -Math.PI / 2; sled.add(curl);
    for (const xx of [-0.035, 0.035]) sled.add(mk(box(0.006, 0.026, 0.006), MAT.dark, { pos: [xx, 0.018, zz], edges: false }));
  }
  sled.add(mk(box(0.1, 0.012, 0.078), MAT.red, { pos: [-0.005, 0.034, 0] }));
  const sledP = piece(0, Z, sled);
  return diorama('slide', [ramp, flag, matP, sledP], { sled, matMesh, warm, Z, SC, xa, xb, xe, H });
})();

// 3. light and heat: a torch with the battery cut away, shining on a card wall
function torchModel(cutaway) {
  const t = new T.Group(), o = { t };
  t.add(mk(alongX(new T.CylinderGeometry(0.052, 0.052, 0.035, 28)), MAT.dark, { thr: 30, hull: 0.003, pos: [0.0175, 0, 0] }));
  t.add(mk(alongX(new T.CylinderGeometry(0.052, 0.052, 0.24, 28, 1, true, 0, cutaway ? Math.PI : Math.PI * 2)),
    cutaway ? MAT.shell : MAT.teal, { thr: 30, edges: cutaway, hull: cutaway ? 0 : 0.003, pos: [0.155, 0, 0] }));
  if (cutaway) {
    t.add(mk(alongX(new T.CylinderGeometry(0.034, 0.034, 0.21, 24)), MAT.grey, { thr: 30, pos: [0.155, 0, 0] }));
    o.band = mk(alongX(new T.CylinderGeometry(0.0352, 0.0352, 0.17, 24).translate(0, 0.085, 0)), MAT.purple, { thr: 30 });
    o.band.position.x = 0.07; t.add(o.band);
    t.add(mk(alongX(new T.CylinderGeometry(0.011, 0.011, 0.014, 12)), MAT.grey, { thr: 30, pos: [0.267, 0, 0] }));
  }
  t.add(mk(alongX(new T.CylinderGeometry(0.068, 0.052, 0.1, 28)), MAT.teal, { thr: 30, hull: 0.003, pos: [0.325, 0, 0] }));
  o.lensMat = new T.MeshBasicMaterial({ color: 0xdcd6c6 });
  o.lens = new T.Mesh(new T.CircleGeometry(0.061, 28).rotateY(Math.PI / 2), o.lensMat); o.lens.position.x = 0.3755; t.add(o.lens);
  t.add(mk(new T.TorusGeometry(0.064, 0.005, 6, 28).rotateY(Math.PI / 2), MAT.dark, { edges: false, pos: [0.376, 0, 0] }));
  o.sw = mk(box(0.034, 0.014, 0.022), MAT.red, { pos: [0.3, 0.061, 0] }); t.add(o.sw);
  return o;
}
const TO = (() => {
  const Z = -0.28, K = 1.35, AXY = 0.25, X = 0.09;
  const m = torchModel(true); m.t.position.set(X, AXY, 0); m.t.scale.setScalar(K);
  const torchP = piece(0, Z, m.t);
  const stand = piece(X + 0.23, Z, mk(box(0.3, AXY - 0.07, 0.14), MAT.wood, { pos: [0, (AXY - 0.07) / 2, 0] }));
  const wall = piece(1.33, Z, mk(box(0.028, 0.56, 0.7), MAT.paper, { pos: [0, 0.28, 0] }), mk(box(0.15, 0.022, 0.7), MAT.woodDark, { pos: [0.045, 0.011, 0] }));
  const lensX = X + 0.3755 * K, wallX = 1.33 - 0.014, len = wallX - lensX;
  const beamMat = new T.MeshBasicMaterial({ color: 0xffd54a, transparent: true, opacity: 0, depthWrite: false, side: T.DoubleSide });
  const beam = new T.Mesh(alongX(new T.CylinderGeometry(0.2, 0.078, len, 32, 1, true)), beamMat); beam.position.set(lensX + len / 2, AXY, 0);
  const spotMat = new T.MeshBasicMaterial({ color: 0xffcf3a, transparent: true, opacity: 0, depthWrite: false });
  const spot = new T.Mesh(new T.CircleGeometry(0.2, 40).rotateY(-Math.PI / 2), spotMat); spot.position.set(wallX - 0.001, AXY, 0);
  const lightP = piece(0, Z, beam, spot);
  return diorama('torch', [stand, torchP, wall, lightP], { m, beamMat, spotMat, Z, AXY, lensX, wallX });
})();

// 4. spread thin: the corner of a room, the torch on the floor, a thermometer
const RO = (() => {
  const back = piece(0.72, -0.86, mk(box(1.3, 0.56, 0.024), MAT.wall, { pos: [0, 0.28, 0] }),
    mk(box(1.3, 0.034, 0.012), MAT.woodDark, { pos: [0, 0.017, 0.018] }),
    mk(box(0.3, 0.22, 0.016), MAT.woodDark, { pos: [-0.06, 0.33, 0.02] }), mk(box(0.25, 0.17, 0.004), MAT.paper, { pos: [-0.06, 0.33, 0.029], edges: false }));
  const side = piece(1.376, -0.42, mk(box(0.024, 0.56, 0.9), MAT.wall, { pos: [0, 0.28, 0] }),
    mk(box(0.012, 0.034, 0.9), MAT.woodDark, { pos: [-0.018, 0.017, 0] }));
  const thermo = piece(0.24, -0.84,
    mk(new T.CylinderGeometry(0.016, 0.016, 0.36, 12), MAT.paper, { thr: 40, hull: 0.003, pos: [0, 0.3, 0.02] }),
    mk(new T.SphereGeometry(0.028, 16, 12), MAT.red, { edges: false, hull: 0.003, pos: [0, 0.11, 0.02] }),
    mk(new T.CylinderGeometry(0.007, 0.007, 0.15, 8), MAT.red, { edges: false, pos: [0, 0.19, 0.03] }));
  const K = 1.2, ang = Math.PI / 4;
  const m = torchModel(false); m.t.scale.setScalar(K); m.t.rotation.y = ang; m.t.position.set(0, 0.068 * K, 0);
  const torchP = piece(0.36, -0.06, m.t);
  const hit = new T.Vector3(0.36 + Math.cos(ang) * (0.06 + 0.86), 0, -0.86 + 0.0125);
  const wallGlowMat = new T.MeshBasicMaterial({ color: 0xf08a4a, transparent: true, opacity: 0, depthWrite: false });
  const wallGlow = new T.Mesh(new T.CircleGeometry(0.17, 36), wallGlowMat); wallGlow.position.set(0, 0.1, 0.0125);
  const glowP = piece(hit.x, -0.86, wallGlow);
  const halo = new T.Sprite(new T.SpriteMaterial({ map: fxTexs.glow, transparent: true, opacity: 0, depthWrite: false }));
  halo.scale.set(0.36, 0.36, 1); halo.position.set(0.37, 0, 0); m.t.add(halo);
  return diorama('room', [back, side, thermo, torchP, glowP], { m, wallGlowMat, halo });
})();

// 5. the books balance: a balance scale, before on one pan and after on the other
const BA = (() => {
  const Z = -0.3, X0 = 0.71, PIV = 0.66, ARM = 0.46, DROP = 0.3;
  const base = piece(X0, Z, mk(box(0.56, 0.04, 0.28), MAT.woodDark, { pos: [0, 0.02, 0] }),
    mk(new T.CylinderGeometry(0.022, 0.03, PIV - 0.04, 12), MAT.dark, { thr: 40, hull: 0.003, pos: [0, (PIV - 0.04) / 2 + 0.04, 0] }));
  const beam = new T.Group(); beam.position.set(0, PIV, 0);
  beam.add(mk(box(ARM * 2 + 0.05, 0.022, 0.03), MAT.wood));
  beam.add(mk(new T.CylinderGeometry(0.028, 0.028, 0.045, 18).rotateX(Math.PI / 2), MAT.dark, { thr: 40 }));
  beam.add(mk(box(0.008, 0.11, 0.008), MAT.red, { pos: [0, 0.07, 0.026], edges: false }));
  const beamP = piece(X0, Z, beam);
  const pans = [-1, 1].map(s => {
    const pan = new T.Group();
    pan.add(mk(new T.CylinderGeometry(0.14, 0.12, 0.014, 32), MAT.grey, { thr: 40 }));
    for (let k = 0; k < 3; k++) {
      const a = k / 3 * Math.PI * 2 + 0.5;
      pan.add(cylBetween(new T.Vector3(Math.cos(a) * 0.13, 0.007, Math.sin(a) * 0.13), new T.Vector3(0, DROP, 0), 0.002, MAT.dark));
    }
    const cols = s < 0 ? [ST.G.col, ST.K.col, ST.C.col, ST.L.col, ST.G.col, ST.C.col] : [ST.T.col, ST.T.col, ST.T.col, ST.R.col, ST.T.col, ST.T.col];
    cols.forEach((c, k) => pan.add(mk(new T.CylinderGeometry(0.05, 0.05, 0.0135, 28), inkMat(c, { ink: 0.7 }),
      { thr: 30, pos: [(k % 2 ? 0.004 : -0.003) + (s < 0 ? 0 : 0.002), 0.014 + k * 0.0142, k % 3 ? 0.003 : -0.002] })));
    const p = piece(X0, Z, pan); p.userData.pan = pan; p.userData.s = s; return p;
  });
  return diorama('end', [base, beamP, ...pans], { beam, pans, PIV, ARM, DROP, tilt: 0, tv: 0 });
})();
function placePans() {
  const c = Math.cos(BA.tilt), s = Math.sin(BA.tilt);
  for (const p of BA.pans) {
    const k = p.userData.s;
    p.userData.pan.position.set(k * BA.ARM * c, BA.PIV + k * BA.ARM * s - BA.DROP, 0);
  }
  BA.beam.rotation.z = BA.tilt;
}
placePans();

// ---------- coins and the ledger ----------
const COIN_R = 0.05, COIN_H = 0.0135;
const coinGeo = new T.CylinderGeometry(COIN_R, COIN_R, COIN_H, 30);
const coinLines = edgeLines(coinGeo, 30);
const flakeGeo = new T.BoxGeometry(0.016, 0.004, 0.011);
const coins = [];
for (let i = 0; i < 22; i++) {
  const mat = inkMat(0xffffff, { ink: 0.7 });
  const m = new T.Mesh(coinGeo, mat); m.castShadow = m.receiveShadow = true;
  m.add(new T.LineSegments2(coinLines, LM));
  const fl = new T.Group(), r = rng(900 + i);
  for (let k = 0; k < 7; k++) {
    const f = new T.Mesh(flakeGeo, mat), a = r() * 6.283, rad = 0.09 + r() * 0.1;
    f.position.set(Math.cos(a) * rad, -COIN_H / 2, Math.sin(a) * rad); f.rotation.y = r() * 3; f.receiveShadow = true; fl.add(f);
  }
  fl.visible = false; m.add(fl); m.visible = false; scene.add(m);
  coins.push({ m, mat, fl, store: null, mode: 'off', t: 0, dur: 0.35, delay: 0, idx: 0, spin: 1, from: new T.Vector3(), c0: new T.Color(), c1: new T.Color(),
    jx: (r() - 0.5) * 0.008, jz: (r() - 0.5) * 0.008, ry: r() * 6 });
}
const ROOM_SPOTS = (() => {
  const r = rng(99), out = []; let guard = 0;
  while (out.length < 22 && guard++ < 8000) {
    const x = 0.14 + r() * 1.1, z = -0.74 + r() * 0.92;
    const tx = x - 0.36, tz = z + 0.06, along = (tx - tz) / Math.SQRT2, across = (tx + tz) / Math.SQRT2;
    if (along > -0.08 && along < 0.56 && Math.abs(across) < 0.14) continue;
    if (x < 0.36 && z < -0.6) continue;
    if (out.some(p => Math.hypot(p.x - x, p.z - z) < 0.1)) continue;
    out.push(new T.Vector3(x, PY + COIN_H * 0.3, z));
  }
  return out;
})();
const LG = { stores: [], stacks: {}, prev: null };
function slotOf(store, i, out) {
  if (store === 'R') return out.copy(ROOM_SPOTS[i % ROOM_SPOTS.length]);
  const k = LG.stores.indexOf(store);
  return out.set(COLS[k], PY + COIN_H / 2 + i * (COIN_H + 0.0008), STACK_Z);
}
function alloc(vals, N, prev) {
  const ks = Object.keys(vals), tot = ks.reduce((a, k) => a + Math.max(0, vals[k]), 0) || 1, out = {}, rem = [];
  let sum = 0;
  for (const k of ks) {
    const v = Math.max(0, vals[k]) * N / tot, f = Math.floor(v);
    out[k] = f; sum += f; rem.push([k, v - f + (prev && prev[k] > f ? 0.25 : 0)]);
  }
  rem.sort((a, b) => b[1] - a[1]);
  for (let i = 0; sum < N; i = (i + 1) % rem.length, sum++) out[rem[i][0]]++;
  return out;
}
let clinkGate = 0;
function coinIn(c, s, delay = 0) {
  c.store = s; c.mode = 'drop'; c.t = 0; c.delay = delay; c.mat.color.set(ST[s].col); c.m.scale.setScalar(1); c.m.visible = false;
  LG.stacks[s].push(c);
}
function coinOut(c) { c.mode = 'out'; c.t = 0; }
function coinFly(c, to) {
  c.from.copy(c.m.position); c.t = 0; c.mode = 'fly'; c.dur = 0.32 + Math.random() * 0.12;
  c.c0.copy(c.mat.color); c.c1.set(ST[to].col); c.store = to; c.spin = Math.random() < 0.5 ? 1 : -1;
}
function ledgerUpdate(vals, N, instant = false) {
  const tgt = alloc(vals, N, LG.prev), st = LG.stacks; LG.prev = tgt;
  let have = LG.stores.reduce((a, s) => a + st[s].length, 0);
  while (have > N) {
    const s = LG.stores.slice().sort((a, b) => (st[b].length - tgt[b]) - (st[a].length - tgt[a]))[0];
    coinOut(st[s].pop()); have--;
  }
  while (have < N) {
    const s = LG.stores.slice().sort((a, b) => (tgt[b] - st[b].length) - (tgt[a] - st[a].length))[0];
    const c = coins.find(c => c.mode === 'off'); if (!c) break;
    coinIn(c, s, instant ? 0 : st[s].length * 0.035); have++;
    if (instant) { c.mode = 'rest'; c.m.visible = true; slotOf(s, st[s].length - 1, c.m.position); }
  }
  for (let guard = 0; guard < 60; guard++) {
    const src = LG.stores.find(s => st[s].length > tgt[s]), dst = LG.stores.find(s => st[s].length < tgt[s]);
    if (!src || !dst) break;
    const c = st[src].pop(); st[dst].push(c);
    if (instant) { c.store = dst; c.mat.color.set(ST[dst].col); c.mode = 'rest'; } else coinFly(c, dst);
  }
}
function ledgerStart(stores) {
  LG.stores = stores; LG.stacks = {}; LG.prev = null; stores.forEach(s => (LG.stacks[s] = []));
}
function ledgerClear() {
  for (const c of coins) if (c.mode !== 'off') coinOut(c);
  LG.stores = []; LG.stacks = {};
}
const _v = new T.Vector3();
function updateCoins(dt) {
  for (const s of LG.stores) LG.stacks[s].forEach((c, i) => (c.idx = i));
  for (const c of coins) {
    if (c.mode === 'off') continue;
    if (c.mode === 'out') {
      c.t += dt / 0.25; c.m.scale.setScalar(Math.max(0.001, 1 - c.t));
      if (c.t >= 1) { c.mode = 'off'; c.m.visible = false; c.store = null; c.fl.visible = false; }
      continue;
    }
    slotOf(c.store, c.idx, _v);
    const inR = c.store === 'R';
    if (!inR) { _v.x += c.jx; _v.z += c.jz; }
    if (c.mode === 'drop') {
      if (c.delay > 0) { c.delay -= dt; continue; }
      c.m.visible = true; c.t = Math.min(1, c.t + dt / 0.55);
      c.m.position.set(_v.x, _v.y + 0.45 * (1 - bounceOut(c.t)), _v.z); c.m.rotation.set(0, c.ry, 0);
      if (c.t >= 1) { c.mode = 'rest'; clink(); }
    } else if (c.mode === 'fly') {
      c.t = Math.min(1, c.t + dt / c.dur); const e = easeIO(c.t);
      const lift = 0.1 + 0.12 * c.from.distanceTo(_v);
      c.m.position.lerpVectors(c.from, _v, e); c.m.position.y += lift * 4 * e * (1 - e);
      c.m.rotation.set(c.spin * e * Math.PI * 2, c.ry, 0);
      c.mat.color.lerpColors(c.c0, c.c1, e);
      if (c.t >= 1) { c.mode = 'rest'; clink(); }
    } else {
      c.m.position.lerp(_v, 1 - Math.exp(-dt * 16)); c.m.rotation.set(0, c.ry, 0);
    }
    const sc = inR && c.mode === 'rest' ? 0.55 : 1;
    c.m.scale.setScalar(c.m.scale.x + (sc - c.m.scale.x) * (1 - Math.exp(-dt * 8)));
    c.fl.visible = inR && c.mode === 'rest';
  }
}

// ---------- little effects: heat squiggles, light, air curls ----------
const fxPool = [];
for (let i = 0; i < 48; i++) {
  const s = new T.Sprite(new T.SpriteMaterial({ transparent: true, depthWrite: false, opacity: 0 }));
  s.visible = false; scene.add(s); fxPool.push({ s, age: 0, life: 1, vel: new T.Vector3(), op: 1, on: false, sway: 0 });
}
function spawn(tex, pos, o = {}) {
  const f = fxPool.find(f => !f.on); if (!f) return;
  f.on = true; f.age = 0; f.life = o.life ?? 1.1; f.op = o.op ?? 0.9; f.sway = o.sway ?? 0;
  f.vel.copy(o.vel ?? new T.Vector3(0, 0.16, 0));
  f.s.material.map = tex; f.s.material.blending = o.add ? T.AdditiveBlending : T.NormalBlending; f.s.material.needsUpdate = true;
  f.s.position.copy(pos); f.s.scale.set(...(o.scale ?? [0.035, 0.07]), 1); f.s.visible = true;
}
function updateFx(dt) {
  for (const f of fxPool) {
    if (!f.on) continue;
    f.age += dt; if (f.age >= f.life) { f.on = false; f.s.visible = false; continue; }
    f.s.position.addScaledVector(f.vel, dt);
    if (f.sway) f.s.position.x += Math.sin(f.age * 9) * f.sway * dt;
    f.s.material.opacity = f.op * Math.sin(Math.PI * f.age / f.life);
  }
}
function clearFx() { for (const f of fxPool) { f.on = false; f.s.visible = false; } }

// ---------- the models' physics ----------
const SIM = {};
SIM.swing = {
  m: 2, Lm: 2, b: 0.12, th0: 1.0472, th: 1.0472, w: 0, run: false, air: false, heat: 0, hint: true,
  E0() { return this.m * GRAV * this.Lm * (1 - Math.cos(this.th0)); },
  N() { return Math.round(this.E0()); },
  setPull(a) { this.th0 = clamp(a, -1.0472, 1.0472); if (Math.abs(this.th0) < 0.17) this.th0 = 0.17 * Math.sign(this.th0 || 1); this.catchIt(); setGuide(this.th0); },
  catchIt() { this.run = false; this.th = this.th0; this.w = 0; this.heat = 0; },
  release() { if (!this.run) { this.catchIt(); this.run = true; this.hint = false; } },
  step(dt) {
    if (!this.run) return;
    const n = 8, h = dt / n, k = GRAV / this.Lm;
    for (let i = 0; i < n; i++) {
      const a = -k * Math.sin(this.th) - (this.air ? this.b * this.w : 0);
      this.w += a * h; this.th += this.w * h;
      if (this.air) this.heat += this.b * this.m * this.Lm * this.Lm * this.w * this.w * h;
    }
    this.heat = Math.min(this.heat, this.E0());
    const v = Math.abs(this.w) * this.Lm;
    if (this.air && v > 2 && Math.random() < dt * 9) {
      SW.bob.getWorldPosition(_v); _v.x -= Math.sign(this.w) * 0.07;
      spawn(fxTexs.curl, _v, { vel: new T.Vector3(-Math.sign(this.w) * 0.12, 0.02, 0), life: 0.6, op: 0.7, scale: [0.05, 0.05] });
    }
  },
  values() {
    const E = this.E0(), g = Math.min(this.m * GRAV * this.Lm * (1 - Math.cos(this.th)), E - this.heat);
    return { G: Math.max(0, g), K: Math.max(0, E - this.heat - g), T: this.heat };
  },
};
SIM.slide = {
  m: 2, H: 1, alpha: 40 * Math.PI / 180, mu: 0.25, mats: [0.25, 0.5, 1], mat: 0, phase: 'top', s: 0, v: 0, x: 0, hint: true,
  N() { return 20; },
  setMat(i) { this.mat = i; this.mu = this.mats[i]; SL.matMesh.material.map = matTex[i]; SL.matMesh.material.needsUpdate = true; this.reset(); },
  reset() { this.phase = 'top'; this.s = 0; this.v = 0; this.x = 0; SL.warm.material.opacity = 0; SL.warm.scale.x = 0.001; },
  go() { if (this.phase === 'stopped') this.reset(); if (this.phase === 'top') { this.phase = 'ramp'; this.hint = false; } },
  step(dt) {
    dt *= 0.8;
    const Lr = this.H / Math.sin(this.alpha);
    if (this.phase === 'ramp') {
      this.v += GRAV * Math.sin(this.alpha) * dt; this.s += this.v * dt;
      if (this.s >= Lr) { this.s = Lr; this.v = Math.sqrt(2 * GRAV * this.H); this.phase = 'flat'; }
    } else if (this.phase === 'flat') {
      const d = this.mu * GRAV, v0 = Math.sqrt(2 * GRAV * this.H);
      this.v = Math.max(0, this.v - d * dt);
      this.x = (v0 * v0 - this.v * this.v) / (2 * d);
      if (Math.random() < dt * (6 + 30 * this.mu * this.v / v0)) {
        SL.sled.getWorldPosition(_v); _v.x += (Math.random() - 0.6) * 0.1; _v.y += 0.03; _v.z += (Math.random() - 0.5) * 0.08;
        spawn(fxTexs.squig, _v, { life: 1.2, op: 0.85 });
      }
      if (this.v <= 0) this.phase = 'stopped';
    }
    if (this.phase === 'flat' || this.phase === 'stopped') {
      SL.warm.scale.x = Math.max(0.001, this.x * SL.SC + 0.04);
      SL.warm.material.opacity = 0.32 * (1 - this.v * this.v / (2 * GRAV * this.H));
    }
  },
  values() {
    const g = this.phase === 'top' || this.phase === 'ramp' ? this.m * GRAV * (this.H - this.s * Math.sin(this.alpha)) : 0;
    const k = 0.5 * this.m * this.v * this.v;
    return { G: Math.max(0, g), K: Math.max(0, Math.min(k, 20 - g)), T: Math.max(0, 20 - g - k) };
  },
  place() {
    const al = this.alpha;
    if (this.phase === 'top' || this.phase === 'ramp') {
      SL.sled.position.set(SL.xa + this.s * Math.cos(al) * SL.SC, (this.H - this.s * Math.sin(al)) * SL.SC, 0);
      SL.sled.rotation.z = -al;
    } else {
      SL.sled.position.set(SL.xb + this.x * SL.SC, 0.006, 0);
      SL.sled.rotation.z = -al * Math.max(0, 1 - this.x * 4);
    }
  },
};
SIM.torch = {
  bulb: 0, on: false, chem: 20, light: 0, heat: 0, P: 2.6, hint: true,
  N() { return 20; },
  eff() { return this.bulb ? 0.3 : 0.1; },
  setBulb(i) { this.bulb = i; this.reset(); },
  reset() { this.on = false; this.chem = 20; this.light = 0; this.heat = 0; },
  toggle() { if (this.chem <= 0) this.reset(); this.on = !this.on; this.hint = false; click(); },
  step(dt) {
    if (this.on) {
      const dE = Math.min(this.chem, this.P * dt);
      this.chem -= dE; this.light += dE * this.eff(); this.heat += dE * (1 - this.eff());
      if (this.chem <= 1e-6) { this.chem = 0; this.on = false; }
      const b = this.eff() / 0.3;
      if (Math.random() < dt * 22 * b) {
        _v.set(TO.lensX + 0.02, PY + TO.AXY + (Math.random() - 0.5) * 0.08, TO.Z + (Math.random() - 0.5) * 0.08);
        spawn(fxTexs.dot, _v, { vel: new T.Vector3(1.0, 0, 0), life: (TO.wallX - TO.lensX - 0.03) / 1.0, op: 1, scale: [0.036, 0.036] });
      }
      if (Math.random() < dt * 10 * (1 - this.eff())) {
        _v.set(TO.lensX - 0.09 + Math.random() * 0.06, PY + TO.AXY + 0.1, TO.Z + (Math.random() - 0.5) * 0.06);
        spawn(fxTexs.squig, _v, { life: 1.1 });
      }
    }
    const b = this.on ? this.eff() / 0.3 : 0, m = TO.m;
    m.lensMat.color.setRGB(0.86 + 0.14 * b, 0.84 + 0.16 * b, 0.78 + 0.0 * b);
    if (b > 0) m.lensMat.color.setRGB(1, 0.93 + 0.07 * b, 0.6 + 0.3 * b);
    TO.beamMat.opacity += ((this.on ? 0.07 + 0.16 * b : 0) - TO.beamMat.opacity) * Math.min(1, dt * 10);
    TO.spotMat.opacity += ((this.on ? 0.22 + 0.55 * b : 0) - TO.spotMat.opacity) * Math.min(1, dt * 10);
    m.band.scale.x = Math.max(0.001, this.chem / 20);
    m.sw.position.x = this.on ? 0.312 : 0.3;
  },
  values() { return { C: this.chem, L: this.light, T: this.heat }; },
};
SIM.room = {
  t: 0, play: false,
  N() { return 20; },
  f() { return 1 - Math.exp(-this.t / 140); },
  reset() { this.t = 0; this.play = false; },
  step(dt) {
    if (this.play) { this.t = Math.min(600, this.t + dt * 60); if (this.t >= 600) this.play = false; }
    const f = this.f();
    RO.wallGlowMat.opacity = 0.45 * (1 - f); RO.halo.material.opacity = 0.85 * (1 - f);
    if (Math.random() < dt * 5 * (1 - f)) {
      RO.halo.getWorldPosition(_v); _v.x += (Math.random() - 0.5) * 0.06; _v.y += 0.06;
      spawn(fxTexs.squig, _v, { life: 1.2, op: 0.8 * (1 - f) });
    }
  },
  values() { const f = this.f(); return { B: 18 * (1 - f), W: 2 * (1 - f), R: 20 * f }; },
};
SIM.end = {
  N() { return 0; },
  step(dt) {
    const a = -42 * BA.tilt - 3.2 * BA.tv; BA.tv += a * dt; BA.tilt += BA.tv * dt;
    if (Math.abs(BA.tilt) < 1e-4 && Math.abs(BA.tv) < 1e-4) BA.tilt = BA.tv = 0;
    placePans();
  },
  values() { return {}; },
};
SIM.cover = { N() { return 0; }, step() {}, values() { return {}; } };

// ---------- labels that follow the models ----------
const labelsEl = $('#labels'), labels = [];
let labelsHidden = true;
function addLabel(html, posFn, cls = '', showFn = null) {
  const el = document.createElement('div'); el.className = 'lbl ' + cls; el.innerHTML = html; labelsEl.appendChild(el);
  const L = { el, posFn, showFn, op: -1, dy: cls.includes('up') ? '-100%' : '0' }; labels.push(L); return L;
}
function clearLabels() { labels.forEach(l => l.el.remove()); labels.length = 0; }
function updateLabels() {
  const W = innerWidth, H = innerHeight;
  for (const L of labels) {
    const show = !labelsHidden && (!L.showFn || L.showFn());
    if (show) {
      _v.copy(L.posFn()).project(camera);
      L.el.style.transform = `translate(${((_v.x * 0.5 + 0.5) * W).toFixed(1)}px,${((-_v.y * 0.5 + 0.5) * H).toFixed(1)}px) translate(-50%,${L.dy})`;
    }
    const op = show ? 1 : 0;
    if (op !== L.op) { L.el.style.opacity = op; L.op = op; }
  }
}
function stackLabels(stores) {
  stores.forEach((s, k) => {
    const L = addLabel(`${ST[s].name}<b>0 J</b>`, () => _w.set(COLS[k], PY, STACK_Z + 0.105));
    L.store = s; L.b = L.el.querySelector('b'); L.last = -1;
  });
}
const _w = new T.Vector3();
function updateStackLabels() {
  for (const L of labels) {
    if (!L.store) continue;
    const n = LG.stacks[L.store] ? LG.stacks[L.store].length : 0;
    if (n !== L.last) { L.b.textContent = n + ' J'; L.last = n; }
  }
}
function spreadLabels(id) {
  const S = SIM[id];
  if (id === 'swing') {
    addLabel('', () => SW.bob.getWorldPosition(_w).add(_v.set(0, 0.07, 0)), 'lnote up', () => !S.run).update = el => {
      const h = S.Lm * (1 - Math.cos(S.th0)); el.textContent = `h = ${h.toFixed(2)} m`;
    };
    addLabel('drag the ball', () => SW.bob.getWorldPosition(_w).add(_v.set(-Math.sign(S.th0 || 1) * 0.15, -0.08, 0)), 'lnote', () => S.hint && !S.run);
  }
  if (id === 'slide') {
    addLabel('tap the sledge ↓', () => SL.sled.getWorldPosition(_w).add(_v.set(0.03, 0.08, 0)), 'lnote up', () => S.hint && S.phase === 'top');
    addLabel('', () => SL.sled.getWorldPosition(_w).add(_v.set(0, 0.07, 0)), 'lnote up', () => S.phase === 'stopped').update = el => {
      el.textContent = `stopped after ${S.x.toFixed(1)} m`;
    };
  }
  if (id === 'torch') addLabel('tap the torch ↓', () => _w.set(TO.lensX - 0.2, PY + TO.AXY + 0.11, TO.Z), 'lnote up', () => S.hint && !S.on);
  if (id === 'room') {
    addLabel('', () => _w.set(0.24, PY + 0.5, -0.82), 'lnote up').update = el => {
      el.textContent = `+${(0.0004 * S.f()).toFixed(4)} °C`;
    };
  }
  if (id === 'end') {
    addLabel('before<b>20 J</b>', () => _w.set(BA.pans[0].userData.pan.position.x + 0.71, PY + BA.pans[0].userData.pan.position.y - 0.03, -0.3 + 0.16));
    addLabel('after<b>20 J</b>', () => _w.set(BA.pans[1].userData.pan.position.x + 0.71, PY + BA.pans[1].userData.pan.position.y - 0.03, -0.3 + 0.16));
  }
}

// ---------- the card: text, controls, guesses ----------
const card = $('#card'), cbody = $('#cbody');
const GUESS = {}, visited = new Set();
const chipsHTML = stores => `<div class="ledger">${stores.map(s =>
  `<span class="chip" data-s="${s}"><i style="--c:${hex(ST[s].col)}"></i>${ST[s].name}<b>0 J</b></span>`).join('')}<span class="tot">Total<b>0 J</b></span></div>`;
const guessHTML = (id, q, opts, right, why) => `<div class="guess" data-guess="${id}" data-right="${right}">
  <p class="q"><span>Your guess</span>${q}</p><div class="opts">${opts.map((o, i) => `<button data-a="${i}">${o}</button>`).join('')}</div>
  <p class="ans" hidden>${why}</p></div>`;
const CARD = {
  cover: () => `<h1>The Energy Ledger</h1>
<p class="lead">Energy never gets used up. It moves from one store to another, and the total stays the same.</p>
<p>This notebook keeps the accounts: <b>1 coin = 1 joule</b>. Play with each model, then count the coins.</p>
<div class="row"><button class="primary" data-act="open">Open the notebook</button></div>
<p class="small">Drag the desk to look around. Turn the pages with ‹ Back and Next ›.</p>
<div class="links"><button data-sheet="0">For teachers</button><button data-sheet="1">Sources &amp; credits</button></div>`,
  swing: () => `<h2>The swing</h2>
<p>A 2 kg ball hangs on a 2 m string. Pulling it back lifts it, and that fills its <b class="g">gravitational</b> store: <i>E&nbsp;=&nbsp;m&nbsp;×&nbsp;g&nbsp;×&nbsp;h</i>. We use g = 10&nbsp;N/kg.</p>
<label class="range">Pull back<input type="range" min="10" max="60" step="1" data-ctl="pull"><output data-out="pull"></output></label>
<p class="readout" data-out="lift"></p>
<div class="row"><button class="primary" data-act="go">Let go</button><button data-act="reset">Catch it</button></div>
<div class="seg" data-seg="air"><span>Air</span><button data-v="0">Off</button><button data-v="1">On</button></div>
<p class="note" data-note="air" hidden>The air pushes back on the ball. Each swing, a little energy goes to the <b class="t">thermal</b> store of the air. The swing dies down, but the total stays the same.</p>
${guessHTML('swing', 'When the ball passes the lowest point, where are the coins?', ['Still in gravitational', 'Half in each store', 'All in kinetic'], 2,
  'At the lowest point the ball has no height left to lose, so all the coins are kinetic. As it climbs again, they move back to gravitational. Let go and watch the stacks.')}
${chipsHTML(SP[1].stores)}`,
  slide: () => `<h2>Slide and stop</h2>
<p>A 2 kg sledge waits at the top of a 1 m slope, with 20 J in its <b class="g">gravitational</b> store. The slope is polished, so we ignore its friction. At the bottom, the sledge slides onto a mat.</p>
<div class="seg" data-seg="mat"><span>Mat</span><button data-v="0">Plastic</button><button data-v="1">Carpet</button><button data-v="2">Sandpaper</button></div>
<div class="row"><button class="primary" data-act="go">Let it slide</button><button data-act="reset">Back to the top</button></div>
<p class="readout" data-out="stop"></p>
${guessHTML('slide', 'Which mat ends up with the most energy in the thermal store?', ['Plastic', 'Sandpaper', 'All the same'], 2,
  'The sledge always stops, so its kinetic store always ends up empty: all 20 J go to the thermal store of the mat and the runners. A rougher mat does it over a shorter distance, because friction force × distance = 20 J. Try all three.')}
${chipsHTML(SP[2].stores)}`,
  torch: () => `<h2>Light and heat</h2>
<p>This torch battery is nearly flat. It has just 20 J left in its <b class="c">chemical</b> store. Switch on, and an electric current carries that energy to the bulb.</p>
<div class="seg" data-seg="bulb"><span>Bulb</span><button data-v="0">Filament</button><button data-v="1">LED</button></div>
<div class="row"><button class="primary" data-act="go">Switch on</button><button data-act="reset">New battery</button></div>
<p class="readout" data-out="eff"></p>
${guessHTML('torch', 'With the filament bulb, how many of the 20 J come out as light?', ['About 18 J', 'About 10 J', 'About 2 J'], 2,
  'Only about 2 J. The other 18 J heat the bulb and the air around it. Efficiency = useful energy out ÷ total energy in = 2 ÷ 20 = 10%. An LED turns about 6 J into light: 30%. Try both.')}
${chipsHTML(SP[3].stores)}
<p class="small">Light is not a store. It is a way energy travels, and it warms whatever it lands on: here, the wall.</p>`,
  room: () => `<h2>Spread thin</h2>
<p>The filament torch has just been switched off. Its light warmed the wall (2 J), and the hot bulb holds the other 18 J. Move the clock and watch where the energy goes.</p>
<label class="range">Time<input type="range" min="0" max="600" step="5" data-ctl="time"><output data-out="time"></output></label>
<div class="row"><button class="primary" data-act="go">▶ Run 10 minutes</button><button data-act="reset">Back to 0</button></div>
<p class="readout" data-out="temp"></p>
${guessHTML('room', 'After 10 minutes, could the room’s energy light the bulb again?', ['Yes, all 20 J are still there', 'No, it is too spread out'], 1,
  'No. All 20 J are still there, in the thermal store of the room. But they are shared out over the air, the walls and the floor, and the room is less than 0.001 °C warmer. Energy this spread out is very hard to use again. We say it has been dissipated.')}
${chipsHTML(SP[4].stores)}`,
  end: () => `<h2>The books balance</h2>
<p>Four models, one rule: energy is never made or destroyed. It only moves from store to store, and some of it always spreads out as heat.</p>
<ul class="sum">
<li data-p="swing"><b>The swing:</b> gravitational ⇄ kinetic, 20 J all the way.</li>
<li data-p="slide"><b>Slide and stop:</b> friction moved all 20 J to the thermal store.</li>
<li data-p="torch"><b>Light and heat:</b> 2 J of light from a filament bulb, 6 J from an LED.</li>
<li data-p="room"><b>Spread thin:</b> still 20 J, but too spread out to use.</li></ul>
<p class="readout" data-out="score"></p>
<p class="small">Tap a pan to test the scale.</p>
<div class="row"><button data-act="restart">Close the notebook</button></div>
<div class="links"><button data-sheet="0">For teachers</button><button data-sheet="1">Sources &amp; credits</button></div>`,
};
const outs = {};
function renderCard(i) {
  const sp = SP[i];
  $('#kick').textContent = i === 0 ? 'Cover' : i === SP.length - 1 ? 'Last page' : `Page ${i} of 4`;
  $('#dots').innerHTML = SP.map((_, k) => `<i class="${k === i ? 'on' : ''}"></i>`).join('');
  cbody.innerHTML = CARD[sp.id](); cbody.scrollTop = 0;
  $('#prev').disabled = i === 0; $('#next').disabled = i === SP.length - 1;
  $('#next').textContent = i === 0 ? 'Open ›' : 'Next ›';
  for (const k in outs) delete outs[k];
  cbody.querySelectorAll('[data-out]').forEach(el => (outs[el.dataset.out] = el));
  lastChips = '';
  wire(sp.id);
}
function wire(id) {
  const on = (sel, fn) => cbody.querySelectorAll(sel).forEach(el => el.addEventListener('click', fn));
  const seg = (name, cur, fn) => {
    const s = cbody.querySelector(`[data-seg="${name}"]`); if (!s) return;
    const bs = s.querySelectorAll('button');
    bs.forEach(b => { b.classList.toggle('on', +b.dataset.v === cur); b.setAttribute('aria-pressed', +b.dataset.v === cur); });
    bs.forEach(b => b.addEventListener('click', () => {
      bs.forEach(x => { x.classList.toggle('on', x === b); x.setAttribute('aria-pressed', x === b); }); fn(+b.dataset.v);
    }));
  };
  cbody.querySelectorAll('[data-guess]').forEach(g => {
    const gid = g.dataset.guess, right = +g.dataset.right, bs = g.querySelectorAll('.opts button'), ans = g.querySelector('.ans');
    const show = pick => {
      bs.forEach((b, i) => { b.classList.toggle('right', i === right); b.classList.toggle('wrong', i === pick && pick !== right); b.disabled = true; });
      ans.hidden = false;
      ans.insertAdjacentHTML('afterbegin', pick === right ? '<b>Right.</b> ' : '<b class="no">Not quite.</b> ');
    };
    bs.forEach(b => b.addEventListener('click', () => { GUESS[gid] = +b.dataset.a; show(GUESS[gid]); }));
    if (gid in GUESS) show(GUESS[gid]);
  });
  cbody.querySelectorAll('[data-sheet]').forEach(b => b.addEventListener('click', () => openSheet(+b.dataset.sheet)));
  if (id === 'cover') on('[data-act=open]', () => go(1));
  if (id === 'swing') {
    const S = SIM.swing, pull = cbody.querySelector('[data-ctl=pull]');
    pull.value = Math.round(Math.abs(S.th0) * 180 / Math.PI);
    pull.addEventListener('input', () => S.setPull(+pull.value * Math.PI / 180 * Math.sign(S.th0 || 1)));
    on('[data-act=go]', () => S.release()); on('[data-act=reset]', () => S.catchIt());
    seg('air', +S.air, v => { S.air = !!v; cbody.querySelector('[data-note=air]').hidden = !v; S.catchIt(); });
    cbody.querySelector('[data-note=air]').hidden = !S.air;
  }
  if (id === 'slide') {
    const S = SIM.slide;
    seg('mat', S.mat, v => S.setMat(v));
    on('[data-act=go]', () => S.go()); on('[data-act=reset]', () => S.reset());
  }
  if (id === 'torch') {
    const S = SIM.torch;
    seg('bulb', S.bulb, v => S.setBulb(v));
    on('[data-act=go]', () => S.toggle()); on('[data-act=reset]', () => S.reset());
  }
  if (id === 'room') {
    const S = SIM.room, tm = cbody.querySelector('[data-ctl=time]');
    tm.value = S.t;
    tm.addEventListener('input', () => { S.t = +tm.value; S.play = false; });
    on('[data-act=go]', () => { if (S.t >= 600) S.t = 0; S.play = true; });
    on('[data-act=reset]', () => S.reset());
  }
  if (id === 'end') {
    cbody.querySelectorAll('[data-p]').forEach(li => li.classList.toggle('done', visited.has(li.dataset.p)));
    const ids = ['swing', 'slide', 'torch', 'room'], tried = ids.filter(k => k in GUESS), right = { swing: 2, slide: 2, torch: 2, room: 1 };
    if (outs.score) outs.score.textContent = tried.length ? `Your guesses: ${tried.filter(k => GUESS[k] === right[k]).length} of ${tried.length} right` : '';
    on('[data-act=restart]', () => go(0));
  }
}
let lastChips = '';
function cardTick() {
  const id = SP[cur]?.id; if (!id) return;
  if (LG.stores.length) {
    const counts = LG.stores.map(s => LG.stacks[s].length), key = counts.join(',');
    if (key !== lastChips) {
      lastChips = key;
      cbody.querySelectorAll('.chip').forEach((c, k) => (c.querySelector('b').textContent = counts[k] + ' J'));
      const tot = cbody.querySelector('.tot b'); if (tot) tot.textContent = counts.reduce((a, b) => a + b, 0) + ' J';
    }
  }
  const set = (k, t) => { if (outs[k] && outs[k].textContent !== t) outs[k].textContent = t; };
  if (id === 'swing') {
    const S = SIM.swing, deg = Math.round(Math.abs(S.th0) * 180 / Math.PI), h = S.Lm * (1 - Math.cos(S.th0));
    set('pull', deg + '°');
    set('lift', `Lifted ${h.toFixed(2)} m, so the store holds ${(2 * GRAV * h).toFixed(1)} J`);
  }
  if (id === 'slide') {
    const S = SIM.slide, F = S.mu * S.m * GRAV, names = ['plastic', 'carpet', 'sandpaper'];
    set('stop', S.phase === 'stopped' ? `Stopped after ${S.x.toFixed(1)} m: ${F} N × ${S.x.toFixed(1)} m = 20 J` : `Friction from the ${names[S.mat]}: ${F} N`);
  }
  if (id === 'torch') {
    const S = SIM.torch, used = 20 - S.chem;
    set('eff', used > 0.5 ? `Light ${S.light.toFixed(1)} J out of ${used.toFixed(1)} J used: efficiency ${Math.round(100 * S.light / used)}%` : 'Efficiency: switch on to find out');
    const b = cbody.querySelector('[data-act=go]'); const t = S.on ? 'Switch off' : S.chem <= 0 ? 'Battery flat: tap New battery' : 'Switch on';
    if (b && b.textContent !== t) { b.textContent = t; b.disabled = S.chem <= 0; }
  }
  if (id === 'room') {
    const S = SIM.room, tm = cbody.querySelector('[data-ctl=time]');
    if (S.play && tm) tm.value = S.t;
    set('time', S.t < 60 ? Math.round(S.t) + ' s' : (S.t / 60).toFixed(S.t < 600 ? 1 : 0) + ' min');
    set('temp', `Room ${(0.0004 * S.f()).toFixed(4)} °C warmer, ${Math.round(20 * S.f())} J spread out`);
  }
}

// ---------- camera: fit the important part of the book into the space the card leaves ----------
const cam = { az: 0, el: 1.0, dist: 6, tx: 0.7, ty: 0.1, tz: 0, ox: 0, oy: 0 };
const user = { az: 0, el: 0, zoom: 1 };
function freeRect() {
  const W = innerWidth, H = innerHeight, r = card.getBoundingClientRect();
  if (W >= H && card.classList.contains('folded')) return { x: 0, y: r.bottom + 4, w: W, h: Math.max(80, H - r.bottom - 4) };
  if (W >= H) return { x: r.right + 4, y: 0, w: Math.max(80, W - r.right - 4), h: H };
  return { x: 0, y: 0, w: W, h: Math.max(80, r.top - 4) };
}
function roiFor(i) {
  if (i === 0) return new T.Box3(new T.Vector3(-0.04, 0, -1.02), new T.Vector3(1.52, 0.2, 1.02));
  const wide = innerWidth >= innerHeight;
  return new T.Box3(new T.Vector3(wide ? -1.44 : -0.06, 0, -1.0), new T.Vector3(1.46, 0.82, 0.98));
}
function placeCam(st) {
  const az = st.az, el = st.el, d = st.dist;
  camera.position.set(st.tx + d * Math.sin(az) * Math.cos(el), st.ty + d * Math.sin(el), st.tz + d * Math.cos(az) * Math.cos(el));
  camera.lookAt(st.tx, st.ty, st.tz);
}
function fitFor(i) {
  const W = innerWidth, H = innerHeight, rect = freeRect(), roi = roiFor(i), c = roi.getCenter(new T.Vector3());
  const st = { az: 0, el: i === 0 ? 1.08 : 0.66, tx: c.x, ty: c.y, tz: c.z, dist: 4, ox: W / 2 - (rect.x + rect.w / 2), oy: H / 2 - (rect.y + rect.h / 2) };
  camera.aspect = W / H; camera.setViewOffset(W, H, st.ox, st.oy, W, H); camera.updateProjectionMatrix();
  const pts = []; for (let k = 0; k < 8; k++) pts.push(new T.Vector3(k & 1 ? roi.max.x : roi.min.x, k & 2 ? roi.max.y : roi.min.y, k & 4 ? roi.max.z : roi.min.z));
  const m = Math.min(18, rect.w * 0.04);
  let lo = 0.5, hi = 40;
  for (let it = 0; it < 26; it++) {
    st.dist = (lo + hi) / 2; placeCam(st); camera.updateMatrixWorld();
    let ok = true;
    for (const p of pts) {
      _v.copy(p).project(camera);
      const sx = (_v.x * 0.5 + 0.5) * W, sy = (-_v.y * 0.5 + 0.5) * H;
      if (_v.z > 1 || sx < rect.x + m || sx > rect.x + rect.w - m || sy < rect.y + m || sy > rect.y + rect.h - m) { ok = false; break; }
    }
    ok ? (hi = st.dist) : (lo = st.dist);
  }
  st.dist = hi;
  return st;
}
let camTween = null;
function fitCamera(i, dur = 1.1) {
  const st = fitFor(i);
  if (camTween) camTween.kill();
  if (dur <= 0 || REDUCED) { Object.assign(cam, st); return; }
  camTween = gsap.to(cam, { ...st, duration: dur, ease: 'power2.inOut' });
  gsap.to(user, { az: 0, el: 0, zoom: 1, duration: dur, ease: 'power2.inOut' });
}
function applyCam(time) {
  const W = innerWidth, H = innerHeight;
  camera.aspect = W / H; camera.setViewOffset(W, H, cam.ox, cam.oy, W, H); camera.updateProjectionMatrix();
  const sway = SHOT || REDUCED ? 0 : 0.02 * Math.sin(time * 0.23);
  placeCam({ ...cam, az: cam.az + user.az + sway, el: clamp(cam.el + user.el, 0.32, 1.38), dist: cam.dist * user.zoom });
}
function resize() {
  const W = innerWidth, H = innerHeight;
  renderer.setSize(W, H, false);
  for (const m of LMs) { m.resolution.set(W * DPR, H * DPR); m.linewidth = m.userData.px * DPR; }
  if (cur >= 0) fitCamera(cur, 0.35);
}
let resizeT = 0;
addEventListener('resize', () => { clearTimeout(resizeT); resizeT = setTimeout(resize, 60); });
new ResizeObserver(() => { clearTimeout(resizeT); resizeT = setTimeout(resize, 60); }).observe(card);

// ---------- sound (synthesised, no files) ----------
let AC = null, soundOn = true, sfxLoading = null;
const SFX_SRC = /*SFX*/{}, SFX = {};
function audio() {
  if (!AC) { try { AC = new (window.AudioContext || window.webkitAudioContext)(); } catch (e) { AC = null; } }
  if (AC && AC.state === 'suspended') AC.resume();
  if (AC && !sfxLoading) sfxLoading = Promise.all(Object.entries(SFX_SRC).map(async ([k, uri]) => {
    try {
      const bin = Uint8Array.from(atob(uri.slice(uri.indexOf(',') + 1)), c => c.charCodeAt(0));
      SFX[k] = await AC.decodeAudioData(bin.buffer);
    } catch (e) { /* that sound falls back to the synthesised one */ }
  }));
  return AC;
}
function sample(names, gain, rate = 1) {
  if (!soundOn || !AC || AC.state !== 'running') return true;
  const have = names.filter(n => SFX[n]); if (!have.length) return false;
  const s = AC.createBufferSource(), g = AC.createGain();
  s.buffer = SFX[have[(Math.random() * have.length) | 0]]; s.playbackRate.value = rate; g.gain.value = gain;
  s.connect(g).connect(AC.destination); s.start();
  return true;
}
function tone(f0, f1, dur, gain, type = 'sine') {
  if (!soundOn || !AC || AC.state !== 'running') return;
  const t = AC.currentTime, o = AC.createOscillator(), g = AC.createGain();
  o.type = type; o.frequency.setValueAtTime(f0, t); o.frequency.exponentialRampToValueAtTime(f1, t + dur);
  g.gain.setValueAtTime(gain, t); g.gain.exponentialRampToValueAtTime(0.0001, t + dur);
  o.connect(g).connect(AC.destination); o.start(t); o.stop(t + dur + 0.02);
}
function clink() {
  const now = performance.now(); if (now - clinkGate < 80) return; clinkGate = now;
  if (sample(['chip1', 'chip2', 'chip3'], 0.32, 0.94 + Math.random() * 0.14)) return;
  const f = 2500 * (0.9 + Math.random() * 0.2); tone(f, f * 0.98, 0.12, 0.03); tone(f * 2.7, f * 2.6, 0.08, 0.012);
}
function click() { sample(['switch'], 0.6) || tone(1200, 600, 0.05, 0.05, 'square'); }
function popSound() { sample(['place1', 'place2'], 0.28, 1.12 + Math.random() * 0.12) || tone(260, 120, 0.12, 0.05); }
function bookSound(open) { sample([open ? 'open' : 'close'], 0.8); }
function flipSound() {
  if (sample(['flip1', 'flip2'], 0.75, 0.95 + Math.random() * 0.1)) return;
  if (!soundOn || !AC || AC.state !== 'running') return;
  const t = AC.currentTime, n = Math.floor(AC.sampleRate * 0.45), buf = AC.createBuffer(1, n, AC.sampleRate), d = buf.getChannelData(0);
  for (let i = 0; i < n; i++) d[i] = (Math.random() * 2 - 1) * Math.sin(Math.PI * i / n) ** 2;
  const s = AC.createBufferSource(), f = AC.createBiquadFilter(), g = AC.createGain();
  s.buffer = buf; f.type = 'bandpass'; f.frequency.value = 1400; f.Q.value = 0.6; g.gain.value = 0.16;
  s.connect(f).connect(g).connect(AC.destination); s.start(t);
}
$('#snd').addEventListener('click', () => {
  soundOn = !soundOn; audio();
  $('#snd').textContent = soundOn ? '♪ On' : '♪ Off'; $('#snd').setAttribute('aria-pressed', soundOn);
});

// ---------- turning pages ----------
let cur = -1, busy = false;
gsap.ticker.lagSmoothing(0);   // page turns keep to their own length even if a slow frame happens (e.g. a shader compiling)
const tw = (target, vars) => new Promise(res => gsap.to(target, { ...vars, onComplete: res }));
function popIn(i, instant) {
  const D = DIO[SP[i].id]; if (!D) return Promise.resolve();
  D.g.visible = true;
  if (instant) { D.pieces.forEach(p => p.scale.set(1, 1, 1)); return Promise.resolve(); }
  D.pieces.forEach((p, k) => {
    p.scale.set(1, 0.001, 1);
    gsap.to(p.scale, { y: 1, duration: 0.55, delay: k * 0.07, ease: 'back.out(1.7)', onStart: k % 2 ? null : popSound });
  });
  return new Promise(res => setTimeout(res, (D.pieces.length * 0.07 + 0.4) * 1000));
}
function foldOut(i, instant) {
  const D = DIO[SP[i].id]; if (!D) return Promise.resolve();
  if (instant) { D.g.visible = false; return Promise.resolve(); }
  D.pieces.forEach((p, k) => gsap.to(p.scale, { y: 0.001, duration: 0.32, delay: (D.pieces.length - 1 - k) * 0.03, ease: 'power2.in' }));
  return new Promise(res => setTimeout(() => { D.g.visible = false; res(); }, (D.pieces.length * 0.03 + 0.34) * 1000));
}
function enter(i, instant) {
  const sp = SP[i], S = SIM[sp.id];
  visited.add(sp.id);
  if (S.reset && sp.id !== 'swing') S.reset();
  if (sp.id === 'swing') { S.catchIt(); setGuide(S.th0); }
  if (sp.id === 'slide') S.place();
  ledgerStart(sp.stores);
  if (sp.stores.length) ledgerUpdate(S.values(), S.N(), instant);
  clearLabels(); stackLabels(sp.stores); spreadLabels(sp.id);
}
function leave(i) {
  ledgerClear(); clearFx(); clearLabels();
  const S = SIM[SP[i].id];
  if (S && S.run !== undefined) S.run = false;
  if (S && S.on !== undefined) S.on = false;
  if (S && S.play !== undefined) S.play = false;
}
async function go(to, instant = false) {
  if (busy || to === cur || to < 0 || to >= SP.length) return;
  busy = true; audio();
  const from = cur; cur = to;
  labelsHidden = true;
  if (!instant) { card.classList.add('fade'); await new Promise(r => setTimeout(r, 180)); }
  renderCard(to); card.classList.remove('fade');
  fitCamera(to, instant ? 0 : 1.2);
  if (from >= 1) { leave(from); await foldOut(from, instant); }
  if (from <= 0 && to >= 1) {
    drawPage(pL, 'L', to); drawPage(pR, 'R', to);
    if (instant) leftHalf.rotation.z = 0;
    else { bookSound(true); flipSound(); await tw(leftHalf.rotation, { z: 0, duration: 1.15, ease: 'power2.inOut' }); }
  } else if (to === 0) {
    if (instant) leftHalf.rotation.z = -Math.PI;
    else { flipSound(); await tw(leftHalf.rotation, { z: -Math.PI, duration: 1.05, ease: 'power2.inOut' }); bookSound(false); }
  } else {
    const fwd = to > from, st = { th: fwd ? 0 : Math.PI };
    if (fwd) { drawPage(pF, 'R', from); drawPage(pB, 'L', to); drawPage(pR, 'R', to); }
    else { drawPage(pB, 'L', from); drawPage(pF, 'R', to); drawPage(pL, 'L', to); }
    bendSheet(st.th); sheet.visible = true;
    if (!instant) { flipSound(); await tw(st, { th: fwd ? Math.PI : 0, duration: 0.95, ease: 'power2.inOut', onUpdate: () => bendSheet(st.th) }); }
    if (fwd) drawPage(pL, 'L', to); else drawPage(pR, 'R', to);
    sheet.visible = false;
  }
  if (to >= 1) { await popIn(to, instant); enter(to, instant); }
  labelsHidden = false; busy = false;
}
$('#prev').addEventListener('click', () => go(cur - 1));
$('#next').addEventListener('click', () => go(cur + 1));
$('#fold').addEventListener('click', () => {
  const f = card.classList.toggle('folded'); $('#fold').setAttribute('aria-expanded', !f);
});
const sheetEl = $('#sheet'), tabs = [$('#tabT'), $('#tabS')], panes = [$('#panT'), $('#panS')];
let sheetReturn = null;
function showTab(k) {
  tabs.forEach((t, i) => { t.setAttribute('aria-selected', i === k); t.tabIndex = i === k ? 0 : -1; panes[i].hidden = i !== k; });
  sheetEl.querySelector('.sheet-body').scrollTop = 0;
}
function openSheet(k = 0) { sheetReturn = document.activeElement; showTab(k); sheetEl.hidden = false; tabs[k].focus(); }
function closeSheet() { sheetEl.hidden = true; if (sheetReturn && sheetReturn.focus) sheetReturn.focus(); }
tabs.forEach((t, i) => t.addEventListener('click', () => showTab(i)));
$('#sheetX').addEventListener('click', closeSheet);
$('#notes').addEventListener('click', () => openSheet(0));
sheetEl.addEventListener('click', e => { if (e.target === sheetEl) closeSheet(); });
sheetEl.addEventListener('keydown', e => {
  if (e.key === 'Escape') { closeSheet(); e.preventDefault(); return; }
  const k = tabs.indexOf(document.activeElement);
  if (k >= 0 && (e.key === 'ArrowRight' || e.key === 'ArrowLeft')) {
    const n = (k + (e.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length; showTab(n); tabs[n].focus(); e.preventDefault();
  }
  if (e.key === 'Tab') {
    const f = [...sheetEl.querySelectorAll('button, a[href]')].filter(el => el.offsetParent && el.tabIndex >= 0);
    if (e.shiftKey && document.activeElement === f[0]) { f[f.length - 1].focus(); e.preventDefault(); }
    else if (!e.shiftKey && document.activeElement === f[f.length - 1]) { f[0].focus(); e.preventDefault(); }
  }
  e.stopPropagation();
});
addEventListener('keydown', e => {
  if (!sheetEl.hidden) return;
  if (e.target.closest && e.target.closest('input')) return;
  if (e.key === 'ArrowRight') go(cur + 1);
  if (e.key === 'ArrowLeft') go(cur - 1);
});

// ---------- pointer: drag the models, or drag the desk to look around ----------
const ray = new T.Raycaster(), ndc = new T.Vector2();
function setRay(e) { ndc.set(e.clientX / innerWidth * 2 - 1, -(e.clientY / innerHeight) * 2 + 1); ray.setFromCamera(ndc, camera); }
function targets() {
  const id = SP[cur]?.id;
  if (busy) return [];
  if (id === 'swing') return [{ obj: SW.bob, drag: true }];
  if (id === 'slide') return [{ obj: SL.sled, tap: () => SIM.slide.go() }];
  if (id === 'torch') return [{ obj: TO.m.t, tap: () => SIM.torch.toggle() }];
  if (id === 'end') return BA.pans.map(p => ({ obj: p, tap: () => { BA.tv += p.userData.s * 1.6; sample(['collide'], 0.5) || clink(); } }));
  return [];
}
function hit(e) {
  setRay(e);
  for (const t of targets()) {
    const meshes = []; t.obj.traverse(o => { if (o.isMesh && !o.isLineSegments2 && o.material.type !== 'MeshBasicMaterial') meshes.push(o); });
    if (ray.intersectObjects(meshes, false).length) return t;
  }
  return null;
}
let drag = null;
const swingPlane = new T.Plane(new T.Vector3(0, 0, 1), 0);
canvas.addEventListener('pointerdown', e => {
  audio(); canvas.setPointerCapture(e.pointerId);
  const t = hit(e);
  drag = t ? { t, x: e.clientX, y: e.clientY, moved: false } : { orbit: true, x: e.clientX, y: e.clientY, az: user.az, el: user.el, moved: false };
  if (t && t.drag) { drag.was = SIM.swing.run; SIM.swing.run = false; }
  canvas.classList.add('drag');
});
canvas.addEventListener('pointermove', e => {
  if (!drag) {
    if (e.pointerType === 'mouse') canvas.classList.toggle('hand', !!hit(e));
    return;
  }
  if (Math.hypot(e.clientX - drag.x, e.clientY - drag.y) > 6) drag.moved = true;
  if (drag.orbit) {
    user.az = clamp(drag.az - (e.clientX - drag.x) * 0.005, -0.75, 0.75);
    user.el = clamp(drag.el + (e.clientY - drag.y) * 0.004, -0.45, 0.5);
  } else if (drag.t.drag) {
    setRay(e); swingPlane.constant = -(SW.Z);
    if (ray.ray.intersectPlane(swingPlane, _w)) {
      const px = SW.X0, py = PY + SW.TOP - 0.006;
      SIM.swing.setPull(Math.atan2(_w.x - px, py - _w.y));
      SIM.swing.hint = false;
      const pull = cbody.querySelector('[data-ctl=pull]'); if (pull) pull.value = Math.round(Math.abs(SIM.swing.th0) * 180 / Math.PI);
    }
  }
});
function endDrag() {
  if (!drag) return;
  if (drag.t && drag.t.drag) { if (drag.moved || !drag.was) SIM.swing.release(); else SIM.swing.catchIt(); }
  else if (drag.t && !drag.moved && drag.t.tap) drag.t.tap();
  else if (drag.orbit && !drag.moved && cur === 0) go(1);
  drag = null; canvas.classList.remove('drag');
}
canvas.addEventListener('pointerup', endDrag);
canvas.addEventListener('pointercancel', endDrag);
canvas.addEventListener('dblclick', () => gsap.to(user, { az: 0, el: 0, zoom: 1, duration: 0.6 }));
canvas.addEventListener('wheel', e => { e.preventDefault(); user.zoom = clamp(user.zoom * (1 + e.deltaY * 0.001), 0.6, 1.5); }, { passive: false });

// ---------- main loop ----------
let last = performance.now();
function frame(now) {
  const dt = Math.min(0.05, (now - last) / 1000); last = now;
  if (cur >= 0 && !busy) {
    const id = SP[cur].id, S = SIM[id];
    S.step(dt);
    if (id === 'swing') SW.arm.rotation.z = S.th;
    if (id === 'slide') S.place();
    if (LG.stores.length) ledgerUpdate(S.values(), S.N());
  }
  updateCoins(dt); updateFx(dt);
  const a = leftHalf.rotation.z, sh = Math.max(0.001, PY * (1 - Math.cos(a)));
  spine.scale.y = sh; spine.position.y = sh / 2; spine.visible = sh > 0.002;
  applyCam(now / 1000);
  renderer.render(scene, camera);
  updateLabels(); updateStackLabels();
  for (const L of labels) if (L.update && L.op) L.update(L.el);
  cardTick();
  requestAnimationFrame(frame);
}

// ---------- start ----------
leftHalf.rotation.z = -Math.PI;
resize();
const startAt = clamp(parseInt(Q.get('p') || '0', 10) || 0, 0, SP.length - 1);
if (startAt > 0) { cur = 0; renderCard(0); await go(startAt, true); }
else { cur = -1; await go(0, true); }
fitCamera(cur, 0);
requestAnimationFrame(t => { last = t; frame(t); document.documentElement.dataset.ready = '1'; });
window.__book = { go, SIM, get cur() { return cur; }, get busy() { return busy; }, user, cam, sounds: () => Object.keys(SFX).length };
})();
