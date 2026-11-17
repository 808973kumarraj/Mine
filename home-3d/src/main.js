/*! =====================================================================
   3D HOME — floor plan → 3D: minimalist concrete house, furnished interior,
   walk-through + guided tour, night mode and a path-traced Photo mode.
   Interior light is baked in the browser (ray-traced global illumination:
   lightmaps + light probes + a reflection snapshot) — see BAKED GLOBAL ILLUMINATION.

   Open-source libraries bundled below as readable code (MIT licence):
     three.js              Copyright © 2010-2026 three.js authors
     three-gpu-pathtracer  Copyright (c) 2021 Garrett Johnson
     three-mesh-bvh        Copyright (c) 2018 Garrett Johnson
   Permission is hereby granted, free of charge, to any person obtaining a copy of this
   software and associated documentation files (the "Software"), to deal in the Software
   without restriction, including without limitation the rights to use, copy, modify, merge,
   publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
   to whom the Software is furnished to do so, subject to the following conditions: the above
   copyright notice and this permission notice shall be included in all copies or substantial
   portions of the Software. THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.
   ===================================================================== */
import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { RoomEnvironment } from 'three/addons/environments/RoomEnvironment.js';
import { EffectComposer } from 'three/addons/postprocessing/EffectComposer.js';
import { RenderPass } from 'three/addons/postprocessing/RenderPass.js';
import { GTAOPass } from 'three/addons/postprocessing/GTAOPass.js';
import { OutputPass } from 'three/addons/postprocessing/OutputPass.js';
import { RoundedBoxGeometry } from 'three/addons/geometries/RoundedBoxGeometry.js';
import { mergeGeometries } from 'three/addons/utils/BufferGeometryUtils.js';
import { FullScreenQuad } from 'three/addons/postprocessing/Pass.js';
// Photo mode: open-source progressive path tracer (three-gpu-pathtracer + three-mesh-bvh, MIT licence),
// bundled into this file as readable code — nothing is downloaded at runtime.
import { WebGLPathTracer, ProceduralEquirectTexture, DenoiseMaterial, PhysicalSpotLight } from 'three-gpu-pathtracer';

/* =====================================================================
   FLOOR PLAN DATA (pixels measured on the original plan image)
   Interior doors are 66 px (= 0.80 m) → 1 px ≈ 0.012 m.
   ===================================================================== */
const S = 0.012;
const OX = 541, OY = 1269;
const X = (p) => (p - OX) * S;           // plan x → world x (east +)
const Z = (p) => (p - OY) * S;           // plan y → world z (front/south +)

const WALL_H = 2.8, DOOR_H = 2.1, SILL = 0.12, HEAD = 2.6, PLINTH = 0.3, CUT_H = 1.15;
const SLAB_T = 0.18, TERR = WALL_H + SLAB_T;            // terrace floor level (house-local y)
const XW = X(2), XE = X(1080), ZF = Z(1642), ZN = Z(974), ZNN = Z(919), ZB = Z(896);

const WALLS = [
  [2, 974, 712, 993], [694, 938, 712, 974], [694, 919, 1080, 938], [1061, 938, 1080, 1623],
  [2, 1623, 1080, 1642], [2, 993, 21, 1623], [127, 896, 145, 974], [288, 896, 307, 974],
  [145, 896, 288, 915], [404, 993, 423, 1623], [21, 1273, 404, 1292], [227, 1292, 246, 1476],
  [196, 1476, 404, 1494], [196, 1494, 215, 1623], [423, 1333, 1061, 1352], [693, 1352, 712, 1623],
];
// hinge: 'a' = west/north end, 'b' = east/south end; swing: -1 north/west, +1 south/east
const OPENINGS = [
  { r: [296, 1273, 362, 1292], kind: 'door', hinge: 'a', swing: -1 },
  { r: [227, 1355, 246, 1421], kind: 'door', hinge: 'b', swing: -1 },
  { r: [404, 1388, 423, 1454], kind: 'door', hinge: 'b', swing: -1 },
  { r: [260, 1476, 326, 1494], kind: 'door', hinge: 'a', swing: 1 },
  { r: [445, 1333, 512, 1352], kind: 'door', hinge: 'a', swing: -1 },
  { r: [742, 1333, 808, 1352], kind: 'door', hinge: 'a', swing: 1 },
  { r: [496, 1623, 621, 1642], kind: 'door', main: true },
  { r: [780, 919, 844, 938], kind: 'window' },
  { r: [939, 919, 1000, 938], kind: 'window' },
  { r: [839, 1623, 948, 1642], kind: 'window' },
  { r: [154, 896, 283, 915], kind: 'window' },
  { r: [196, 974, 262, 993], kind: 'door', slide: true },   // added: kitchen → balcony glass slider
];
const ROOMS = [
  { name: 'Kitchen', floor: 'porcelainGrey', rects: [[21, 993, 404, 1273]], at: [212, 1190] },
  { name: 'Living & Dining', floor: 'porcelain', rects: [[423, 993, 1061, 1333], [712, 938, 1061, 993]], at: [745, 1245] },
  { name: 'Store Room', floor: 'tileGrey', rects: [[21, 1292, 227, 1476], [21, 1476, 196, 1623]], at: [118, 1530] },
  { name: 'Passage', floor: 'porcelain', rects: [[246, 1292, 404, 1476]], at: [325, 1335] },
  { name: 'Bathroom', floor: 'tileBath', rects: [[215, 1494, 404, 1623]], at: [290, 1578] },
  { name: 'Entrance', floor: 'porcelain', rects: [[423, 1352, 693, 1623]], at: [555, 1480] },
  { name: 'TV Room', floor: 'wood', rects: [[712, 1352, 1061, 1623]], at: [962, 1420] },
  { name: 'Balcony', floor: 'outdoor', rects: [[145, 915, 288, 974]], at: [228, 944] },
];
const OUT_PX = [[2, 974], [127, 974], [127, 896], [307, 896], [307, 974], [694, 974], [694, 919], [1080, 919], [1080, 1642], [2, 1642]];
const OUTLINE = OUT_PX.map(([x, y]) => [X(x), Z(y)]);
function inOutline(x, y) {
  let c = false;
  for (let i = 0, j = OUT_PX.length - 1; i < OUT_PX.length; j = i++) {
    const [xi, yi] = OUT_PX[i], [xj, yj] = OUT_PX[j];
    if ((yi > y) !== (yj > y) && x < ((xj - xi) * (y - yi)) / (yj - yi) + xi) c = !c;
  }
  return c;
}

/* =====================================================================
   PALETTE
   ===================================================================== */
const C = (h) => new THREE.Color(h);
const COL = {
  wall: C('#f5f3ef'), ext: C('#a6a49f'), cap: C('#2f3337'),
  casing: C('#ece5d9'), door: C('#bf9265'), mainDoor: C('#1e2023'), mainFrame: C('#2a2c2f'), groove: C('#2f3236'), brass: C('#b9bec4'),
  steel: C('#c9ced3'), frame: C('#16181a'), sill: C('#ddd6cb'), glass: C('#a9d0e2'), railGlass: C('#c4e7ec'),
  charcoal: C('#3a3e43'), teak: C('#8d552f'), soffit: C('#ebe6de'), pillar: C('#f5f1ea'), granite: C('#56524e'),
  stairBody: C('#e9e3d9'), ms: C('#25272a'), kerb: C('#bdb8af'), white: C('#f2f2ee'), terracotta: C('#b4643c'),
  oak: C('#b4946f'), walnut: C('#6b4a35'), cushion: C('#e9dfcf'),
  cabinet: C('#e6dfd3'), cabinetDoor: C('#f3eee5'), counter: C('#45484c'), fridge: C('#dce0e3'),
  black: C('#1c1e21'), screen: C('#0f1216'), kick: C('#3b3e42'),
  sofa: C('#5a7182'), sofaSeat: C('#667f90'), pillow: C('#d9c39b'), arm: C('#9b7357'), armSeat: C('#a8805f'),
  ceramic: C('#f7f7f5'), mirror: C('#b7c8d3'), closet: C('#d6c8b3'), closetDoor: C('#e2d5c1'),
  hob: C('#1d1f22'), burner: C('#5a5e63'), vase: C('#2f5d62'),
  rattan: C('#b8894f'), maroon: C('#8b2b2b'), mustard: C('#d39b2a'), tank: C('#1b1c1e'),
};
const GREENS = [C('#4c7a38'), C('#5e9044'), C('#3d6a2f')];

/* =====================================================================
   RENDERER / SCENE / SKY / LIGHTS
   ===================================================================== */
const app = document.getElementById('app');
let renderer;
try {
  renderer = new THREE.WebGLRenderer({ antialias: true, powerPreference: 'high-performance' });
} catch (e) {
  document.getElementById('err').hidden = false;
  throw e;
}
renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
renderer.setSize(window.innerWidth, window.innerHeight);
renderer.toneMapping = THREE.NeutralToneMapping;
renderer.shadowMap.enabled = true;
renderer.shadowMap.type = THREE.PCFShadowMap;
app.appendChild(renderer.domElement);
const MAX_ANISO = Math.min(8, renderer.capabilities.getMaxAnisotropy());

const scene = new THREE.Scene();
const SKY_DAY = { top: C('#86b7e0'), horizon: C('#e8eef2'), ground: C('#d9dfd6') };
const SKY_NIGHT = { top: C('#08102b'), horizon: C('#3a4369'), ground: C('#171b28') };
const skyU = { top: { value: SKY_DAY.top.clone() }, horizon: { value: SKY_DAY.horizon.clone() }, ground: { value: SKY_DAY.ground.clone() } };
scene.fog = new THREE.Fog(SKY_DAY.horizon.clone(), 95, 330);
const pmrem = new THREE.PMREMGenerator(renderer);
scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
scene.environmentIntensity = 0.55;
const camera = new THREE.PerspectiveCamera(40, window.innerWidth / window.innerHeight, 0.2, 1500);

{
  const sky = new THREE.Mesh(
    new THREE.SphereGeometry(600, 32, 16),
    new THREE.ShaderMaterial({
      side: THREE.BackSide, depthWrite: false, fog: false, uniforms: skyU,
      vertexShader: 'varying vec3 vDir; void main(){ vDir = normalize(position); gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0); }',
      fragmentShader: `uniform vec3 top; uniform vec3 horizon; uniform vec3 ground; varying vec3 vDir;
        void main(){ float h = vDir.y; vec3 c = h > 0.0 ? mix(horizon, top, pow(smoothstep(0.0, 0.55, h), 0.8)) : mix(horizon, ground, smoothstep(0.0, -0.2, h));
        gl_FragColor = vec4(c, 1.0);
        #include <tonemapping_fragment>
        #include <colorspace_fragment>
        }`,
    })
  );
  sky.renderOrder = -1;
  sky.userData.noPhoto = true;          // the path tracer draws its own sky
  scene.add(sky);
}
const starMat = new THREE.PointsMaterial({ color: 0xffffff, size: 1.7, sizeAttenuation: false, transparent: true, opacity: 0, fog: false, depthWrite: false });
const stars = (() => {
  const r = rng(77), p = [];
  for (let i = 0; i < 450; i++) {
    const th = r() * Math.PI * 2, y = 0.1 + r() * 0.9, rr = Math.sqrt(1 - y * y);
    p.push(Math.cos(th) * rr * 550, y * 550, Math.sin(th) * rr * 550);
  }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(p, 3));
  const s = new THREE.Points(g, starMat);
  s.visible = false;
  scene.add(s);
  return s;
})();

const hemi = new THREE.HemisphereLight(0xeaf2ff, 0xbcae94, 1.15);
scene.add(hemi);
const sun = new THREE.DirectionalLight(0xfff2e0, 2.35);
sun.position.set(9, 17, 12);
sun.castShadow = true;
sun.shadow.mapSize.set(2048, 2048);
Object.assign(sun.shadow.camera, { left: -14.5, right: 14.5, top: 14.5, bottom: -14.5, near: 1, far: 70 });
sun.shadow.bias = -0.0005;
sun.shadow.normalBias = 0.03;
sun.shadow.radius = 2.5;
scene.add(sun, sun.target);

const house = new THREE.Group();          // house-local: y = 0 is the finished floor
house.position.y = PLINTH;
scene.add(house);
const terrace = new THREE.Group();        // everything on the roof (fades in/out)
house.add(terrace);
const site = new THREE.Group();           // plot, garden, road (world coords)
scene.add(site);

/* =====================================================================
   BATCHED GEOMETRY (many boxes/quads in one draw call, vertex colours)
   ===================================================================== */
const FACES = [
  { n: [1, 0, 0], u: [0, 0, -1], v: [0, 1, 0] },
  { n: [-1, 0, 0], u: [0, 0, 1], v: [0, 1, 0] },
  { n: [0, 1, 0], u: [1, 0, 0], v: [0, 0, -1] },
  { n: [0, -1, 0], u: [1, 0, 0], v: [0, 0, 1] },
  { n: [0, 0, 1], u: [1, 0, 0], v: [0, 1, 0] },
  { n: [0, 0, -1], u: [-1, 0, 0], v: [0, 1, 0] },
];
const CORNERS = [[-1, -1], [1, -1], [1, 1], [-1, 1]];

class Batch {
  constructor(maxQuads, material, { cast = true, receive = true, parent = house, lm = false } = {}) {
    this.max = maxQuads; this.q = 0;
    this.lm = lm; this.strip = null;                   // lm: the uv attribute carries lightmap coordinates (see LM)
    this.pos = new Float32Array(maxQuads * 12);
    this.nor = new Float32Array(maxQuads * 12);
    this.col = new Float32Array(maxQuads * 12);
    this.uv = new Float32Array(maxQuads * 8);          // world-space UVs in metres (for textured batches)
    const idx = new Uint32Array(maxQuads * 6);
    for (let i = 0; i < maxQuads; i++) {
      const v = i * 4, o = i * 6;
      idx[o] = v; idx[o + 1] = v + 1; idx[o + 2] = v + 2; idx[o + 3] = v; idx[o + 4] = v + 2; idx[o + 5] = v + 3;
    }
    const g = this.geo = new THREE.BufferGeometry();
    g.setAttribute('position', new THREE.BufferAttribute(this.pos, 3).setUsage(THREE.DynamicDrawUsage));
    g.setAttribute('normal', new THREE.BufferAttribute(this.nor, 3).setUsage(THREE.DynamicDrawUsage));
    g.setAttribute('color', new THREE.BufferAttribute(this.col, 3).setUsage(THREE.DynamicDrawUsage));
    g.setAttribute('uv', new THREE.BufferAttribute(this.uv, 2).setUsage(THREE.DynamicDrawUsage));
    g.setIndex(new THREE.BufferAttribute(idx, 1));
    this.mesh = new THREE.Mesh(g, material);
    this.mesh.castShadow = cast; this.mesh.receiveShadow = receive;
    parent.add(this.mesh);
  }
  begin() { this.q = 0; }
  // face f of the axis-aligned box [x0,x1]×[y0,y1]×[z0,z1]; exact bounds → crack-free neighbours
  faceOf(f, x0, x1, y0, y1, z0, z1, col) {
    if (this.q >= this.max) { console.warn('Batch full'); return; }
    const F = FACES[f], P = this.pos, N = this.nor, K = this.col, U = this.uv;
    let o = this.q * 12, t = this.q * 8;
    for (let k = 0; k < 4; k++) {
      const a = CORNERS[k][0], b = CORNERS[k][1];
      P[o] = F.n[0] + F.u[0] * a + F.v[0] * b < 0 ? x0 : x1;
      P[o + 1] = F.n[1] + F.u[1] * a + F.v[1] * b < 0 ? y0 : y1;
      P[o + 2] = F.n[2] + F.u[2] * a + F.v[2] * b < 0 ? z0 : z1;
      N[o] = F.n[0]; N[o + 1] = F.n[1]; N[o + 2] = F.n[2];
      K[o] = col.r; K[o + 1] = col.g; K[o + 2] = col.b;
      if (!this.lm) worldUV(U, t, P[o], P[o + 1], P[o + 2], N[o], N[o + 1], N[o + 2]);
      else if (this.strip) { const sp = this.strip; U[t] = sp.u0 + ((sp.horiz ? P[o] : P[o + 2]) - sp.a0) * sp.ku; U[t + 1] = sp.v0 + P[o + 1] * sp.kv; }
      else { U[t] = -1; U[t + 1] = -1; }               // no lightmap for this face → lit by the light probes
      o += 3; t += 2;
    }
    this.q++;
  }
  // box: centre (cx,cy,cz), size (sx,sy,sz), rotation ry about Y, side colour, optional top colour
  box(cx, cy, cz, sx, sy, sz, ry, color, topColor) {
    const c = Math.cos(ry), s = Math.sin(ry), P = this.pos, N = this.nor, K = this.col;
    for (let f = 0; f < 6; f++) {
      if (this.q >= this.max) { console.warn('Batch full'); return; }
      const F = FACES[f], col = (f === 2 && topColor) ? topColor : color;
      const nx = F.n[0] * c + F.n[2] * s, ny = F.n[1], nz = -F.n[0] * s + F.n[2] * c;
      let o = this.q * 12, t = this.q * 8;
      for (let k = 0; k < 4; k++) {
        const a = CORNERS[k][0], b = CORNERS[k][1];
        const lx = (F.n[0] + F.u[0] * a + F.v[0] * b) * 0.5 * sx;
        const ly = (F.n[1] + F.u[1] * a + F.v[1] * b) * 0.5 * sy;
        const lz = (F.n[2] + F.u[2] * a + F.v[2] * b) * 0.5 * sz;
        P[o] = cx + lx * c + lz * s; P[o + 1] = cy + ly; P[o + 2] = cz - lx * s + lz * c;
        N[o] = nx; N[o + 1] = ny; N[o + 2] = nz;
        K[o] = col.r; K[o + 1] = col.g; K[o + 2] = col.b;
        if (this.lm) { this.uv[t] = -1; this.uv[t + 1] = -1; } else worldUV(this.uv, t, P[o], P[o + 1], P[o + 2], nx, ny, nz);
        o += 3; t += 2;
      }
      this.q++;
    }
  }
  end() {
    const g = this.geo;
    g.setDrawRange(0, this.q * 6);
    g.attributes.position.needsUpdate = true;
    g.attributes.normal.needsUpdate = true;
    g.attributes.color.needsUpdate = true;
    g.attributes.uv.needsUpdate = true;
    g.computeBoundingSphere();
  }
}
// planar world UV (metres) picked by the dominant normal axis: walls get (along, height), floors (x, z)
function worldUV(U, t, x, y, z, nx, ny, nz) {
  const ax = Math.abs(nx), ay = Math.abs(ny), az = Math.abs(nz);
  if (ay >= ax && ay >= az) { U[t] = x; U[t + 1] = -z; }
  else if (ax >= az) { U[t] = nx > 0 ? -z : z; U[t + 1] = y; }
  else { U[t] = nz > 0 ? x : -x; U[t + 1] = y; }
}

/* ---------- materials ---------- */
const TERRACE_MATS = [];   // terrace materials fade together when the terrace is toggled
function tMat(m) { TERRACE_MATS.push({ m, base: m.opacity, trans: m.transparent }); return m; }
const LAMP_MATS = [];      // emissive lamp materials (light up at night)
const PHOTO_SWAP = new Map();   // material → calmer variant used by the path tracer (tiny bright emitters cause speckles)
function lampMaterial(terr = false) {
  const m = new THREE.MeshStandardMaterial({ color: C('#fff6e6'), emissive: C('#ffc98a'), emissiveIntensity: 0, roughness: 0.4 });
  LAMP_MATS.push(m);
  return terr ? tMat(m) : m;
}
const matMatte = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.86, metalness: 0 });
const matWall = matMatte.clone(); matWall.userData.gi = 'wall';     // painted walls: lit by the baked lightmap
const matGloss = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.25, metalness: 0 });
const matMetal = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.3, metalness: 0.85 });
const matGlass = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.05, metalness: 0, transparent: true, opacity: 0.32, depthWrite: false, emissive: C('#ffc27a'), emissiveIntensity: 0 });
const lampMat = lampMaterial();
const TM = {
  matte: tMat(new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.86 })),
  metal: tMat(new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.3, metalness: 0.85 })),
  glass: tMat(new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.04, transparent: true, opacity: 0.34, depthWrite: false })),
  slabTop: tMat(new THREE.MeshStandardMaterial({ color: C('#9a9ea3'), roughness: 0.9 })),
  slabEdge: tMat(new THREE.MeshStandardMaterial({ color: C('#9b9994'), roughness: 0.88 })),
  parapet: tMat(new THREE.MeshStandardMaterial({ color: COL.ext, roughness: 0.88 })),
  coping: tMat(new THREE.MeshStandardMaterial({ color: COL.charcoal, roughness: 0.7 })),
  leaf: tMat(new THREE.MeshStandardMaterial({ roughness: 0.85, flatShading: true })),
  lamp: lampMaterial(true),
};

// board-formed concrete for every exterior wall face (world UVs → texture repeats every 2.4 m)
const concreteMap = concreteTex();
concreteMap.repeat.set(1 / 2.4, 1 / 2.4);
const matConcrete = new THREE.MeshStandardMaterial({ map: concreteMap, roughness: 0.92 });
matConcrete.userData.gi = 'off';                                   // outdoor surfaces keep the ordinary sky light
TM.slabEdge.map = concreteMap; TM.slabEdge.color.set('#ffffff');
const B = {
  wall: new Batch(9000, matWall, { lm: true }),    // walls & everything attached (re-cut in "Inside" mode)
  wallExt: new Batch(5000, matConcrete),           // exterior faces of the walls (concrete)
  wallMetal: new Batch(900, matMetal),
  wallGloss: new Batch(120, matGloss),
  glass: new Batch(300, matGlass, { cast: false, receive: false }),
  furn: new Batch(7000, matMatte),
  furnGloss: new Batch(1500, matGloss),
  furnMetal: new Batch(1200, matMetal),
  tMatte: new Batch(5000, TM.matte, { parent: terrace }),
  tMetal: new Batch(700, TM.metal, { parent: terrace }),
  tGlass: new Batch(300, TM.glass, { parent: terrace, cast: false, receive: false }),
  sMatte: new Batch(6000, matMatte, { parent: site }),
  upper: new Batch(400, matMatte),                 // wall-hung kitchen units (hidden in the cut-away view)
  upperMetal: new Batch(60, matMetal),
};
const CUT_BATCHES = [B.wall, B.wallExt, B.wallMetal, B.wallGloss, B.glass];
const STATIC_BATCHES = [B.furn, B.furnGloss, B.furnMetal, B.tMatte, B.tMetal, B.tGlass, B.sMatte, B.upper, B.upperMetal];
for (const b of STATIC_BATCHES) b.begin();

/* ---------- light fixtures for the baked lighting (see BAKED GLOBAL ILLUMINATION) ---------- */
const GI = {
  SKY: 3.2,            // daylight boost through the windows (a camera exposing for the room would see it this strong)
  LAMP: 0.8,           // candela of one recessed ceiling downlight (main beam)
  LAMP_DAY: 0.9,       // lamps are dimmed a little while the sun is up
  EXP_IN: 2.3,         // exposure indoors (the eye adapts when you walk in)
  fade: 0, target: 0, inside: 0, failed: false, collecting: false, task: null, progress: 0, rays: 0,
  env: null, envWanted: 0, envDone: 0, envNight: 0,
};
// { x, y, z (world), r, g, b (candela), cone: [cos outer, cos inner] | null = shines all round, spill: soft light to the sides }
const GI_LAMPS = [];
function giLamp(x, y, z, cd, cone = [0.3, 0.9], color = '#fff8f0', spill = 0.5) {
  const c = C(color);
  GI_LAMPS.push({ x, y: y + PLINTH, z, r: c.r * cd, g: c.g * cd, b: c.b * cd, cone, spill });
}

const GLOWS = { small: [], big: [] }, GLOW_PTS = [];      // where lamps get a soft glow sprite (house coordinates)
function glow(x, y, z, big = false) { (big ? GLOWS.big : GLOWS.small).push(x, y, z); }

/* ---------- the "cut" system: walls & wall-mounted things are re-cut in Inside mode ---------- */
const CUT_ITEMS = [], CUT_TEX = [], CUT_MESH = [];
function cutRect(b, x0, z0, x1, z1, y0, y1, color, cap = null) {
  CUT_ITEMS.push({ b, cx: (x0 + x1) / 2, cz: (z0 + z1) / 2, sx: Math.abs(x1 - x0), sz: Math.abs(z1 - z0), ry: 0, y0, y1, color, cap });
}
function cutRot(b, cx, cz, sx, sz, ry, y0, y1, color) {
  CUT_ITEMS.push({ b, cx, cz, sx, sz, ry, y0, y1, color, cap: null });
}
// mode 'scale': shortened to the cut height; 'hide': shown only while the cut is above its top
function cutMesh(m, y0, y1, mode = 'scale') { CUT_MESH.push({ m, y0, y1, mode, sy: m.scale.y }); }
function rebuildCut(h) {
  const hh = h >= WALL_H - 1e-4 ? Infinity : h;            // full height: nothing is clipped
  for (const b of CUT_BATCHES) b.begin();
  meshWalls(B.wall, B.wallExt, hh);
  for (const it of CUT_ITEMS) {
    if (it.mainDoor && (walking || GI.collecting)) continue;
    const top = Math.min(it.y1, hh);
    if (top - it.y0 < 0.003) continue;
    const capped = it.cap && it.y1 >= hh - 1e-4;
    it.b.box(it.cx, (it.y0 + top) / 2, it.cz, it.sx, top - it.y0, it.sz, it.ry, it.color, capped ? it.cap : null);
  }
  for (const b of CUT_BATCHES) b.end();
  for (const t of CUT_TEX) t.update(hh);
  for (const c of CUT_MESH) {
    if (c.mode === 'hide') { c.m.visible = hh >= Math.min(c.y1, WALL_H) - 1e-3; continue; }
    const top = Math.min(c.y1, hh);
    if (top - c.y0 < 0.003) { c.m.visible = false; continue; }
    c.m.visible = true;
    c.m.scale.y = (c.sy * (top - c.y0)) / (c.y1 - c.y0);
    c.m.position.y = c.y0 + (top - c.y0) / 2;
  }
}

/* ---------- helpers ---------- */
function fbox(b, x0, z0, x1, z1, y0, y1, col) {
  b.box((x0 + x1) / 2, (y0 + y1) / 2, (z0 + z1) / 2, Math.abs(x1 - x0), y1 - y0, Math.abs(z1 - z0), 0, col);
}
function pbox(b, px0, py0, px1, py1, y0, y1, col) { fbox(b, X(px0), Z(py0), X(px1), Z(py1), y0, y1, col); }
function frame(cx, cz, ry) {           // local frame: origin (cx,cz), local +z faces direction ry
  const c = Math.cos(ry), s = Math.sin(ry);
  return (b, lx, lz, sx, sz, y0, y1, col) => b.box(cx + lx * c + lz * s, (y0 + y1) / 2, cz - lx * s + lz * c, sx, y1 - y0, sz, ry, col);
}
const FACE = { s: 0, n: Math.PI, e: Math.PI / 2, w: -Math.PI / 2 };
const meshMats = new Map();
function matFor(parent, col, rough = 0.6, metal = 0) {
  const key = (parent === terrace ? 't' : 'h') + col.getHexString() + rough + metal;
  if (!meshMats.has(key)) {
    const m = new THREE.MeshStandardMaterial({ color: col, roughness: rough, metalness: metal });
    meshMats.set(key, parent === terrace ? tMat(m) : m);
  }
  return meshMats.get(key);
}
function cyl(cx, cz, rt, rb, y0, y1, col, { seg = 32, rough = 0.6, metal = 0, sx = 1, sz = 1, parent = house, mat = null } = {}) {
  const m = new THREE.Mesh(new THREE.CylinderGeometry(rt, rb, y1 - y0, seg), mat || matFor(parent, col, rough, metal));
  m.position.set(cx, (y0 + y1) / 2, cz);
  m.scale.set(sx, 1, sz);
  m.castShadow = m.receiveShadow = true;
  parent.add(m);
  return m;
}
const BOX1 = new THREE.BoxGeometry(1, 1, 1);
function lampBox(parent, mat, x0, x1, y0, y1, z0, z1) {
  const m = new THREE.Mesh(BOX1, mat);
  m.scale.set(x1 - x0, y1 - y0, z1 - z0);
  m.position.set((x0 + x1) / 2, (y0 + y1) / 2, (z0 + z1) / 2);
  parent.add(m);
  return m;
}
const lerp = (a, b, t) => a + (b - a) * t;
const ease = (k) => (k < 0.5 ? 4 * k * k * k : 1 - Math.pow(-2 * k + 2, 3) / 2);
const deg = THREE.MathUtils.degToRad;

/* ---------- polygons ---------- */
function polySign(pts) {
  let A = 0;
  for (let i = 0; i < pts.length; i++) { const [x1, z1] = pts[i], [x2, z2] = pts[(i + 1) % pts.length]; A += x1 * z2 - x2 * z1; }
  return A > 0 ? 1 : -1;
}
const edgeN = (p, q, s) => { const L = Math.hypot(q[0] - p[0], q[1] - p[1]); return [((q[1] - p[1]) / L) * s, (-(q[0] - p[0]) / L) * s]; };
const sameN = (a, b) => Math.abs(a[0] - b[0]) < 1e-9 && Math.abs(a[1] - b[1]) < 1e-9;
function offsetPoly(pts, d) {           // rectilinear polygon offset (d > 0 = outwards)
  const n = pts.length, s = polySign(pts), nm = pts.map((p, i) => edgeN(p, pts[(i + 1) % n], s));
  return pts.map((p, i) => {
    const a = nm[(i - 1 + n) % n], b = nm[i];
    return sameN(a, b) ? [p[0] + d * a[0], p[1] + d * a[1]] : [p[0] + d * (a[0] + b[0]), p[1] + d * (a[1] + b[1])];
  });
}
function offsetLine(pts, d, s) {        // open polyline offset using the parent polygon's orientation
  const n = pts.length;
  return pts.map((p, i) => {
    const a = i > 0 ? edgeN(pts[i - 1], p, s) : null, b = i < n - 1 ? edgeN(p, pts[i + 1], s) : null;
    const v = !a ? b : !b ? a : sameN(a, b) ? a : [a[0] + b[0], a[1] + b[1]];
    return [p[0] + d * v[0], p[1] + d * v[1]];
  });
}
function extrudeShape(outer, holes, y0, h, mat, parent) {
  const sh = new THREE.Shape(outer.map(([x, z]) => new THREE.Vector2(x, -z)));
  for (const hp of holes || []) sh.holes.push(new THREE.Path(hp.map(([x, z]) => new THREE.Vector2(x, -z))));
  const g = new THREE.ExtrudeGeometry(sh, { depth: h, bevelEnabled: false, curveSegments: 1 });
  g.rotateX(-Math.PI / 2);
  g.translate(0, y0, 0);
  const m = new THREE.Mesh(g, mat);
  m.castShadow = m.receiveShadow = true;
  parent.add(m);
  return m;
}
function flatShape(outer, holes, y, mat, parent) {   // horizontal shape; UVs are metres
  const sh = new THREE.Shape(outer.map(([x, z]) => new THREE.Vector2(x, -z)));
  for (const hp of holes || []) sh.holes.push(new THREE.Path(hp.map(([x, z]) => new THREE.Vector2(x, -z))));
  const g = new THREE.ShapeGeometry(sh);
  g.rotateX(-Math.PI / 2);
  g.translate(0, y, 0);
  const m = new THREE.Mesh(g, mat);
  m.receiveShadow = true;
  parent.add(m);
  return m;
}

/* =====================================================================
   PROCEDURAL TEXTURES
   ===================================================================== */
function rng(seed) { let s = seed; return () => (s = (s * 16807) % 2147483647) / 2147483647; }
function canvas(w, h) { const c = document.createElement('canvas'); c.width = w; c.height = h; return [c, c.getContext('2d')]; }
function toTex(c) {
  const t = new THREE.CanvasTexture(c);
  t.wrapS = t.wrapT = THREE.RepeatWrapping;
  t.colorSpace = THREE.SRGBColorSpace;
  t.anisotropy = MAX_ANISO;
  return t;
}
function addNoise(g, w, h, amt, seed) {
  const r = rng(seed), img = g.getImageData(0, 0, w, h), d = img.data;
  for (let p = 0; p < d.length; p += 4) { const k = (r() - 0.5) * 255 * amt; d[p] += k; d[p + 1] += k; d[p + 2] += k; }
  g.putImageData(img, 0, 0);
}
function woodTex({ base = [188, 142, 98], planks = 5, seed = 7, seam = 0.5 } = {}) {
  const [c, g] = canvas(512, 512); const r = rng(seed);
  const pw = 512 / planks;
  for (let i = 0; i < planks; i++) {
    const off = Math.floor(r() * 512);
    for (let k = 0; k < 2; k++) {
      const t = (r() - 0.5) * 30;
      g.fillStyle = `rgb(${(base[0] + t) | 0},${(base[1] + t * 0.8) | 0},${(base[2] + t * 0.6) | 0})`;
      for (const y0 of [off + k * 256 - 512, off + k * 256]) g.fillRect(i * pw, y0, pw, 256);
    }
    for (let s = 0; s < 26; s++) {
      const x = i * pw + r() * pw, a = 0.04 + r() * 0.09, amp = 0.8 + r() * 2.2, ph = r() * 6.28, fr = 1 + Math.floor(r() * 3);
      g.strokeStyle = `rgba(80,48,24,${a})`; g.lineWidth = 0.6 + r() * 1.3;
      g.beginPath();
      for (let y = 0; y <= 512; y += 16) { const xx = Math.min(i * pw + pw - 1, Math.max(i * pw + 1, x + Math.sin((y / 512) * Math.PI * 2 * fr + ph) * amp)); if (y) g.lineTo(xx, y); else g.moveTo(xx, y); }
      g.stroke();
    }
    g.fillStyle = `rgba(60,36,18,${seam})`;
    g.fillRect(i * pw, 0, 1.5, 512);
    for (let k = 0; k < 2; k++) g.fillRect(i * pw, (off + k * 256) % 512, pw, 1.5);
  }
  return toTex(c);
}
function tileTex({ base, grout, n = 2, jitter = 10, seed = 3, size = 256, noise = 0.05 }) {
  const [c, g] = canvas(size, size); const r = rng(seed);
  g.fillStyle = grout; g.fillRect(0, 0, size, size);
  const t = size / n, gw = Math.max(2, size / 110);
  for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
    const v = (r() - 0.5) * jitter;
    g.fillStyle = `rgb(${(base[0] + v) | 0},${(base[1] + v) | 0},${(base[2] + v) | 0})`;
    g.fillRect(i * t + gw / 2, j * t + gw / 2, t - gw, t - gw);
  }
  addNoise(g, size, size, noise, seed + 1);
  return toTex(c);
}
function grassTex(base = '#86a35e', seed = 11) {
  const [c, g] = canvas(256, 256); const r = rng(seed);
  g.fillStyle = base; g.fillRect(0, 0, 256, 256);
  for (let i = 0; i < 6500; i++) {
    g.fillStyle = `hsla(${80 + r() * 30},${30 + r() * 25}%,${32 + r() * 26}%,${0.2 + r() * 0.35})`;
    g.fillRect(r() * 256, r() * 256, 1 + r() * 2, 1 + r() * 3);
  }
  return toTex(c);
}
function stoneTex() {                     // stacked ledgestone cladding
  const W = 512; const [c, g] = canvas(W, W); const r = rng(31);
  g.fillStyle = '#3c3833'; g.fillRect(0, 0, W, W);
  const rows = []; let y = 0;
  while (y < W) { let h = 20 + Math.floor(r() * 28); if (W - y - h < 20) h = W - y; rows.push([y, h]); y += h; }
  const tones = [[178, 167, 150], [152, 142, 128], [196, 185, 167], [134, 125, 113], [170, 153, 132], [188, 172, 150]];
  for (const [ry, rh] of rows) {
    let x = -Math.floor(r() * 140);
    const xEnd = x + W;
    while (x < xEnd) {
      let w = 44 + Math.floor(r() * 120);
      if (xEnd - (x + w) < 34) w = xEnd - x;
      const tn = tones[Math.floor(r() * tones.length)], v = (r() - 0.5) * 22;
      const fill = `rgb(${(tn[0] + v) | 0},${(tn[1] + v) | 0},${(tn[2] + v) | 0})`;
      for (const ox of [0, W, -W]) {
        const sx = x + ox;
        if (sx + w < 0 || sx > W) continue;
        g.fillStyle = fill; g.fillRect(sx + 1.5, ry + 1.5, w - 3, rh - 3);
        g.fillStyle = 'rgba(255,255,255,0.18)'; g.fillRect(sx + 1.5, ry + 1.5, w - 3, 2);
        g.fillStyle = 'rgba(0,0,0,0.24)'; g.fillRect(sx + 1.5, ry + rh - 4, w - 3, 2.5);
      }
      x += w;
    }
  }
  addNoise(g, W, W, 0.07, 32);
  return toTex(c);
}
function jaaliTex() {                     // horizontal timber slat screen (alpha gaps)
  const W = 256; const [c, g] = canvas(W, W);
  for (let y = 0; y < W; y += 32) { g.fillStyle = '#8d5a33'; g.fillRect(0, y, W, 22); g.fillStyle = 'rgba(255,255,255,0.08)'; g.fillRect(0, y, W, 3); }
  addNoise(g, W, W, 0.04, 44);
  g.globalCompositeOperation = 'destination-out';
  for (let y = 22; y < W; y += 32) g.fillRect(0, y, W, 10);
  g.globalCompositeOperation = 'source-over';
  return toTex(c);
}
function graniteTex(base = [150, 148, 144], seed = 41) {
  const [c, g] = canvas(256, 256); const r = rng(seed);
  g.fillStyle = `rgb(${base.join(',')})`; g.fillRect(0, 0, 256, 256);
  for (let i = 0; i < 9000; i++) {
    const t = r();
    g.fillStyle = t < 0.4 ? 'rgba(18,18,18,0.5)' : t < 0.7 ? 'rgba(205,200,192,0.35)' : 'rgba(126,114,102,0.45)';
    g.fillRect(r() * 256, r() * 256, 1 + r() * 1.8, 1 + r() * 1.8);
  }
  return toTex(c);
}
function paverTex(palette, seed = 51) {   // running-bond paver blocks
  const W = 512; const [c, g] = canvas(W, W); const r = rng(seed);
  g.fillStyle = '#6c645b'; g.fillRect(0, 0, W, W);
  const bw = W / 4, bh = W / 8, tone = {};
  for (let j = 0; j < 8; j++) for (let i = 0; i < 4; i++) tone[i + ',' + j] = [palette[Math.floor(r() * palette.length)], (r() - 0.5) * 18];
  for (let j = 0; j < 8; j++) for (let i = -1; i < 5; i++) {
    const [b, v] = tone[((i % 4) + 4) % 4 + ',' + j];
    g.fillStyle = `rgb(${(b[0] + v) | 0},${(b[1] + v) | 0},${(b[2] + v) | 0})`;
    g.fillRect(i * bw + (j % 2) * (bw / 2) + 2.5, j * bh + 2.5, bw - 5, bh - 5);
  }
  addNoise(g, W, W, 0.06, seed + 1);
  return toTex(c);
}
function asphaltTex() {
  const [c, g] = canvas(256, 256); const r = rng(61);
  g.fillStyle = '#46484b'; g.fillRect(0, 0, 256, 256);
  for (let i = 0; i < 7000; i++) { g.fillStyle = r() < 0.5 ? 'rgba(20,20,22,0.35)' : 'rgba(150,150,150,0.22)'; g.fillRect(r() * 256, r() * 256, 1 + r() * 1.5, 1 + r() * 1.5); }
  return toTex(c);
}
function trunkTex() {
  const [c, g] = canvas(64, 256);
  g.fillStyle = '#8a735b'; g.fillRect(0, 0, 64, 256);
  for (let y = 0; y < 256; y += 16) { g.fillStyle = 'rgba(58,44,32,0.55)'; g.fillRect(0, y, 64, 3); g.fillStyle = 'rgba(196,176,146,0.25)'; g.fillRect(0, y + 4, 64, 2); }
  addNoise(g, 64, 256, 0.08, 72);
  return toTex(c);
}
function rangoliTex() {
  const W = 512; const [c, g] = canvas(W, W);
  g.translate(W / 2, W / 2);
  const petals = (r0, r1, w, n, col, rot = 0) => {
    g.fillStyle = col;
    for (let i = 0; i < n; i++) {
      g.save(); g.rotate(rot + (i * Math.PI * 2) / n);
      g.beginPath(); g.moveTo(0, r0); g.quadraticCurveTo(w, (r0 + r1) / 2, 0, r1); g.quadraticCurveTo(-w, (r0 + r1) / 2, 0, r0); g.fill();
      g.restore();
    }
  };
  const disc = (r, col) => { g.beginPath(); g.arc(0, 0, r, 0, Math.PI * 2); g.fillStyle = col; g.fill(); };
  const dots = (r, n, rad, col, rot = 0) => { g.fillStyle = col; for (let i = 0; i < n; i++) { const a = rot + (i * Math.PI * 2) / n; g.beginPath(); g.arc(Math.cos(a) * r, Math.sin(a) * r, rad, 0, Math.PI * 2); g.fill(); } };
  petals(160, 250, 44, 16, '#e0336f');
  petals(165, 234, 22, 16, '#ffd23a', Math.PI / 16);
  disc(162, '#ffffff'); disc(154, '#27a99a');
  petals(84, 152, 34, 12, '#ff8a1c');
  petals(92, 140, 13, 12, '#fff1d0');
  disc(86, '#ffffff'); disc(79, '#3557d6');
  petals(24, 78, 20, 8, '#ffcc29');
  petals(30, 66, 8, 8, '#e0336f', Math.PI / 8);
  disc(24, '#ffffff'); disc(17, '#e0336f');
  dots(206, 32, 5, '#ffffff', Math.PI / 32);
  dots(118, 24, 4.5, '#ffffff');
  const t = toTex(c);
  t.wrapS = t.wrapT = THREE.ClampToEdgeWrapping;
  return t;
}

// board-formed architectural concrete: one 2.4 m tile = 2 × 4 form panels (1.2 × 0.6 m) with tie holes
function concreteTex() {
  const W = 1024, m = W / 2.4; const [c, g] = canvas(W, W); const r = rng(91);
  g.fillStyle = '#a9a8a3'; g.fillRect(0, 0, W, W);
  const wrapped = (x, y, rad, fn) => { for (const ox of [-W, 0, W]) for (const oy of [-W, 0, W]) { if (x + ox + rad < 0 || x + ox - rad > W || y + oy + rad < 0 || y + oy - rad > W) continue; fn(x + ox, y + oy); } };
  for (let i = 0; i < 320; i++) {          // soft cloudy mottling
    const x = r() * W, y = r() * W, rad = 25 + r() * 150, lt = r() < 0.5, a = 0.025 + r() * 0.05;
    wrapped(x, y, rad, (px, py) => {
      const grd = g.createRadialGradient(px, py, 0, px, py, rad);
      grd.addColorStop(0, lt ? `rgba(255,255,250,${a})` : `rgba(45,44,42,${a})`); grd.addColorStop(1, 'rgba(128,128,128,0)');
      g.fillStyle = grd; g.fillRect(px - rad, py - rad, rad * 2, rad * 2);
    });
  }
  for (let i = 0; i < 2; i++) for (let j = 0; j < 4; j++) {   // each form panel cures a slightly different tone
    const v = r(); g.fillStyle = v < 0.5 ? `rgba(255,255,255,${0.02 + v * 0.06})` : `rgba(30,30,30,${0.02 + (v - 0.5) * 0.06})`;
    g.fillRect(i * 1.2 * m, j * 0.6 * m, 1.2 * m, 0.6 * m);
  }
  for (let i = 0; i < 2200; i++) {         // tiny air pockets
    const x = r() * W, y = r() * W, rad = 0.6 + r() * 1.6;
    g.fillStyle = `rgba(55,54,52,${0.25 + r() * 0.35})`; g.beginPath(); g.arc(x, y, rad, 0, Math.PI * 2); g.fill();
  }
  for (let j = 0; j <= 4; j++) {           // horizontal form joints
    const y = Math.round(j * 0.6 * m);
    g.fillStyle = 'rgba(60,60,58,0.38)'; g.fillRect(0, y - 1.5, W, 2.5);
    g.fillStyle = 'rgba(255,255,255,0.16)'; g.fillRect(0, y + 1, W, 1.5);
  }
  for (let i = 0; i <= 2; i++) {           // vertical form joints
    const x = Math.round(i * 1.2 * m);
    g.fillStyle = 'rgba(60,60,58,0.3)'; g.fillRect(x - 1.5, 0, 2.5, W);
    g.fillStyle = 'rgba(255,255,255,0.12)'; g.fillRect(x + 1, 0, 1.5, W);
  }
  for (let i = 0; i < 2; i++) for (let j = 0; j < 4; j++) for (const hx of [0.3, 0.9]) for (const hy of [0.15, 0.45]) {   // tie holes
    const x = (i * 1.2 + hx) * m, y = (j * 0.6 + hy) * m;
    g.fillStyle = 'rgba(255,255,255,0.14)'; g.beginPath(); g.arc(x, y + 1, 8.5, 0, Math.PI * 2); g.fill();
    g.fillStyle = 'rgba(52,51,49,0.85)'; g.beginPath(); g.arc(x, y, 6, 0, Math.PI * 2); g.fill();
    g.fillStyle = 'rgba(20,20,20,0.6)'; g.beginPath(); g.arc(x, y - 1, 3.5, 0, Math.PI * 2); g.fill();
  }
  addNoise(g, W, W, 0.05, 92);
  return toTex(c);
}
// large-format porcelain floor tiles (n × n tiles per texture) with faint stone veining
function porcelainTex({ base = [219, 213, 202], grout = 'rgba(150,141,129,0.85)', n = 2, seed = 95, veins = 0.05 } = {}) {
  const W = 1024, t = W / n; const [c, g] = canvas(W, W); const r = rng(seed);
  for (let i = 0; i < n; i++) for (let j = 0; j < n; j++) {
    const v = (r() - 0.5) * 7;
    g.fillStyle = `rgb(${(base[0] + v) | 0},${(base[1] + v) | 0},${(base[2] + v) | 0})`;
    g.fillRect(i * t, j * t, t, t);
    g.save(); g.beginPath(); g.rect(i * t, j * t, t, t); g.clip();
    for (let k = 0; k < 7; k++) {
      let x = i * t + r() * t, y = j * t + r() * t, a = r() * Math.PI;
      g.strokeStyle = `rgba(120,112,100,${veins * (0.4 + r())})`; g.lineWidth = 1 + r() * 3;
      g.beginPath(); g.moveTo(x, y);
      for (let s2 = 0; s2 < 30; s2++) { a += (r() - 0.5) * 0.5; x += Math.cos(a) * 14; y += Math.sin(a) * 14; g.lineTo(x, y); }
      g.stroke();
    }
    g.restore();
  }
  g.fillStyle = grout;
  for (let i = 0; i <= n; i++) { g.fillRect(i * t - 1, 0, 2, W); g.fillRect(0, i * t - 1, W, 2); }
  addNoise(g, W, W, 0.02, seed + 1);
  return toTex(c);
}
// white marble with soft grey veins (seamless)
function marbleTex(seed = 97, base = '#f2efea', vein = [118, 114, 108]) {
  const W = 1024; const [c, g] = canvas(W, W); const r = rng(seed);
  g.fillStyle = base; g.fillRect(0, 0, W, W);
  for (let i = 0; i < 90; i++) {
    const x = r() * W, y = r() * W, rad = 60 + r() * 220, a = 0.02 + r() * 0.035;
    for (const ox of [-W, 0, W]) for (const oy of [-W, 0, W]) {
      const px = x + ox, py = y + oy; if (px + rad < 0 || px - rad > W || py + rad < 0 || py - rad > W) continue;
      const grd = g.createRadialGradient(px, py, 0, px, py, rad);
      grd.addColorStop(0, `rgba(${vein.join(',')},${a})`); grd.addColorStop(1, `rgba(${vein.join(',')},0)`);
      g.fillStyle = grd; g.fillRect(px - rad, py - rad, rad * 2, rad * 2);
    }
  }
  const path = (x, y, a, n, step, wob) => { const pts = [[x, y]]; for (let i = 0; i < n; i++) { a += (r() - 0.5) * wob; x += Math.cos(a) * step; y += Math.sin(a) * step; pts.push([x, y]); } return pts; };
  const stroke = (pts, w, alpha) => {
    for (const [ww, aa] of [[w * 5, alpha * 0.08], [w * 2.2, alpha * 0.22], [w, alpha]]) {
      g.strokeStyle = `rgba(${vein.join(',')},${aa})`; g.lineWidth = ww; g.lineJoin = g.lineCap = 'round';
      for (const ox of [-W, 0, W]) for (const oy of [-W, 0, W]) {
        g.beginPath(); pts.forEach(([x, y], i) => (i ? g.lineTo(x + ox, y + oy) : g.moveTo(x + ox, y + oy))); g.stroke();
      }
    }
  };
  for (let k = 0; k < 7; k++) {
    const main = path(r() * W, r() * W, -0.6 + (r() - 0.5) * 0.8, 70, 16, 0.55);
    stroke(main, 1.4 + r() * 2.2, 0.55 + r() * 0.35);
    for (let b = 0; b < 4; b++) { const [bx, by] = main[Math.floor(r() * main.length)]; stroke(path(bx, by, r() * 6.28, 18, 10, 0.9), 0.6 + r() * 0.8, 0.35 + r() * 0.3); }
  }
  addNoise(g, W, W, 0.015, seed + 2);
  return toTex(c);
}
// vertical timber slats / fluted veneer (512 px ≙ 0.64 m, groove every 4 cm)
// long pale timber boards for the ceiling: 1024 px ≙ 4.2 m along the boards, 512 px ≙ 12 boards of 14 cm
function plankTex({ base = [217, 201, 179], rows = 12, seed = 31 } = {}) {
  const W = 1024, H = 512, ph = H / rows; const [c, g] = canvas(W, H); const r = rng(seed);
  for (let k = 0; k < rows; k++) {
    const j0 = r() * W, j1 = j0 + W * (0.35 + r() * 0.3);                 // two boards per row, butt joints at random places
    for (const [a, b] of [[j0, j1], [j1, j0 + W]]) {
      const v = (r() - 0.5) * 15;
      g.fillStyle = `rgb(${(base[0] + v) | 0},${(base[1] + v * 0.9) | 0},${(base[2] + v * 0.8) | 0})`;
      for (const off of [-W, 0]) g.fillRect(a + off, k * ph, b - a, ph);
      for (let i = 0; i < 22; i++) {                                     // long soft grain lines
        const y = k * ph + 2 + r() * (ph - 4), x0 = a + r() * (b - a), len = 80 + r() * 420, w1 = (r() - 0.5) * 3, w2 = (r() - 0.5) * 3;
        g.strokeStyle = `rgba(122,94,62,${0.035 + r() * 0.06})`; g.lineWidth = 0.6 + r() * 0.9;
        for (const off of [-W, 0, W]) { g.beginPath(); g.moveTo(x0 + off, y); g.bezierCurveTo(x0 + off + len / 3, y + w1, x0 + off + (2 * len) / 3, y + w2, x0 + off + len, y); g.stroke(); }
      }
      g.fillStyle = 'rgba(74,56,38,0.45)';
      for (const off of [-W, 0, W]) g.fillRect(a + off - 1, k * ph, 2, ph);
    }
    g.fillStyle = 'rgba(62,46,30,0.5)'; g.fillRect(0, k * ph - 1, W, 2);    // shadow gap between boards
    g.fillStyle = 'rgba(255,249,238,0.2)'; g.fillRect(0, k * ph + 1, W, 1.2);
  }
  addNoise(g, W, H, 0.022, seed + 1);
  return toTex(c);
}
function slatTex(base = [176, 128, 84], seed = 98) {
  const [c, g] = canvas(512, 512); const r = rng(seed);
  for (let x = 0; x < 512; x += 32) {
    const v = (r() - 0.5) * 22;
    g.fillStyle = `rgb(${(base[0] + v) | 0},${(base[1] + v * 0.8) | 0},${(base[2] + v * 0.6) | 0})`; g.fillRect(x, 0, 32, 512);
    for (let k = 0; k < 7; k++) {
      const gx = x + 3 + r() * 26; g.strokeStyle = `rgba(70,42,20,${0.06 + r() * 0.1})`; g.lineWidth = 0.6 + r();
      g.beginPath(); g.moveTo(gx, 0); for (let y = 0; y <= 512; y += 32) g.lineTo(gx + Math.sin(y * 0.02 + k) * 1.2, y); g.stroke();
    }
    g.fillStyle = 'rgba(40,24,12,0.55)'; g.fillRect(x, 0, 3, 512);
    g.fillStyle = 'rgba(255,240,220,0.18)'; g.fillRect(x + 3, 0, 2, 512);
  }
  return toTex(c);
}
// upholstery weave (bouclé-like flecks); tinted by the material colour
function fabricTex(seed = 99) {
  const [c, g] = canvas(256, 256); const r = rng(seed);
  g.fillStyle = '#e9e9e9'; g.fillRect(0, 0, 256, 256);
  for (let y = 0; y < 256; y += 4) { g.fillStyle = 'rgba(0,0,0,0.045)'; g.fillRect(0, y, 256, 1.5); }
  for (let x = 0; x < 256; x += 4) { g.fillStyle = 'rgba(255,255,255,0.05)'; g.fillRect(x, 0, 1.5, 256); }
  for (let i = 0; i < 2600; i++) {
    g.fillStyle = r() < 0.5 ? `rgba(255,255,255,${0.15 + r() * 0.2})` : `rgba(0,0,0,${0.06 + r() * 0.08})`;
    g.beginPath(); g.arc(r() * 256, r() * 256, 0.6 + r() * 1.3, 0, Math.PI * 2); g.fill();
  }
  addNoise(g, 256, 256, 0.05, seed + 1);
  return toTex(c);
}

const TX = {
  stone: stoneTex(),
  teak: woodTex({ base: [146, 90, 52], planks: 6, seed: 19, seam: 0.65 }),
  granite: graniteTex(),
  jaali: jaaliTex(),
  terraceTile: tileTex({ base: [206, 166, 128], grout: '#98755a', n: 2, jitter: 18, seed: 23 }),
};
TX.terraceTile.repeat.set(1 / 0.6, 1 / 0.6);
const M = {
  stone: new THREE.MeshStandardMaterial({ map: TX.stone, roughness: 0.92 }),
  teak: new THREE.MeshStandardMaterial({ map: TX.teak, roughness: 0.62 }),
  granite: new THREE.MeshStandardMaterial({ map: TX.granite, color: C('#d8d6d2'), roughness: 0.8 }),
  jaali: new THREE.MeshStandardMaterial({ map: TX.jaali, alphaTest: 0.5, side: THREE.DoubleSide, roughness: 0.6, metalness: 0 }),
};
for (const m of Object.values(M)) m.userData.gi = 'off';
TM.tile = tMat(new THREE.MeshStandardMaterial({ map: TX.terraceTile, roughness: 0.8 }));
TM.stone = tMat(new THREE.MeshStandardMaterial({ map: TX.stone, roughness: 0.92 }));

/* ---------- textured boxes with world-space UVs (cuttable) ---------- */
class TexBox {
  constructor(x0, x1, y0, y1, z0, z1, material, size, { parent = house, cut = true, faces = 63, cast = true } = {}) {
    this.b = [x0, x1, y0, y1, z0, z1]; this.size = size; this.faces = faces;
    this.pos = new Float32Array(72); this.nor = new Float32Array(72); this.uv = new Float32Array(48);
    const idx = [];
    for (let i = 0; i < 6; i++) { const v = i * 4; idx.push(v, v + 1, v + 2, v, v + 2, v + 3); }
    const g = this.geo = new THREE.BufferGeometry();
    g.setAttribute('position', new THREE.BufferAttribute(this.pos, 3));
    g.setAttribute('normal', new THREE.BufferAttribute(this.nor, 3));
    g.setAttribute('uv', new THREE.BufferAttribute(this.uv, 2));
    g.setIndex(idx);
    this.mesh = new THREE.Mesh(g, material);
    this.mesh.castShadow = cast; this.mesh.receiveShadow = true;
    parent.add(this.mesh);
    this.update(Infinity);
    if (cut) CUT_TEX.push(this);
  }
  update(h) {
    const [x0, x1, y0, y1, z0, z1] = this.b, top = Math.min(y1, h), s = this.size;
    if (top - y0 < 0.003) { this.mesh.visible = false; return; }
    this.mesh.visible = true;
    let q = 0;
    for (let f = 0; f < 6; f++) {
      if (!(this.faces & (1 << f))) continue;
      const F = FACES[f];
      for (let k = 0; k < 4; k++) {
        const a = CORNERS[k][0], b = CORNERS[k][1];
        const px = F.n[0] + F.u[0] * a + F.v[0] * b < 0 ? x0 : x1;
        const py = F.n[1] + F.u[1] * a + F.v[1] * b < 0 ? y0 : top;
        const pz = F.n[2] + F.u[2] * a + F.v[2] * b < 0 ? z0 : z1;
        const o = (q * 4 + k) * 3, t = (q * 4 + k) * 2;
        this.pos[o] = px; this.pos[o + 1] = py; this.pos[o + 2] = pz;
        this.nor[o] = F.n[0]; this.nor[o + 1] = F.n[1]; this.nor[o + 2] = F.n[2];
        this.uv[t] = (f < 2 ? pz : px) / s; this.uv[t + 1] = (f === 2 || f === 3 ? pz : py) / s;
      }
      q++;
    }
    const g = this.geo;
    g.setDrawRange(0, q * 6);
    g.attributes.position.needsUpdate = true; g.attributes.normal.needsUpdate = true; g.attributes.uv.needsUpdate = true;
    g.computeBoundingSphere();
  }
}

/* ---------- instanced foliage (one draw call per area) ---------- */
const FOL = { house: [], terrace: [], site: [] };
const DOM_PARENT = { house, terrace, site };
function blob(dom, x, y, z, r, col, sy = 1) { FOL[dom].push({ x, y, z, r, col, sy }); }
const leafGeo = new THREE.IcosahedronGeometry(1, 1);
function buildFoliage(dom, mat) {
  const list = FOL[dom];
  if (!list.length) return;
  const im = new THREE.InstancedMesh(leafGeo, mat, list.length);
  const m4 = new THREE.Matrix4(), q = new THREE.Quaternion(), e = new THREE.Euler(), p = new THREE.Vector3(), s = new THREE.Vector3();
  list.forEach((b, i) => {
    e.set(i * 1.3, i * 2.1, 0); q.setFromEuler(e);
    im.setMatrixAt(i, m4.compose(p.set(b.x, b.y, b.z), q, s.set(b.r, b.r * b.sy, b.r)));
    im.setColorAt(i, b.col);
  });
  im.castShadow = im.receiveShadow = true;
  im.frustumCulled = false;
  DOM_PARENT[dom].add(im);
}
const PLANT_BLOBS = [[0, 0.62, 0, 0.24], [0.13, 0.52, 0.06, 0.18], [-0.11, 0.54, -0.08, 0.19], [0.05, 0.82, -0.05, 0.17], [-0.07, 0.75, 0.11, 0.16], [0.1, 0.7, -0.12, 0.14]];
function plant(dom, cx, cz, s = 1, y = 0, potCol = COL.terracotta) {
  const parent = DOM_PARENT[dom];
  cyl(cx, cz, 0.17 * s, 0.13 * s, y, y + 0.36 * s, potCol, { rough: 0.85, seg: 20, parent });
  cyl(cx, cz, 0.155 * s, 0.155 * s, y + 0.33 * s, y + 0.35 * s, C('#4a3828'), { rough: 1, seg: 20, parent });
  PLANT_BLOBS.forEach(([bx, by, bz, br], i) => blob(dom, cx + bx * s, y + by * s, cz + bz * s, br * s, GREENS[i % 3]));
}

/* =====================================================================
   FLOORS
   ===================================================================== */
const FLOORS = {
  wood: { tex: woodTex(), size: 1.0, rough: 0.55 },
  tileWarm: { tex: tileTex({ base: [226, 216, 200], grout: '#b4ab9c', n: 2 }), size: 1.2, rough: 0.4 },
  tileGrey: { tex: tileTex({ base: [215, 214, 209], grout: '#a8a7a1', n: 2, seed: 5 }), size: 1.2, rough: 0.5 },
  tileBath: { tex: tileTex({ base: [205, 219, 225], grout: '#9db0b8', n: 4, seed: 9 }), size: 1.2, rough: 0.35 },
  stone: { tex: tileTex({ base: [222, 216, 205], grout: '#a29a8d', n: 2, jitter: 20, seed: 13 }), size: 1.6, rough: 0.45 },
  terracotta: { tex: tileTex({ base: [192, 116, 80], grout: '#8a573e', n: 3, jitter: 24, seed: 17 }), size: 0.9, rough: 0.85 },
  porcelain: { tex: porcelainTex(), size: 2.4, rough: 0.3 },                                           // 1.2 m polished tiles
  porcelainGrey: { tex: porcelainTex({ base: [214, 212, 207], seed: 96, veins: 0.035 }), size: 2.4, rough: 0.32 },
  outdoor: { tex: porcelainTex({ base: [176, 174, 168], n: 4, seed: 94, veins: 0.03, grout: 'rgba(104,100,95,0.9)' }), size: 2.4, rough: 0.75 },
};
for (const f of Object.values(FLOORS)) { f.mat = new THREE.MeshStandardMaterial({ map: f.tex, roughness: f.rough }); f.mat.userData.gi = 'floor'; }
function groundRect(parent, x0, z0, x1, z1, y, mat, size) {
  const geo = new THREE.PlaneGeometry(x1 - x0, z1 - z0);
  geo.rotateX(-Math.PI / 2);
  geo.translate((x0 + x1) / 2, y, (z0 + z1) / 2);
  const p = geo.attributes.position, uv = geo.attributes.uv;
  for (let i = 0; i < p.count; i++) uv.setXY(i, p.getX(i) / size, -p.getZ(i) / size);
  const m = new THREE.Mesh(geo, mat);
  m.receiveShadow = true;
  parent.add(m);
  return m;
}
for (const room of ROOMS) {
  const f = FLOORS[room.floor];
  for (const [a, b, c, d] of room.rects) groundRect(house, X(a - 4), Z(b - 4), X(c + 4), Z(d + 4), 0.003, f.mat, f.size);
}

/* =====================================================================
   WALLS — each wall is a grid of cells (along × height bands); only faces
   between solid and empty cells are emitted (no hidden faces to flicker).
   Faces that look outside get the exterior paint.
   ===================================================================== */
const overlap = (r, w) => r[0] < w[2] && r[2] > w[0] && r[1] < w[3] && r[3] > w[1];
const inside = (o, p) => p[0] >= o[0] && p[2] <= o[2] && p[1] >= o[1] && p[3] <= o[3];
const WALL_INFO = WALLS.map((w) => {
  const [x0, y0, x1, y1] = w;
  const horiz = (x1 - x0) >= (y1 - y0);
  const A0 = horiz ? x0 : y0, A1 = horiz ? x1 : y1, C0 = horiz ? y0 : x0, C1 = horiz ? y1 : x1;
  const probe = (a0, a1) => (horiz ? [a0, C0 + 1, a1, C1 - 1] : [C0 + 1, a0, C1 - 1, a1]);
  const abut0 = WALLS.some((o) => o !== w && inside(o, probe(A0 - 1, A0)));
  const abut1 = WALLS.some((o) => o !== w && inside(o, probe(A1, A1 + 1)));
  const ops = OPENINGS.filter((o) => overlap(o.r, w)).map((o) => ({
    a: Math.max(horiz ? o.r[0] : o.r[1], A0), b: Math.min(horiz ? o.r[2] : o.r[3], A1),
    lo: o.kind === 'window' ? SILL : 0, hi: o.kind === 'window' ? HEAD : DOOR_H,
  }));
  const coverOf = (cLo, cHi) => WALLS.filter((o) => o !== w && overlap(o, horiz ? [A0, cLo, A1, cHi] : [cLo, A0, cHi, A1]))
    .map((o) => (horiz ? [Math.max(o[0], A0), Math.min(o[2], A1)] : [Math.max(o[1], A0), Math.min(o[3], A1)]));
  return { horiz, A0, A1, C0, C1, abut0, abut1, ops, cover0: coverOf(C0 - 1, C0), cover1: coverOf(C1, C1 + 1) };
});
/* ---------- lightmap atlas layout (filled by BAKED GLOBAL ILLUMINATION) ----------
   One texture holds the floor map (5 cm texels over the whole footprint), the ceiling map (12 cm)
   and one strip per interior wall face (8 cm). Wall faces get their atlas UVs while being meshed. */
const LM = { W: 1024, H: 0, floor: null, ceil: null, strips: [] };
{
  const pad = 0.1, fr = 0.05, cr = 0.12, wr = 0.08;
  const x0 = XW - pad, z0 = ZB - pad, w = XE - XW + 2 * pad, d = ZF - ZB + 2 * pad;
  LM.floor = { x: 1, y: 1, nu: Math.ceil(w / fr), nv: Math.ceil(d / fr), res: fr, x0, z0 };
  LM.ceil = { x: LM.floor.nu + 3, y: 1, nu: Math.ceil(w / cr), nv: Math.ceil(d / cr), res: cr, x0, z0 };
  const nv = Math.ceil(WALL_H / wr);
  let cx = 1, cy = LM.floor.nv + 3;
  for (const W of WALL_INFO) {
    W.strips = [null, null];
    for (const side of [0, 1]) {
      const cPx = side ? W.C1 + 3 : W.C0 - 3;           // a face is indoors if a point just in front of it is inside the outline
      let indoors = false;
      for (let k = 0; k < 9 && !indoors; k++) { const a = W.A0 + ((k + 0.5) * (W.A1 - W.A0)) / 9; indoors = W.horiz ? inOutline(a, cPx) : inOutline(cPx, a); }
      if (!indoors) continue;
      const a0 = (W.horiz ? X : Z)(W.A0 - 1), a1 = (W.horiz ? X : Z)(W.A1 + 1), nu = Math.ceil((a1 - a0) / wr);
      if (cx + nu + 1 > LM.W) { cx = 1; cy += nv + 2; }
      const sp = { x: cx, y: cy, nu, nv, res: wr, a0, horiz: W.horiz, c: (W.horiz ? Z : X)(side ? W.C1 : W.C0), sign: side ? 1 : -1 };
      cx += nu + 2;
      W.strips[side] = sp; LM.strips.push(sp);
    }
  }
  LM.H = cy + nv + 2;
  // texture = two layers stacked vertically (daylight below, lamp light above) → v spans half the height
  for (const sp of LM.strips) { sp.ku = 1 / (sp.res * LM.W); sp.u0 = sp.x / LM.W; sp.kv = 1 / (sp.res * LM.H * 2); sp.v0 = sp.y / (LM.H * 2); }
}
const JOIN = 0.15;
function meshWalls(b, bExt, Hc) {
  const hs = [...new Set([0, SILL, DOOR_H, HEAD, WALL_H].map((h) => Math.min(h, Hc)))].sort((p, q) => p - q);
  const nH = hs.length - 1;
  for (const W of WALL_INFO) {
    const along = [...new Set([W.abut0 ? W.A0 - JOIN : W.A0, W.abut1 ? W.A1 + JOIN : W.A1,
      ...W.ops.flatMap((o) => [o.a, o.b]), ...W.cover0.flat(), ...W.cover1.flat()])].sort((p, q) => p - q);
    const nA = along.length - 1;
    const covered = (cov, m) => cov.some(([p, q]) => m > p && m < q);
    const solid = (i, j) => {
      if (i < 0 || i >= nA || j < 0 || j >= nH) return false;
      const am = (along[i] + along[i + 1]) / 2, hm = (hs[j] + hs[j + 1]) / 2;
      return !W.ops.some((o) => am > o.a && am < o.b && hm > o.lo && hm < o.hi);
    };
    const toW = W.horiz ? X : Z;
    const c0 = W.horiz ? Z(W.C0) : X(W.C0), c1 = W.horiz ? Z(W.C1) : X(W.C1), Cm = (W.C0 + W.C1) / 2;
    const extAt = (a, c) => (W.horiz ? !inOutline(a, c) : !inOutline(c, a));
    const colLo = extAt(along[0] - 3, Cm) ? COL.ext : COL.wall, colHi = extAt(along[nA] + 3, Cm) ? COL.ext : COL.wall;
    const F_C1 = W.horiz ? 4 : 0, F_C0 = W.horiz ? 5 : 1, F_LO = W.horiz ? 1 : 5, F_HI = W.horiz ? 0 : 4;
    for (let i = 0; i < nA; i++) {
      const a0 = toW(along[i]), a1 = toW(along[i + 1]), mPx = (along[i] + along[i + 1]) / 2;
      const show0 = !covered(W.cover0, mPx), show1 = !covered(W.cover1, mPx);
      const e0 = extAt(mPx, W.C0 - 3), e1 = extAt(mPx, W.C1 + 3);
      const col0 = e0 ? COL.ext : COL.wall, col1 = e1 ? COL.ext : COL.wall, colR = e0 || e1 ? COL.ext : COL.wall;
      for (let j = 0; j < nH; j++) {
        if (!solid(i, j)) continue;
        const h0 = hs[j], h1 = hs[j + 1];
        const face = (f, col) => {
          const bb = col === COL.ext ? bExt : b;      // exterior faces → concrete batch
          bb.strip = bb !== b ? null : f === F_C1 ? W.strips[1] : f === F_C0 ? W.strips[0] : null;   // long indoor faces → lightmap strip
          if (W.horiz) bb.faceOf(f, a0, a1, h0, h1, c0, c1, col); else bb.faceOf(f, c0, c1, h0, h1, a0, a1, col);
        };
        if (show1) face(F_C1, col1);
        if (show0) face(F_C0, col0);
        if (i === 0 ? !W.abut0 : !solid(i - 1, j)) face(F_LO, i === 0 ? colLo : colR);
        if (i === nA - 1 ? !W.abut1 : !solid(i + 1, j)) face(F_HI, i === nA - 1 ? colHi : colR);
        if (j === nH - 1) face(2, COL.cap);
        else if (!solid(i, j + 1)) face(2, colR);
        if (j > 0 && !solid(i, j - 1)) face(3, colR);
      }
    }
  }
}

/* ---------- windows & doors ---------- */
const DOOR_LEAVES = [];                    // footprints of the open door leaves (added to the furniture list)
for (const o of OPENINGS) {
  const [px0, py0, px1, py1] = o.r;
  const horiz = (px1 - px0) >= (py1 - py0);
  const a0 = horiz ? X(px0) : Z(py0), a1 = horiz ? X(px1) : Z(py1);
  const c0 = horiz ? Z(py0) : X(px0), c1 = horiz ? Z(py1) : X(px1);
  const R = (aa0, aa1, cc0, cc1, y0, y1, col, b = B.wall, cap = null) =>
    horiz ? cutRect(b, aa0, cc0, aa1, cc1, y0, y1, col, cap) : cutRect(b, cc0, aa0, cc1, aa1, y0, y1, col, cap);

  if (o.kind === 'window') {
    const fw = 0.05, fd = 0.04, cm = (c0 + c1) / 2;
    R(a0, a0 + fw, cm - fd, cm + fd, SILL, HEAD, COL.frame);
    R(a1 - fw, a1, cm - fd, cm + fd, SILL, HEAD, COL.frame);
    R(a0 + fw, a1 - fw, cm - fd, cm + fd, SILL, SILL + fw, COL.frame);
    R(a0 + fw, a1 - fw, cm - fd, cm + fd, HEAD - fw, HEAD, COL.frame);
    if (a1 - a0 > 0.9) { const m = (a0 + a1) / 2; R(m - 0.025, m + 0.025, cm - fd, cm + fd, SILL + fw, HEAD - fw, COL.frame); }
    R(a0 + fw, a1 - fw, cm - 0.006, cm + 0.006, SILL + fw, HEAD - fw, COL.glass, B.glass);
    R(a0 - 0.05, a1 + 0.05, c0 - 0.05, c1 + 0.05, SILL - 0.035, SILL + 0.005, COL.sill);
    continue;
  }
  const lw = o.main ? 0.11 : 0.06, ccol = o.main ? COL.mainFrame : COL.casing;
  R(a0 - lw, a0 + 0.02, c0 - 0.015, c1 + 0.015, 0, DOOR_H + 0.05, ccol, B.wall, COL.cap);
  R(a1 - 0.02, a1 + lw, c0 - 0.015, c1 + 0.015, 0, DOOR_H + 0.05, ccol, B.wall, COL.cap);
  R(a0 + 0.02, a1 - 0.02, c0 - 0.015, c1 + 0.015, DOOR_H - 0.02, DOOR_H + 0.05, ccol, B.wall, COL.cap);

  if (o.slide) {   // glass sliding door (slid open)
    const cm = (c0 + c1) / 2;
    R(a0 + 0.02, a0 + 0.05, cm - 0.03, cm + 0.03, 0.01, DOOR_H - 0.03, COL.frame);
    R(a0 + 0.05, a1 - 0.25, cm - 0.006, cm + 0.006, 0.05, DOOR_H - 0.06, COL.glass, B.glass);
    R(a0 + 0.02, a1 - 0.22, cm - 0.03, cm + 0.03, DOOR_H - 0.06, DOOR_H - 0.03, COL.frame);
    continue;
  }
  if (o.main) {   // closed teak double door
    const firstMain = CUT_ITEMS.length;
    // (main door: inlay lines and brass pulls)
    const zi = (c0 + c1) / 2 - 0.025, zo = zi + 0.05, mid = (a0 + a1) / 2;
    for (const [x0, x1] of [[a0 + 0.02, mid - 0.003], [mid + 0.003, a1 - 0.02]]) {
      R(x0, x1, zi, zo, 0.01, DOOR_H - 0.03, COL.mainDoor);
      for (let k = 1; k <= 3; k++) { const gx = x0 + ((x1 - x0) * k) / 4; R(gx - 0.007, gx + 0.007, zo, zo + 0.005, 0.24, DOOR_H - 0.24, COL.groove); }
      R(x0 + 0.06, x1 - 0.06, zo, zo + 0.005, 0.2, 0.214, COL.groove);
      R(x0 + 0.06, x1 - 0.06, zo, zo + 0.005, DOOR_H - 0.214, DOOR_H - 0.2, COL.groove);
    }
    for (const px of [mid - 0.075, mid + 0.075]) {
      R(px - 0.013, px + 0.013, zo + 0.028, zo + 0.05, 0.68, 1.58, COL.brass, B.wallMetal);
      for (const y of [0.74, 1.52]) R(px - 0.008, px + 0.008, zo, zo + 0.028, y - 0.008, y + 0.008, COL.brass, B.wallMetal);
    }
    for (let k = firstMain; k < CUT_ITEMS.length; k++) CUT_ITEMS[k].mainDoor = true;
    continue;
  }
  // interior door leaves, shown open as the plan's swing arcs indicate
  const clear = (a1 - a0) - 0.04, len = clear - 0.01, ang = (84 * Math.PI) / 180;
  const ha = o.hinge === 'a' ? a0 + 0.02 : a1 - 0.02, hc = o.swing < 0 ? c0 + 0.03 : c1 - 0.03;
  const shut = o.hinge === 'a' ? 1 : -1;
  const da = shut * Math.cos(ang), dc = o.swing * Math.sin(ang);
  const [hx, hz] = horiz ? [ha, hc] : [hc, ha];
  const [dx, dz] = horiz ? [da, dc] : [dc, da];
  const ry = Math.atan2(-dz, dx);
  cutRot(B.wall, hx + (dx * len) / 2, hz + (dz * len) / 2, len, 0.04, ry, 0.01, DOOR_H - 0.03, COL.door);
  DOOR_LEAVES.push([Math.min(hx, hx + dx * len) - 0.03, Math.min(hz, hz + dz * len) - 0.03, Math.max(hx, hx + dx * len) + 0.03, Math.max(hz, hz + dz * len) + 0.03]);
  const px = -dz, pz = dx, ex = hx + dx * (len - 0.09), ez = hz + dz * (len - 0.09);
  for (const sg of [-1, 1]) cutRot(B.wallMetal, ex + px * sg * 0.035, ez + pz * sg * 0.035, 0.13, 0.022, ry, 0.98, 1.005, COL.steel);
}

/* ---------- bathroom fittings on walls ---------- */
cutRect(B.wallGloss, X(362), Z(1494), X(398), Z(1494) + 0.02, 1.2, 1.85, COL.mirror);
cutRect(B.glass, X(335), Z(1557) - 0.006, X(404), Z(1557) + 0.006, 0.06, 2.0, COL.glass);
cutRect(B.glass, X(335) - 0.006, Z(1590), X(335) + 0.006, Z(1623), 0.06, 2.0, COL.glass);
cutRect(B.wallMetal, X(335) - 0.012, Z(1557) - 0.012, X(335) + 0.012, Z(1557) + 0.012, 0.06, 2.0, COL.steel);
cutRect(B.wallMetal, X(404) - 0.26, Z(1588) - 0.012, X(404), Z(1588) + 0.012, 1.97, 1.99, COL.steel);
cutRect(B.wallMetal, X(404) - 0.32, Z(1588) - 0.08, X(404) - 0.17, Z(1588) + 0.08, 1.94, 1.97, COL.steel);
cutRect(B.wallMetal, X(404) - 0.05, Z(1588) - 0.04, X(404), Z(1588) + 0.04, 1.0, 1.1, COL.steel);

/* =====================================================================
   FURNITURE (positions from the plan)
   ===================================================================== */
/* ---------- soft furnishings & textured pieces ---------- */
const FURN_FP = [...DOOR_LEAVES];          // furniture footprints [x0, z0, x1, z1] (walk-mode collision)
function footprint(cx, cz, ry, w, d) {     // axis-aligned bounds of a rotated w × d rectangle
  const c = Math.abs(Math.cos(ry)), s = Math.abs(Math.sin(ry)), hx = (w * c + d * s) / 2, hz = (w * s + d * c) / 2;
  FURN_FP.push([cx - hx, cz - hz, cx + hx, cz + hz]);
}
const FAB = fabricTex();
function fabric(hex, rough = 0.97) { return new THREE.MeshStandardMaterial({ color: C(hex), map: FAB, roughness: rough }); }
const MAT = {
  terracotta: fabric('#c0623f'), cream: fabric('#f1e9db'), teal: fabric('#3b8e89'), sage: fabric('#9aae8e'),
  ochre: fabric('#d9a24a'), leather: new THREE.MeshStandardMaterial({ color: C('#94603c'), roughness: 0.5 }),
  darkWood: new THREE.MeshStandardMaterial({ color: C('#2e2723'), roughness: 0.38 }),
  walnut: new THREE.MeshStandardMaterial({ color: C('#6b4a35'), roughness: 0.5 }),
  blackMetal: new THREE.MeshStandardMaterial({ color: C('#1b1c1e'), roughness: 0.4, metalness: 0.6 }),
  brass: new THREE.MeshStandardMaterial({ color: C('#c9a46a'), roughness: 0.28, metalness: 1 }),
  glow: new THREE.MeshStandardMaterial({ color: C('#fff4e2'), emissive: C('#ffd9a0'), emissiveIntensity: 2.2, roughness: 0.6 }),
};
const MARBLE = marbleTex(); MARBLE.repeat.set(1 / 1.6, 1 / 1.6);
MAT.marble = new THREE.MeshStandardMaterial({ map: MARBLE, roughness: 0.16 });
PHOTO_SWAP.set(MAT.glow, Object.assign(MAT.glow.clone(), { emissiveIntensity: 0.8 }));
const SLAT = slatTex(); SLAT.repeat.set(1 / 0.64, 1 / 0.64);
MAT.slat = new THREE.MeshStandardMaterial({ map: SLAT, roughness: 0.62 });
function uvByNormal(g, scale) {            // planar UVs (metres / scale) chosen by each vertex normal
  const p = g.attributes.position, n = g.attributes.normal, U = new Float32Array(p.count * 2);
  for (let i = 0; i < p.count; i++) worldUV(U, i * 2, p.getX(i) / scale, p.getY(i) / scale, p.getZ(i) / scale, n.getX(i), n.getY(i), n.getZ(i));
  g.setAttribute('uv', new THREE.BufferAttribute(U, 2));
  return g;
}
// parts: [lx, ly, lz, sx, sy, sz, radius, tiltX] in the piece's own frame (front = local +z)
function soft(parts, mat, cx, cz, ry, { uvScale = 0.3, parent = house } = {}) {
  const geos = parts.map(([lx, ly, lz, sx, sy, sz, rad, tilt = 0]) => {
    const g = new RoundedBoxGeometry(sx, sy, sz, 2, Math.min(rad, sx / 2 - 0.002, sy / 2 - 0.002, sz / 2 - 0.002));
    if (tilt) g.rotateX(tilt);
    return g.translate(lx, ly, lz);
  });
  const g = mergeGeometries(geos);
  g.rotateY(ry); g.translate(cx, 0, cz);
  uvByNormal(g, uvScale);
  const m = new THREE.Mesh(g, mat);
  m.castShadow = m.receiveShadow = true;
  parent.add(m);
  return m;
}
// textured box with world-continuous UVs (positioned at its centre so it can be cut)
function texBox(x0, z0, x1, z1, y0, y1, mat, parent = house) {
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2, cz = (z0 + z1) / 2;
  const g = new THREE.BoxGeometry(x1 - x0, y1 - y0, z1 - z0).translate(cx, cy, cz);
  uvByNormal(g, 1).translate(-cx, -cy, -cz);
  const m = new THREE.Mesh(g, mat);
  m.position.set(cx, cy, cz);
  m.castShadow = m.receiveShadow = true;
  parent.add(m);
  return m;
}
// modern 3-seat sofa: base, arms, back panel, loose seat & back cushions, throw pillows
function sofa3(cx, cz, ry, W, D, mat, { seats = 3, pillows = [] } = {}) {
  const arm = 0.2, f = frame(cx, cz, ry), P = [];
  P.push([0, 0.26, 0, W - 2 * arm + 0.04, 0.2, D - 0.06, 0.04]);
  for (const sx of [-1, 1]) P.push([sx * (W / 2 - arm / 2), 0.34, 0, arm, 0.52, D, 0.075]);
  P.push([0, 0.56, -D / 2 + 0.09, W - 2 * arm + 0.04, 0.6, 0.18, 0.06]);
  const sw = (W - 2 * arm) / seats;
  for (let i = 0; i < seats; i++) {
    const lx = -W / 2 + arm + sw * (i + 0.5);
    P.push([lx, 0.43, 0.06, sw - 0.012, 0.15, D - 0.24, 0.055]);
    P.push([lx, 0.71, -D / 2 + 0.27, sw - 0.02, 0.44, 0.18, 0.075, -0.18]);
  }
  soft(P, mat, cx, cz, ry);
  pillows.forEach((pm, i) => {
    const sx = i % 2 ? 1 : -1;
    soft([[sx * (W / 2 - arm - 0.3), 0.69, -D / 2 + 0.42, 0.44, 0.42, 0.13, 0.06, -0.3]], pm, cx, cz, ry);
  });
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) f(B.furnMetal, sx * (W / 2 - 0.08), sz * (D / 2 - 0.08), 0.04, 0.04, 0, 0.08, COL.black);
  footprint(cx, cz, ry, W, D);
}
// rounded lounge armchair on tapered walnut legs
function armchair(cx, cz, ry, mat) {
  const W = 0.82, D = 0.82, f = frame(cx, cz, ry);
  soft([
    [0, 0.25, 0, W - 0.02, 0.2, D - 0.02, 0.06],
    [0, 0.6, -D / 2 + 0.1, W, 0.56, 0.2, 0.09],
    [-(W / 2 - 0.09), 0.48, 0.02, 0.18, 0.3, D - 0.04, 0.08], [W / 2 - 0.09, 0.48, 0.02, 0.18, 0.3, D - 0.04, 0.08],
    [0, 0.42, 0.06, W - 0.36, 0.15, D - 0.3, 0.055],
  ], mat, cx, cz, ry);
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) f(B.furn, sx * (W / 2 - 0.08), sz * (D / 2 - 0.08), 0.035, 0.035, 0, 0.16, COL.walnut);
  footprint(cx, cz, ry, W, D);
}
// upholstered dining chair with walnut legs
function diningChair(cx, cz, ry, mat) {
  const f = frame(cx, cz, ry);
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) f(B.furn, sx * 0.19, sz * 0.18, 0.032, 0.032, 0, 0.44, COL.walnut);
  soft([[0, 0.48, 0.01, 0.47, 0.085, 0.46, 0.035], [0, 0.79, -0.2, 0.45, 0.42, 0.07, 0.03, -0.1]], mat, cx, cz, ry);
}
// counter stool: upholstered seat, black steel legs and foot ring
function stool(x, z) {
  cyl(x, z, 0.19, 0.18, 0.63, 0.69, null, { mat: MAT.cream, seg: 28 });
  for (let k = 0; k < 4; k++) { const a = Math.PI / 4 + (k * Math.PI) / 2; fbox(B.furnMetal, x + Math.cos(a) * 0.13 - 0.01, z + Math.sin(a) * 0.13 - 0.01, x + Math.cos(a) * 0.13 + 0.01, z + Math.sin(a) * 0.13 + 0.01, 0, 0.63, COL.black); }
  const ring = new THREE.Mesh(new THREE.TorusGeometry(0.14, 0.008, 6, 32), MAT.blackMetal);
  ring.rotation.x = Math.PI / 2; ring.position.set(x, 0.27, z); ring.castShadow = true; house.add(ring);
  footprint(x, z, 0, 0.4, 0.4);
}

/* ---------- decor pieces (styled after the reference interior) ---------- */
MAT.bone = fabric('#f7f2e8');
MAT.oakPanel = (() => {                    // light oak veneer, grain running vertically (512 px ≙ 1 m)
  const [c, g] = canvas(512, 512), r = rng(45);
  g.fillStyle = 'rgb(204,183,153)'; g.fillRect(0, 0, 512, 512);
  for (let i = 0; i < 300; i++) { g.fillStyle = r() < 0.5 ? `rgba(122,92,58,${0.03 + r() * 0.08})` : `rgba(255,245,226,${0.03 + r() * 0.07})`; g.fillRect(r() * 512, 0, 0.5 + r() * 2.4, 512); }
  addNoise(g, 512, 512, 0.02, 46);
  return new THREE.MeshStandardMaterial({ map: toTex(c), roughness: 0.55 });
})();
MAT.white = new THREE.MeshStandardMaterial({ color: C('#f1eee8'), roughness: 0.55, side: THREE.DoubleSide });
MAT.black = new THREE.MeshStandardMaterial({ color: C('#1c1c1e'), roughness: 0.55, side: THREE.DoubleSide });
MAT.stoneware = new THREE.MeshStandardMaterial({ color: C('#3a3531'), roughness: 0.8, side: THREE.DoubleSide });
MAT.shade = new THREE.MeshStandardMaterial({ color: C('#f6ecda'), emissive: C('#ffdcae'), emissiveIntensity: 1.25, roughness: 0.9 });
MAT.tube = new THREE.MeshStandardMaterial({ color: C('#ffffff'), emissive: C('#fff3e0'), emissiveIntensity: 3.2, roughness: 0.5 });
MAT.domeIn = new THREE.MeshStandardMaterial({ color: C('#fff6e4'), emissive: C('#ffe4bd'), emissiveIntensity: 1.6, roughness: 0.7, side: THREE.BackSide });
MAT.leaf = new THREE.MeshStandardMaterial({ vertexColors: true, roughness: 0.5, side: THREE.DoubleSide });
MAT.leaf.userData.noBake = true;           // foliage is left out of the baked shadows (too fine for the lightmap)
for (const k of ['tube', 'shade', 'domeIn']) PHOTO_SWAP.set(MAT[k], Object.assign(MAT[k].clone(), { emissiveIntensity: 0.7 }));   // calmer emitters for Photo mode
const FLUTE = (() => {                      // vertical flutes (shaded stripes) for round side tables
  const [c, g] = canvas(64, 8), grd = g.createLinearGradient(0, 0, 64, 0);
  grd.addColorStop(0, '#8d8679'); grd.addColorStop(0.18, '#d8d1c3'); grd.addColorStop(0.5, '#f3ede1'); grd.addColorStop(0.82, '#d8d1c3'); grd.addColorStop(1, '#8d8679');
  g.fillStyle = grd; g.fillRect(0, 0, 64, 8);
  const t = toTex(c); t.repeat.set(30, 1); return t;
})();
MAT.flute = new THREE.MeshStandardMaterial({ map: FLUTE, roughness: 0.75 });
// a turned shape (vase, bowl, side table …): profile = [radius, height] pairs from the bottom up
function lathe(profile, mat, x, y, z, seg = 28) {
  const m = new THREE.Mesh(new THREE.LatheGeometry(profile.map(([r, h]) => new THREE.Vector2(Math.max(r, 0.0005), h)), seg), mat);
  m.position.set(x, y, z); m.castShadow = m.receiveShadow = true; house.add(m);
  return m;
}
const vase = (x, y, z, mat, s = 1) => lathe([[0.001, 0], [0.05 * s, 0], [0.085 * s, 0.07 * s], [0.09 * s, 0.14 * s], [0.06 * s, 0.22 * s], [0.035 * s, 0.26 * s], [0.045 * s, 0.29 * s]], mat, x, y, z);
const bowl = (x, y, z, mat, r = 0.14) => lathe([[0.001, 0.004], [r * 0.45, 0], [r * 0.85, r * 0.2], [r, r * 0.42], [r * 0.96, r * 0.42], [r * 0.8, r * 0.2], [r * 0.4, r * 0.06], [0.001, r * 0.06]], mat, x, y, z);
// table lamp: stacked sculptural base, drum shade that glows, and the light it gives to the room
function tableLamp(x, y, z) {
  soft([[0, y + 0.05, 0, 0.17, 0.1, 0.14, 0.03], [0.02, y + 0.15, 0.01, 0.12, 0.1, 0.1, 0.03], [-0.015, y + 0.25, -0.01, 0.15, 0.1, 0.12, 0.03]], MAT.white, x, z, 0.4);
  cyl(x, z, 0.008, 0.008, y + 0.3, y + 0.4, null, { mat: MAT.brass, seg: 8 });
  const sh = cyl(x, z, 0.165, 0.185, y + 0.39, y + 0.64, null, { mat: MAT.shade, seg: 36 });
  sh.castShadow = false;
  cutMesh(sh, y + 0.39, y + 0.64, 'hide');
  glow(x, y + 0.52, z, true);
  giLamp(x, y + 0.52, z, 0.4, null, '#ffdcae', 0);
}
// low black hourglass side table
function hourglassTable(x, z, s = 1) {
  lathe([[0.001, 0], [0.19 * s, 0], [0.2 * s, 0.03 * s], [0.1 * s, 0.2 * s], [0.085 * s, 0.26 * s], [0.12 * s, 0.38 * s], [0.21 * s, 0.44 * s], [0.21 * s, 0.47 * s], [0.001, 0.47 * s]], MAT.black, x, 0, z, 36);
  footprint(x, z, 0, 0.42 * s, 0.42 * s);
}
// round fluted side table
function flutedTable(x, z, r = 0.2, h = 0.5) {
  cyl(x, z, r, r, 0.02, h - 0.03, null, { mat: MAT.flute, seg: 40 });
  cyl(x, z, r + 0.015, r + 0.015, h - 0.03, h, null, { mat: MAT.white, seg: 40 });
  footprint(x, z, 0, 2 * r + 0.03, 2 * r + 0.03);
}
// a vase with bare flowering branches
function branchVase(x, y, z, mat = MAT.stoneware) {
  vase(x, y, z, mat, 1.1);
  const r = rng(Math.round(x * 97 + z * 31) + 500), geos = [], buds = [];
  for (let i = 0; i < 7; i++) {
    const a = r() * 6.28, tilt = 0.12 + r() * 0.3, len = 0.42 + r() * 0.32;
    const g = new THREE.CylinderGeometry(0.0035, 0.006, len, 5).translate(0, len / 2, 0);
    g.rotateZ(tilt).rotateY(a).translate(x, y + 0.27, z); geos.push(g);
    for (let k = 0; k < 5; k++) {
      const t = 0.45 + r() * 0.55, b = new THREE.SphereGeometry(0.012 + r() * 0.008, 6, 5);
      b.translate(0.012, len * t, 0).rotateZ(tilt).rotateY(a).translate(x, y + 0.27, z); buds.push(b);
    }
  }
  const tw = new THREE.Mesh(mergeGeometries(geos), MAT.walnut), fl = new THREE.Mesh(mergeGeometries(buds), MAT.white);
  tw.castShadow = fl.castShadow = true; house.add(tw, fl);
}
// white ring sculpture on a little stand
function ringSculpture(x, y, z, ry = 0.5) {
  const g = new THREE.TorusGeometry(0.085, 0.028, 12, 28).rotateY(ry).translate(x, y + 0.14, z);
  const m = new THREE.Mesh(g, MAT.white); m.castShadow = true; house.add(m);
  cyl(x, z, 0.03, 0.035, y, y + 0.03, null, { mat: MAT.black, seg: 16 });
}
// shallow bowl of succulents
function succulents(x, y, z, r = 0.15) {
  bowl(x, y, z, MAT.stoneware, r);
  const rr = rng(Math.round(x * 53 + z * 17) + 900), geos = [];
  for (let i = 0; i < 9; i++) {
    const a = rr() * 6.28, d = rr() * r * 0.6, h = 0.05 + rr() * 0.05;
    const g = new THREE.ConeGeometry(0.035 + rr() * 0.02, h, 6).translate(Math.cos(a) * d, r * 0.3 + h / 2, Math.sin(a) * d);
    const n = g.attributes.position.count, col = new Float32Array(n * 3), k = 0.7 + rr() * 0.6;
    for (let v = 0; v < n; v++) { col[v * 3] = 0.07 * k; col[v * 3 + 1] = 0.2 * k; col[v * 3 + 2] = 0.1 * k; }
    g.setAttribute('color', new THREE.BufferAttribute(col, 3)); geos.push(g.translate(x, y, z));
  }
  const m = new THREE.Mesh(mergeGeometries(geos), MAT.leaf); m.castShadow = true; house.add(m);
}
// a stack of books
function books(x, y, z, ry = 0, cols = ['#d9d1c2', '#394f5c', '#b7a48a']) {
  const f = frame(x, z, ry);
  cols.forEach((c, i) => f(B.furn, (i % 2) * 0.012, 0, 0.26 - i * 0.02, 0.19 - i * 0.015, y + i * 0.03, y + i * 0.03 + 0.03, C(c)));
}
// place setting: slate placemat, plate, bowl and a folded napkin
function placeSetting(x, y, z) {
  cyl(x, z, 0.19, 0.19, y, y + 0.006, null, { mat: MAT.stoneware, seg: 36 });
  cyl(x, z, 0.135, 0.11, y + 0.006, y + 0.02, null, { mat: MAT.white, seg: 36 });
  bowl(x, y + 0.02, z, MAT.black, 0.075);
  fbox(B.furn, x - 0.2, z - 0.05, x - 0.1, z + 0.09, y + 0.006, y + 0.018, C('#c0623f'));
}
// bowl of lemons / vase of flowers
function fruitBowl(x, y, z) {
  bowl(x, y, z, MAT.white, 0.15);
  const mat = new THREE.MeshStandardMaterial({ color: C('#e8c53a'), roughness: 0.6 }), geos = [];
  for (const [dx, dy, dz] of [[0, 0.05, 0], [0.06, 0.055, 0.03], [-0.05, 0.055, 0.04], [0.01, 0.055, -0.06], [0.02, 0.1, 0.01]]) geos.push(new THREE.SphereGeometry(0.036, 12, 8).scale(1.2, 1, 1).translate(x + dx, y + dy, z + dz));
  const m = new THREE.Mesh(mergeGeometries(geos), mat); m.castShadow = true; house.add(m);
}
function flowers(x, y, z) {
  vase(x, y, z, MAT.white, 0.85);
  const r = rng(77), stems = [], heads = [];
  for (let i = 0; i < 8; i++) {
    const a = r() * 6.28, tilt = 0.1 + r() * 0.35, len = 0.2 + r() * 0.12;
    stems.push(new THREE.CylinderGeometry(0.003, 0.003, len, 4).translate(0, len / 2, 0).rotateZ(tilt).rotateY(a).translate(x, y + 0.2, z));
    heads.push(new THREE.SphereGeometry(0.03 + r() * 0.012, 8, 6).translate(0, len, 0).rotateZ(tilt).rotateY(a).translate(x, y + 0.2, z));
  }
  const st = new THREE.Mesh(mergeGeometries(stems), new THREE.MeshStandardMaterial({ color: C('#3f6b34'), roughness: 0.7 }));
  const hd = new THREE.Mesh(mergeGeometries(heads), new THREE.MeshStandardMaterial({ color: C('#e9806b'), roughness: 0.7 }));
  hd.castShadow = true; house.add(st, hd);
}
// one leaf blade: starts at the origin, grows along +y, arches backwards (−z) and folds a little along its midrib
function leafBlade(len, wid, arch, tint) {
  const nu = 4, nv = 8, pos = [], col = [], idx = [];
  for (let j = 0; j <= nv; j++) {
    const t = j / nv, w = wid * Math.pow(Math.sin(Math.PI * (0.04 + 0.96 * t)), 0.75) * (1.08 - 0.3 * t);
    for (let i = 0; i <= nu; i++) {
      const u = i / nu - 0.5, k = tint * (0.82 + 0.36 * (1 - Math.abs(u) * 2) * (1 - Math.abs(u) * 2));
      pos.push(u * w, t * len, -arch * t * t * len + Math.abs(u) * w * 0.22);
      col.push(0.05 * k, 0.2 * k, 0.045 * k);
    }
  }
  for (let j = 0; j < nv; j++) for (let i = 0; i < nu; i++) { const a = j * (nu + 1) + i, b = a + 1, c = a + nu + 1, d = c + 1; idx.push(a, b, d, a, d, c); }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3)); g.setAttribute('color', new THREE.Float32BufferAttribute(col, 3)); g.setIndex(idx);
  g.computeVertexNormals();
  return g;
}
function potted(x, z, r, h, y = 0) {
  cyl(x, z, r, r * 0.86, y, y + h, null, { mat: MAT.black, seg: 28 });
  cyl(x, z, r * 0.92, r * 0.92, y + h - 0.02, y + h - 0.012, C('#2c2219'), { rough: 1, seg: 24 });
  footprint(x, z, 0, 2 * r + 0.1, 2 * r + 0.1);
}
// bird of paradise: big paddle leaves on long stems, in a black pot (reach = how far the leaves may lean out)
function paradisePlant(x, z, s = 1, reach = 0.6, y = 0) {
  const ph = 0.4 * s, r = rng(Math.round(x * 131 + z * 71) + 3000), geos = [];
  potted(x, z, 0.21 * s, ph, y);
  for (let i = 0; i < 9; i++) {
    const stem = (0.55 + r() * 0.5) * s, len = (0.5 + r() * 0.2) * s, a = i * 2.4 + r() * 0.5;
    const lean = Math.min(0.1 + (i / 9) * 0.5 + r() * 0.12, Math.asin(Math.min(1, reach / (stem + len))));
    const st = new THREE.CylinderGeometry(0.009 * s, 0.014 * s, stem, 5).translate(0, stem / 2, 0).toNonIndexed();
    const n = st.attributes.position.count, c = new Float32Array(n * 3);
    for (let v = 0; v < n; v++) { c[v * 3] = 0.06; c[v * 3 + 1] = 0.2; c[v * 3 + 2] = 0.05; }
    st.setAttribute('color', new THREE.BufferAttribute(c, 3)); st.deleteAttribute('uv');
    const bl = leafBlade(len, 0.26 * s, 0.2 + r() * 0.2, 0.8 + r() * 0.5).translate(0, stem, 0).toNonIndexed();
    for (const g of [st, bl]) { g.rotateX(-lean).rotateY(a).translate(x, y + ph - 0.03, z); geos.push(g); }
  }
  const m = new THREE.Mesh(mergeGeometries(geos), MAT.leaf); m.castShadow = m.receiveShadow = true; house.add(m);
}
// fiddle-leaf fig: slim trunk with large rounded leaves towards the top
function figTree(x, z, s = 1) {
  const ph = 0.36 * s, r = rng(Math.round(x * 113 + z * 59) + 4000), geos = [], H = 1.25 * s;
  potted(x, z, 0.2 * s, ph);
  cyl(x, z, 0.012 * s, 0.02 * s, ph - 0.03, ph + H, null, { mat: MAT.walnut, seg: 8 });
  for (let i = 0; i < 22; i++) {
    const t = 0.35 + (i / 22) * 0.68, a = i * 2.39996 + r() * 0.4, lean = 0.7 + r() * 0.5, len = (0.2 + r() * 0.1) * s;
    geos.push(leafBlade(len, 0.2 * s, 0.3 + r() * 0.3, 0.75 + r() * 0.5).toNonIndexed().translate(0, 0.03 * s, 0).rotateX(-lean).rotateY(a).translate(x, ph + H * t, z));
  }
  const m = new THREE.Mesh(mergeGeometries(geos), MAT.leaf); m.castShadow = m.receiveShadow = true; house.add(m);
}
// sunburst wall decor: thin black rods radiating from a small disc (on a wall facing direction ry)
function sunburst(x, y, z, ry, R = 0.46) {
  const geos = [new THREE.CylinderGeometry(0.05, 0.05, 0.02, 20).rotateX(Math.PI / 2)];
  for (let i = 0; i < 40; i++) {
    const a = (i / 40) * Math.PI * 2, len = R * (i % 2 ? 0.62 : 1) - 0.05;
    geos.push(new THREE.CylinderGeometry(0.004, 0.006, len, 4).translate(0, 0.05 + len / 2, 0).rotateZ(a));
  }
  const m = new THREE.Mesh(mergeGeometries(geos), MAT.blackMetal);
  m.position.set(x, y, z); m.rotation.y = ry; m.castShadow = true; house.add(m);
  cutMesh(m, y - R, y + R, 'hide');
}

function cabinet(px0, py0, px1, py1, front, { h, doors = 2, body = COL.cabinet, door = COL.cabinetDoor, top = null, kick = 0.08, handles = 'top', batch = B.furn } = {}) {
  const x0 = X(px0), x1 = X(px1), z0 = Z(py0), z1 = Z(py1);
  FURN_FP.push([x0, z0, x1, z1]);
  const bodyTop = top ? h - 0.04 : h;
  const ns = front === 'n' || front === 's';
  const face = front === 'n' ? z0 : front === 's' ? z1 : front === 'e' ? x1 : x0;
  const out = front === 'n' || front === 'w' ? -1 : 1;
  const A0 = ns ? x0 : z0, A1 = ns ? x1 : z1;
  const FB = (b, a0, a1, d0, d1, y0, y1, col) => {
    const f0 = face + out * d0, f1 = face + out * d1;
    if (ns) fbox(b, a0, Math.min(f0, f1), a1, Math.max(f0, f1), y0, y1, col);
    else fbox(b, Math.min(f0, f1), a0, Math.max(f0, f1), a1, y0, y1, col);
  };
  if (kick > 0) {
    if (ns) fbox(batch, x0, front === 'n' ? z0 + 0.05 : z0, x1, front === 's' ? z1 - 0.05 : z1, 0, kick, COL.kick);
    else fbox(batch, front === 'w' ? x0 + 0.05 : x0, z0, front === 'e' ? x1 - 0.05 : x1, z1, 0, kick, COL.kick);
  }
  fbox(batch, x0, z0, x1, z1, kick, bodyTop, body);
  if (top) {
    if (ns) fbox(B.furnGloss, x0, front === 'n' ? z0 - 0.02 : z0, x1, front === 's' ? z1 + 0.02 : z1, bodyTop, h, top);
    else fbox(B.furnGloss, front === 'w' ? x0 - 0.02 : x0, z0, front === 'e' ? x1 + 0.02 : x1, z1, bodyTop, h, top);
  }
  const pw = (A1 - A0) / doors;
  for (let i = 0; i < doors; i++) {
    const a0 = A0 + i * pw + 0.004, a1 = A0 + (i + 1) * pw - 0.004;
    FB(batch, a0, a1, 0, 0.018, kick + 0.015, bodyTop - 0.015, door);
    if (handles) {
      const hy = handles === 'top' ? bodyTop - 0.09 : (kick + bodyTop) / 2;
      const ha = i % 2 === 0 ? a1 - 0.05 : a0 + 0.05;
      const hh = handles === 'top' ? 0.012 : 0.16;
      FB(B.furnMetal, ha - (handles === 'top' ? 0.07 : 0.008), ha + (handles === 'top' ? 0.07 : 0.008), 0.018, 0.035, hy - hh / 2, hy + hh / 2, COL.steel);
    }
  }
}
function chair(cx, cz, ry, b = B.furn, y = 0) {
  const f = frame(cx, cz, ry), W = 0.44, D = 0.43;
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) f(b, sx * (W / 2 - 0.03), sz * (D / 2 - 0.03), 0.035, 0.035, y, y + 0.43, COL.walnut);
  f(b, 0, 0, W, D, y + 0.43, y + 0.47, COL.walnut);
  f(b, 0, 0.01, W - 0.04, D - 0.06, y + 0.47, y + 0.5, COL.cushion);
  f(b, 0, -D / 2 + 0.018, W, 0.035, y + 0.47, y + 0.92, COL.walnut);
  f(b, 0, -D / 2 + 0.05, W - 0.07, 0.03, y + 0.56, y + 0.86, COL.cushion);
}
function sofa(cx, cz, ry, W, D, seats, col, seatCol, { pillows = true, b = B.furn, y = 0, pillow = COL.pillow } = {}) {
  const f = frame(cx, cz, ry), arm = 0.16, back = 0.2;
  for (const sx of [-1, 1]) for (const sz of [-1, 1]) f(b, sx * (W / 2 - 0.07), sz * (D / 2 - 0.07), 0.05, 0.05, y, y + 0.1, COL.walnut);
  f(b, 0, 0, W, D, y + 0.1, y + 0.4, col);
  f(b, 0, -D / 2 + back / 2, W, back, y + 0.4, y + 0.84, col);
  for (const sx of [-1, 1]) f(b, sx * (W / 2 - arm / 2), 0, arm, D, y + 0.4, y + 0.63, col);
  const sw = (W - 2 * arm) / seats;
  for (let i = 0; i < seats; i++) {
    const lx = -W / 2 + arm + sw * (i + 0.5);
    f(b, lx, back / 2 + 0.01, sw - 0.02, D - back - 0.03, y + 0.4, y + 0.5, seatCol);
    f(b, lx, -D / 2 + back + 0.065, sw - 0.04, 0.13, y + 0.5, y + 0.8, seatCol);
  }
  if (pillows) {
    f(b, -W / 2 + arm + 0.21, -D / 2 + back + 0.19, 0.34, 0.11, y + 0.5, y + 0.78, pillow);
    f(b, W / 2 - arm - 0.21, -D / 2 + back + 0.19, 0.34, 0.11, y + 0.5, y + 0.78, pillow);
  }
}

// Kitchen — oak lower cabinets, marble worktops & splashback, upper cabinets, island with stools
const OAK_DOOR = C('#c9b08e');
cabinet(21, 998, 63, 1178, 'e', { h: 0.86, doors: 4, door: OAK_DOOR });
texBox(X(21), Z(998), X(63) + 0.025, Z(1178), 0.86, 0.9, MAT.marble);
pbox(B.furnMetal, 25, 1006, 57, 1030, 0.89, 0.905, COL.steel);
pbox(B.furnMetal, 25, 1034, 57, 1058, 0.89, 0.905, COL.steel);
fbox(B.furnMetal, X(22), Z(1032) - 0.015, X(22) + 0.03, Z(1032) + 0.015, 0.9, 1.18, COL.steel);
fbox(B.furnMetal, X(22), Z(1032) - 0.012, X(22) + 0.2, Z(1032) + 0.012, 1.15, 1.18, COL.steel);
cabinet(21, 1238, 278, 1273, 'n', { h: 0.86, doors: 4, door: OAK_DOOR });
texBox(X(21), Z(1238) - 0.025, X(278), Z(1273), 0.86, 0.9, MAT.marble);
pbox(B.furnGloss, 158, 1243, 206, 1268, 0.9, 0.908, COL.hob);
for (const [bx, by] of [[167, 1250], [197, 1250], [167, 1261], [197, 1261]]) pbox(B.furnMetal, bx - 5, by - 3, bx + 5, by + 3, 0.908, 0.914, COL.burner);
pbox(B.furnGloss, 22, 1184, 79, 1237, 0, 1.86, COL.fridge);
FURN_FP.push([X(22), Z(1184), X(79), Z(1237)]);
pbox(B.furn, 79, 1185, 80, 1236, 1.22, 1.24, COL.kick);
for (const [y0, y1] of [[0.55, 1.12], [1.32, 1.75]]) fbox(B.furnMetal, X(80), Z(1228) - 0.012, X(80) + 0.03, Z(1228) + 0.012, y0, y1, COL.steel);
// marble splashback (cut with the walls)
cutMesh(texBox(X(21), Z(998), X(21) + 0.014, Z(1178), 0.9, 1.52, MAT.marble), 0.9, 1.52);
cutMesh(texBox(X(21), Z(1273) - 0.014, X(278), Z(1273), 0.9, 1.52, MAT.marble), 0.9, 1.52);
// upper cabinets & range hood (hidden in the cut-away view)
{
  const up = B.upper, x0 = X(21), d = 0.34;
  fbox(up, x0, Z(998), x0 + d, Z(1178), 1.55, 2.3, COL.cabinet);
  for (let i = 0; i < 4; i++) { const za = Z(998) + (i * (Z(1178) - Z(998))) / 4; fbox(up, x0 + d, za + 0.004, x0 + d + 0.018, za + (Z(1178) - Z(998)) / 4 - 0.004, 1.565, 2.285, COL.cabinetDoor); }
  fbox(up, X(22), Z(1184), X(79), Z(1237), 1.93, 2.3, COL.cabinet);
  fbox(up, X(79), Z(1184) + 0.004, X(79) + 0.018, Z(1237) - 0.004, 1.945, 2.285, COL.cabinetDoor);
  const z1 = Z(1273);
  for (const [a, b] of [[X(21), X(150)], [X(214), X(278)]]) {
    fbox(up, a, z1 - d, b, z1, 1.55, 2.3, COL.cabinet);
    const n = Math.max(1, Math.round((b - a) / 0.45));
    for (let i = 0; i < n; i++) { const xa = a + (i * (b - a)) / n; fbox(up, xa + 0.004, z1 - d - 0.018, xa + (b - a) / n - 0.004, z1 - d, 1.565, 2.285, COL.cabinetDoor); }
  }
  // warm LED strip under the upper cabinets
  lampBox(house, MAT.glow, x0 + 0.02, x0 + 0.05, 1.535, 1.548, Z(998) + 0.05, Z(1178) - 0.05);
  for (let k = 0; k < 3; k++) giLamp(x0 + 0.2, 1.5, Z(998) + 0.35 + k * ((Z(1178) - Z(998) - 0.7) / 2), 0.09, [0, 0.6], '#ffe2bd', 0);
  const hood = B.upperMetal, hx = X(182);
  fbox(hood, hx - 0.36, z1 - 0.5, hx + 0.36, z1, 1.62, 1.72, COL.steel);
  fbox(hood, hx - 0.14, z1 - 0.28, hx + 0.14, z1, 1.72, 2.8, COL.steel);
}
// island: light oak fronts between marble "waterfall" ends, marble top with a breakfast overhang, three stools
{
  const x0 = -4.7, x1 = -2.8, z0 = -2.45, z1 = -1.65;
  fbox(B.furn, x0 + 0.06, z0 + 0.06, x1 - 0.06, z1 - 0.06, 0, 0.08, COL.kick);
  texBox(x0 + 0.04, z0, x1 - 0.04, z1, 0.08, 0.88, MAT.oakPanel);
  for (let k = 1; k < 4; k++) { const x = x0 + 0.04 + (k * (x1 - x0 - 0.08)) / 4; fbox(B.furn, x - 0.003, z0 - 0.002, x + 0.003, z1 + 0.002, 0.1, 0.86, COL.kick); }   // shadow gaps between the doors
  texBox(x0 - 0.02, z0 - 0.03, x0 + 0.04, z1 + 0.2, 0, 0.92, MAT.marble);          // waterfall ends
  texBox(x1 - 0.04, z0 - 0.03, x1 + 0.02, z1 + 0.2, 0, 0.92, MAT.marble);
  texBox(x0 + 0.04, z0 - 0.03, x1 - 0.04, z1 + 0.2, 0.88, 0.92, MAT.marble);       // top
  FURN_FP.push([x0 - 0.02, z0 - 0.03, x1 + 0.02, z1 + 0.2]);
  for (const x of [-4.5, -3.95, -3.4]) { stool(x, -1.2); placeSetting(x, 0.92, -1.72); }
  fruitBowl(-3.15, 0.92, -2.2); flowers(-4.4, 0.92, -2.2);
}
// Living & dining — oak table on walnut legs, upholstered chairs
{
  const x0 = X(508) - 0.3, x1 = X(657) - 0.3, z0 = Z(1088), z1 = Z(1181), zm = (z0 + z1) / 2;
  fbox(B.furn, x0, z0, x1, z1, 0.71, 0.76, COL.oak);
  for (const x of [x0 + 0.2, x1 - 0.2]) {
    fbox(B.furn, x - 0.035, z0 + 0.14, x + 0.035, z1 - 0.14, 0.62, 0.71, COL.walnut);
    for (const z of [z0 + 0.2, z1 - 0.2]) fbox(B.furn, x - 0.035, z - 0.035, x + 0.035, z + 0.035, 0, 0.62, COL.walnut);
  }
  for (const x of [x0 + 0.46, x1 - 0.46]) { diningChair(x, z0 - 0.17, FACE.s, MAT.cream); diningChair(x, z1 + 0.17, FACE.n, MAT.cream); }
  diningChair(x0 - 0.17, zm, FACE.e, MAT.cream); diningChair(x1 + 0.17, zm, FACE.w, MAT.cream);
  FURN_FP.push([x0 - 0.42, z0 - 0.42, x1 + 0.42, z1 + 0.42]);
  bowl((x0 + x1) / 2 - 0.28, 0.76, zm, MAT.stoneware, 0.17); branchVase((x0 + x1) / 2 + 0.3, 0.76, zm + 0.04, MAT.white);
}
// lounge — terracotta sofa with a console table and two lamps behind it, cream armchairs around a dark coffee table
{
  const sx = 3.35, sz = -1.7;
  sofa3(sx, sz, FACE.e, 2.4, 1.0, MAT.terracotta, { seats: 2 });
  // loose pillows and a throw (sofa frame: local x runs along the seat, local z points to the front)
  const onSofa = (lx, mat, w = 0.46, tilt = -0.32, lz = -0.1) => soft([[lx, 0.7, lz, w, w * 0.95, 0.15, 0.07, tilt]], mat, sx, sz, FACE.e);
  onSofa(-0.82, MAT.cream); onSofa(-0.45, MAT.bone, 0.4, -0.4, 0.0); onSofa(0.28, MAT.bone, 0.42); onSofa(0.8, MAT.cream, 0.48); onSofa(0.52, MAT.teal, 0.36, -0.44, 0.05);
  soft([[-1.1, 0.62, 0.12, 0.26, 0.035, 0.62, 0.015], [-1.215, 0.42, 0.12, 0.035, 0.4, 0.6, 0.015]], MAT.bone, sx, sz, FACE.e);     // throw over one arm
  // console table behind the sofa
  const cx0 = 2.46, cx1 = 2.8, cz0 = -2.75, cz1 = -0.65, stone = C('#cfc8ba');
  fbox(B.furn, cx0, cz0, cx1, cz1, 0.72, 0.76, stone);
  fbox(B.furn, cx0, cz0, cx1, cz0 + 0.04, 0, 0.72, stone); fbox(B.furn, cx0, cz1 - 0.04, cx1, cz1, 0, 0.72, stone);
  fbox(B.furn, cx0 + 0.02, cz0 + 0.04, cx1 - 0.02, cz1 - 0.04, 0.2, 0.23, stone);
  FURN_FP.push([cx0, cz0, cx1, cz1]);
  tableLamp(2.63, 0.76, -2.4); tableLamp(2.63, 0.76, -1.0);
  ringSculpture(2.63, 0.76, -1.72); books(2.63, 0.76, -2.02, 0.2); succulents(2.63, 0.76, -1.38, 0.11);
  books(2.63, 0.23, -1.2, 0, ['#e6dfd2', '#7a8c8c', '#c9b79c']); vase(2.63, 0.23, -2.2, MAT.black, 1.0);
  // armchairs with teal pillows, coffee table, side tables
  for (const [az, ar] of [[-2.4, 0.15], [-1.0, -0.15]]) { armchair(5.6, az, FACE.w + ar, MAT.cream); soft([[0, 0.62, -0.12, 0.36, 0.34, 0.13, 0.06, -0.3]], MAT.teal, 5.6, az, FACE.w + ar); }
  soft([[0, 0.3, 0, 1.3, 0.09, 0.74, 0.04], [-0.42, 0.13, 0, 0.18, 0.26, 0.52, 0.06], [0.42, 0.13, 0, 0.18, 0.26, 0.52, 0.06]], MAT.darkWood, 4.55, sz, FACE.e, { uvScale: 1 });
  footprint(4.55, sz, FACE.e, 1.3, 0.74);
  books(4.55, 0.345, sz - 0.3, 0.3); vase(4.5, 0.345, sz + 0.34, MAT.black, 0.9); succulents(4.62, 0.345, sz + 0.02, 0.13);
  flutedTable(5.9, sz, 0.19, 0.5); branchVase(5.9, 0.5, sz);
  hourglassTable(5.85, -0.2);
}
cabinet(866, 1300, 1010, 1333, 'n', { h: 0.62, doors: 3, body: COL.walnut, door: C('#7a573f'), handles: 'mid', kick: 0.06 });
vase(X(892), 0.62, Z(1316), MAT.black, 1.2); books(X(955), 0.62, Z(1316), 0.15); vase(X(992), 0.62, Z(1318), MAT.white, 0.8);
sunburst((X(866) + X(1010)) / 2, 1.72, Z(1333) - 0.015, 0);
// TV room — walnut slat feature wall, floating console, cream sofa
{
  const x = X(712), za = 1.72, zb = 4.15, H = WALL_H - 0.2;
  const backer = new THREE.Mesh(new THREE.BoxGeometry(0.02, H, zb - za), new THREE.MeshStandardMaterial({ color: C('#2b2a29'), roughness: 0.9 }));
  backer.position.set(x + 0.01, H / 2, (za + zb) / 2); backer.receiveShadow = true; house.add(backer);
  const geos = [];
  for (let z = za + 0.02; z + 0.045 <= zb - 0.01; z += 0.068) geos.push(new THREE.BoxGeometry(0.03, H, 0.045).translate(x + 0.035, 0, z + 0.0225));
  const slats = new THREE.Mesh(mergeGeometries(geos), new THREE.MeshStandardMaterial({ color: C('#7d5638'), roughness: 0.55 }));
  slats.position.y = H / 2; slats.castShadow = slats.receiveShadow = true; house.add(slats);
  cutMesh(backer, 0, H); cutMesh(slats, 0, H);
}
{ // hearth: a low dark plinth with a linear fireplace, the TV on the wall above
  const x = X(712), za = 1.78, zb = 4.08, tz = Z(1513);
  fbox(B.furn, x, za, x + 0.42, zb, 0, 0.46, C('#34312e'));
  fbox(B.furnGloss, x + 0.42, za + 0.3, x + 0.426, zb - 0.3, 0.09, 0.37, COL.black);
  const [fc, fg] = canvas(512, 64), fr = rng(61);                     // flames
  fg.fillStyle = '#120803'; fg.fillRect(0, 0, 512, 64);
  for (let i = 0; i < 90; i++) {
    const fx = fr() * 512, fh = 18 + fr() * 40, fw = 8 + fr() * 16, grd = fg.createLinearGradient(0, 64, 0, 64 - fh);
    grd.addColorStop(0, 'rgba(255,236,170,0.9)'); grd.addColorStop(0.35, 'rgba(255,150,40,0.75)'); grd.addColorStop(1, 'rgba(200,50,10,0)');
    fg.fillStyle = grd; fg.beginPath(); fg.moveTo(fx - fw, 64); fg.quadraticCurveTo(fx - fw * 0.3, 64 - fh * 0.6, fx + (fr() - 0.5) * 8, 64 - fh); fg.quadraticCurveTo(fx + fw * 0.4, 64 - fh * 0.5, fx + fw, 64); fg.fill();
  }
  const ft = toTex(fc);
  const fire = new THREE.Mesh(new THREE.PlaneGeometry(zb - za - 0.66, 0.24), new THREE.MeshStandardMaterial({ map: ft, emissiveMap: ft, emissive: C('#ffffff'), emissiveIntensity: 1.5, roughness: 1 }));
  fire.rotation.y = Math.PI / 2; fire.position.set(x + 0.43, 0.23, (za + zb) / 2); house.add(fire);
  FURN_FP.push([x, za, x + 0.42, zb]);
  for (const z of [za + 0.6, (za + zb) / 2, zb - 0.6]) giLamp(x + 0.62, 0.24, z, 0.09, null, '#ff9c4d', 0);
  fbox(B.furnGloss, x + 0.05, tz - 0.66, x + 0.085, tz + 0.66, 0.8, 1.54, COL.black);
  fbox(B.furnGloss, x + 0.085, tz - 0.64, x + 0.087, tz + 0.64, 0.82, 1.52, COL.screen);
  vase(x + 0.2, 0.46, za + 0.22, MAT.white, 1.0); books(x + 0.2, 0.46, zb - 0.28, 0.1);
}
sofa3(4.78, 2.93, FACE.w, 2.2, 0.95, MAT.cream, { pillows: [MAT.terracotta, MAT.sage] });
fbox(B.furn, 3.09, 2.5, 3.64, 3.5, 0.34, 0.38, COL.walnut);
fbox(B.furn, 3.155, 2.565, 3.575, 3.435, 0, 0.34, C('#4f3727'));
FURN_FP.push([3.09, 2.5, 3.64, 3.5]);
books(3.36, 0.38, 2.8, 0.25, ['#e6dfd2', '#b5553a', '#394f5c']); succulents(3.36, 0.38, 3.2, 0.12);
{ // tripod floor lamp
  const lx = 5.05, lz = 1.5;
  for (let k = 0; k < 3; k++) { const a = (k * Math.PI * 2) / 3; const leg = new THREE.Mesh(new THREE.CylinderGeometry(0.01, 0.012, 1.42, 6), MAT.walnut);
    leg.position.set(lx + Math.cos(a) * 0.1, 0.7, lz + Math.sin(a) * 0.1); leg.rotation.set(Math.sin(a) * 0.14, 0, -Math.cos(a) * 0.14); leg.castShadow = true; house.add(leg); }
  cyl(lx, lz, 0.17, 0.23, 1.32, 1.62, null, { mat: MAT.shade, seg: 32 }).castShadow = false;
  glow(lx, 1.47, lz, true); giLamp(lx, 1.47, lz, 0.45, null, '#ffdcae', 0);
  footprint(lx, lz, 0, 0.4, 0.4);
}
// Entrance closets, store cupboards
cabinet(549, 1352, 655, 1387, 's', { h: 2.05, doors: 3, body: COL.closet, door: COL.closetDoor, handles: 'mid', kick: 0.06 });
cabinet(657, 1386, 693, 1496, 'w', { h: 2.05, doors: 3, body: COL.closet, door: COL.closetDoor, handles: 'mid', kick: 0.06 });
cabinet(21, 1295, 52, 1356, 'e', { h: 1.8, doors: 2, body: COL.closet, door: COL.closetDoor, handles: 'mid', kick: 0.06 });
cabinet(21, 1362, 52, 1423, 'e', { h: 1.8, doors: 2, body: COL.closet, door: COL.closetDoor, handles: 'mid', kick: 0.06 });
// Bathroom
{
  const tx = X(234.5), wz = Z(1623);
  fbox(B.furnGloss, tx - 0.19, wz - 0.19, tx + 0.19, wz - 0.01, 0.38, 0.8, COL.ceramic);
  fbox(B.furnGloss, tx - 0.2, wz - 0.2, tx + 0.2, wz - 0.0, 0.8, 0.83, COL.ceramic);
  cyl(tx, wz - 0.42, 0.17, 0.14, 0, 0.4, COL.ceramic, { rough: 0.2, sz: 1.3, seg: 32 });
  cyl(tx, wz - 0.42, 0.185, 0.185, 0.4, 0.43, COL.ceramic, { rough: 0.2, sz: 1.3, seg: 32 });
}
{
  cabinet(358, 1494, 401, 1532, 's', { h: 0.82, doors: 2, body: COL.oak, door: C('#c99d6e'), handles: 'mid', kick: 0.0 });
  const vx = X(379.5), vz = Z(1513);
  pbox(B.furnGloss, 357, 1494, 402, 1535, 0.82, 0.86, COL.ceramic);
  cyl(vx, vz, 0.15, 0.15, 0.861, 0.866, C('#e3e8eb'), { rough: 0.15, sz: 0.8, seg: 32 });
  fbox(B.furnMetal, vx - 0.015, Z(1494) + 0.03, vx + 0.015, Z(1494) + 0.06, 0.86, 1.0, COL.steel);
  fbox(B.furnMetal, vx - 0.012, Z(1494) + 0.03, vx + 0.012, Z(1494) + 0.15, 0.97, 1.0, COL.steel);
}
pbox(B.furnGloss, 335, 1557, 404, 1623, 0, 0.06, COL.ceramic);
pbox(B.furnMetal, 345, 1606, 352, 1613, 0.06, 0.064, COL.kick);


/* =====================================================================
   FRONT ELEVATION — stone feature pier, jaali, portico, box frames
   ===================================================================== */
if (false) {
// Stone-clad corner pier (wraps the front-left corner, rises above the terrace)
new TexBox(-6.768, -4.2, -PLINTH, TERR, ZF, ZF + 0.35, M.stone, 1.4);
new TexBox(-6.768, XW, -PLINTH, TERR, 3.9, ZF, M.stone, 1.4);

// Decorative bronze jaali in a charcoal frame (screens the bathroom ventilator)
{
  const x0 = -3.95, x1 = -2.05, y0 = 0.5, y1 = 2.5, t = 0.05;
  cutRect(B.wall, x0, ZF, x1, ZF + 0.1, y1 - t, y1, COL.charcoal);
  cutRect(B.wall, x0, ZF, x1, ZF + 0.1, y0, y0 + t, COL.charcoal);
  cutRect(B.wall, x0, ZF, x0 + t, ZF + 0.1, y0 + t, y1 - t, COL.charcoal);
  cutRect(B.wall, x1 - t, ZF, x1, ZF + 0.1, y0 + t, y1 - t, COL.charcoal);
  new TexBox(x0 + t, x1 - t, y0 + t, y1 - t, ZF + 0.07, ZF + 0.07, M.jaali, 0.6, { faces: 16 });
}

}
// Portico: granite porch, two round pillars, charcoal-framed slab
const PO = { x0: -1.95, x1: 2.35, z1: 6.45, band: 0.3 };
new TexBox(PO.x0, PO.x1, -PLINTH, 0.004, ZF, PO.z1, M.granite, 1.0, { cut: false });
new TexBox(-1.55, 1.95, -PLINTH, -0.1, PO.z1, PO.z1 + 0.32, M.granite, 1.0, { cut: false });
new TexBox(-1.55, 1.95, -PLINTH, -0.2, PO.z1 + 0.32, PO.z1 + 0.64, M.granite, 1.0, { cut: false });
cutRect(B.wallExt, PO.x0, ZF, PO.x1, PO.z1, 2.72, 2.88, C('#a19f9a'));
// cutRect(B.wall, PO.x0, PO.z1 - PO.band, PO.x1, PO.z1, 2.55, 2.975, COL.charcoal);
// cutRect(B.wall, PO.x0, ZF, PO.x0 + 0.12, PO.z1 - PO.band, 2.55, 2.975, COL.charcoal);
// cutRect(B.wall, PO.x1 - 0.12, ZF, PO.x1, PO.z1 - PO.band, 2.55, 2.975, COL.charcoal);
if (false) {
for (const cx of [PO.x0 + 0.25, PO.x1 - 0.25]) {
  const cz = PO.z1 - PO.band / 2;
  cutRect(B.wall, cx - 0.06, cz - 0.06, cx + 0.06, cz + 0.06, 0.004, 2.55, COL.charcoal);
}
}
cutMesh(cyl(0.2, 5.25, 0.07, 0.07, 2.775, 2.79, COL.white, { mat: lampMat }), 2.775, 2.79, 'hide');   // soffit downlight
// wall sconces flanking the main door
for (const sx of [X(496) - 0.33, X(621) + 0.33]) {
  cutRect(B.wall, sx - 0.06, ZF, sx + 0.06, ZF + 0.1, 1.85, 2.15, COL.charcoal);
  cutMesh(lampBox(house, lampMat, sx - 0.035, sx + 0.035, 1.9, 2.1, ZF + 0.1, ZF + 0.108), 1.9, 2.1, 'hide');
}
// plant('house', PO.x0 + 0.42, ZF + 0.38, 1.25, 0.004, COL.charcoal);
// plant('house', PO.x1 - 0.42, ZF + 0.38, 1.25, 0.004, COL.charcoal);

// charcoal box frames with wood cladding & louvers
function boxFrame(x0, x1, y0, y1, face, out, depth, t = 0.15) {
  const za = Math.min(face, face + out * depth), zb = Math.max(face, face + out * depth);
  cutRect(B.wall, x0, za, x1, zb, y1 - t, y1, COL.charcoal);
  cutRect(B.wall, x0, za, x1, zb, y0, y0 + t, COL.charcoal);
  cutRect(B.wall, x0, za, x0 + t, zb, y0 + t, y1 - t, COL.charcoal);
  cutRect(B.wall, x1 - t, za, x1, zb, y0 + t, y1 - t, COL.charcoal);
}
const cladZ = (x0, x1, y0, y1, z0, z1) => new TexBox(x0, x1, y0, y1, z0, z1, M.teak, 1.2);
if (false) {
{ // front: TV room window
  const w0 = X(839), w1 = X(948);
  boxFrame(3.25, 6.0, 0.5, 2.5, ZF, 1, 0.35);
  cladZ(3.4, w0, 0.65, 2.35, ZF, ZF + 0.02);
  cladZ(w1, 5.85, 0.65, 2.35, ZF, ZF + 0.02);
  cladZ(w0, w1, 0.65, SILL - 0.035, ZF, ZF + 0.02);
  cladZ(w0, w1, HEAD, 2.35, ZF, ZF + 0.02);
  for (let i = 0; i < 6; i++) { const cx = 5.02 + i * 0.152; cutRect(B.wall, cx - 0.022, ZF + 0.08, cx + 0.022, ZF + 0.24, 0.65, 2.35, COL.teak); }
}
{ // back: the two living-room windows share one frame
  const a0 = X(780), a1 = X(844), b0 = X(939), b1 = X(1000);
  boxFrame(2.6, 5.8, 0.55, 2.45, ZNN, -1, 0.3);
  const cz = (x0, x1, y0, y1) => cladZ(x0, x1, y0, y1, ZNN - 0.02, ZNN);
  cz(2.75, a0, 0.7, 2.3); cz(a1, b0, 0.7, 2.3); cz(b1, 5.65, 0.7, 2.3);
  cz(a0, a1, 0.7, SILL - 0.035); cz(b0, b1, 0.7, SILL - 0.035);
  cz(a0, a1, HEAD, 2.3); cz(b0, b1, HEAD, 2.3);
}
{ // east (blank) wall: framed teak feature panel with vertical louvers
  const f = XE, z0 = -1.7, z1 = 1.5, y0 = 0.45, y1 = 2.55, d = 0.3, t = 0.15;
  cutRect(B.wall, f, z0, f + d, z1, y1 - t, y1, COL.charcoal);
  cutRect(B.wall, f, z0, f + d, z1, y0, y0 + t, COL.charcoal);
  cutRect(B.wall, f, z0, f + d, z0 + t, y0 + t, y1 - t, COL.charcoal);
  cutRect(B.wall, f, z1 - t, f + d, z1, y0 + t, y1 - t, COL.charcoal);
  new TexBox(f, f + 0.02, y0 + t, y1 - t, z0 + t, z1 - t, M.teak, 1.2);
  for (let z = z0 + t + 0.16; z < z1 - t - 0.08; z += 0.22) cutRect(B.wall, f + 0.08, z - 0.022, f + 0.22, z + 0.022, y0 + t, y1 - t, COL.teak);
}
{ // back: balcony bay clad in teak with a charcoal window surround
  const x0 = X(127), x1 = X(307), w0 = X(154), w1 = X(283);
  cladZ(x0, w0, 0, WALL_H, ZB - 0.02, ZB);
  cladZ(w1, x1, 0, WALL_H, ZB - 0.02, ZB);
  cladZ(w0, w1, 0, SILL - 0.035, ZB - 0.02, ZB);
  cladZ(w0, w1, HEAD, WALL_H, ZB - 0.02, ZB);
  new TexBox(x0 - 0.02, x0, 0, WALL_H, ZB, ZN, M.teak, 1.2);
  new TexBox(x1, x1 + 0.02, 0, WALL_H, ZB, ZN, M.teak, 1.2);
  cutRect(B.wall, w0 - 0.1, ZB - 0.12, w1 + 0.1, ZB, HEAD, HEAD + 0.1, COL.charcoal);
  cutRect(B.wall, w0 - 0.1, ZB - 0.12, w0, ZB, SILL - 0.04, HEAD, COL.charcoal);
  cutRect(B.wall, w1, ZB - 0.12, w1 + 0.1, ZB, SILL - 0.04, HEAD, COL.charcoal);
}

}
/* =====================================================================
   EXTERNAL STAIRCASE (west side) up to the terrace
   ===================================================================== */
const ST = { x0: -7.95, x1: -6.9, z0: 4.3, go: 0.27, n: 20 };
const LAND = TERR + 0.012;                                   // landing top (house-local)
const RISE = (LAND + PLINTH) / ST.n;
const LZ1 = ST.z0 - (ST.n - 1) * ST.go, LZ0 = LZ1 - 1.05;    // landing z range
if (false) {
for (let i = 1; i < ST.n; i++) {
  const zf = ST.z0 - (i - 1) * ST.go, zb = zf - ST.go, top = i * RISE - PLINTH;
  cutRect(B.wall, ST.x0, zb, ST.x1, zf, Math.max(-PLINTH, top - 0.34), top - 0.03, COL.stairBody);
  cutRect(B.wall, ST.x0, zb, ST.x1, zf + 0.025, top - 0.03, top, COL.granite);
  for (const bx of [ST.x0 + 0.05, ST.x1 - 0.05]) for (const zz of [zf - ST.go * 0.25, zf - ST.go * 0.75]) {
    cutRect(B.wall, bx - 0.011, zz - 0.011, bx + 0.011, zz + 0.011, top, top + 0.9, COL.ms);
  }
}
cutRect(B.wall, ST.x0, LZ0, XW, LZ1, LAND - 0.18, LAND - 0.03, COL.stairBody);
cutRect(B.wall, ST.x0, LZ0, XW, LZ1, LAND - 0.03, LAND, COL.granite);
{
  const y0 = LAND + 0.88, y1 = LAND + 0.92, bx = ST.x0 + 0.05;
  cutRect(B.wall, bx - 0.02, LZ0 + 0.03, bx + 0.02, LZ1, y0, y1, COL.ms);
  cutRect(B.wall, ST.x0 + 0.03, LZ0 + 0.03, XW, LZ0 + 0.07, y0, y1, COL.ms);
  cutRect(B.wall, ST.x1 - 0.07, LZ1 - 0.07, XW, LZ1 - 0.03, y0, y1, COL.ms);
  for (let z = LZ0 + 0.1; z < LZ1 - 0.02; z += 0.13) cutRect(B.wall, bx - 0.011, z - 0.011, bx + 0.011, z + 0.011, LAND, y0, COL.ms);
  for (let x = ST.x0 + 0.1; x < XW - 0.02; x += 0.13) cutRect(B.wall, x - 0.011, LZ0 + 0.039, x + 0.011, LZ0 + 0.061, LAND, y0, COL.ms);
  for (let x = ST.x1 - 0.05; x < XW - 0.02; x += 0.13) cutRect(B.wall, x - 0.011, LZ1 - 0.061, x + 0.011, LZ1 - 0.039, LAND, y0, COL.ms);
  // supports
  cutRect(B.wall, ST.x0 + 0.06, LZ0 + 0.06, ST.x0 + 0.3, LZ0 + 0.3, -PLINTH, LAND - 0.18, COL.stairBody);
  const k = 10, zs = ST.z0 - (k - 0.5) * ST.go;
  cutRect(B.wall, ST.x0 + 0.06, zs - 0.12, ST.x0 + 0.3, zs + 0.12, -PLINTH, k * RISE - PLINTH - 0.34, COL.stairBody);
  // sloped handrails (hidden in Inside mode)
  const railMat = new THREE.MeshStandardMaterial({ color: COL.ms, roughness: 0.45, metalness: 0.4 });
  const za = ST.z0 - 0.5 * ST.go, zb = LZ1 + 0.5 * ST.go, ya = RISE - PLINTH + 0.92, yb = (ST.n - 1) * RISE - PLINTH + 0.92;
  for (const x of [ST.x0 + 0.05, ST.x1 - 0.05]) {
    const L = Math.hypot(za - zb, yb - ya);
    const m = new THREE.Mesh(new THREE.BoxGeometry(0.05, 0.04, L + 0.12), railMat);
    m.position.set(x, (ya + yb) / 2, (za + zb) / 2);
    m.rotation.x = Math.atan2(yb - ya, za - zb);
    m.castShadow = true;
    house.add(m);
    cutMesh(m, ya, yb + 0.05, 'hide');
  }
}

}
// plinth (granite) around the house
TX.granite.repeat.set(1, 1);
extrudeShape(offsetPoly(OUTLINE, 0.12), null, -PLINTH, PLINTH, M.granite, house);

/* =====================================================================
   TERRACE — slab, tiles, parapets & glass railing, pergola with jhoola,
   water tank, planters, bougainvillea
   ===================================================================== */
{
  // roof slab with a cornice overhang (notched where the portico slab is)
  const slab = offsetPoly(OUTLINE, 0.12), zf = slab[8][1];
  slab.splice(9, 0, [PO.x1, zf], [PO.x1, ZF], [PO.x0, ZF], [PO.x0, zf]);
  extrudeShape(slab, null, WALL_H, 0.22, [TM.slabTop, TM.slabEdge], terrace);
}
if (false) {
// terrace outline (outer face of the parapet) with the type of each edge
const TO = [
  [XW, ZN, 'solid'], [X(127), ZN, 'solid'], [X(127), ZB, 'solid'], [X(307), ZB, 'solid'], [X(307), ZN, 'solid'],
  [X(694), ZN, 'solid'], [X(694), ZNN, 'solid'], [XE, ZNN, 'solid'], [XE, ZF, 'glass'], [PO.x1, ZF, 'glass'],
  [PO.x1, PO.z1, 'glass'], [PO.x0, PO.z1, 'glass'], [PO.x0, ZF, 'glass'], [-4.2, ZF, 'tower'], [XW, ZF, 'tower'],
  [XW, 3.9, 'solid'], [XW, LZ1, 'gap'], [XW, LZ0, 'solid'],
];
const TO_PTS = TO.map((t) => [t[0], t[1]]), TS = polySign(TO_PTS);
flatShape(offsetPoly(TO_PTS, -0.15), null, TERR + 0.004, TM.tile, terrace);
{
  const NT = TO.length, runs = [];
  const first = TO.findIndex((t, i) => TO[(i - 1 + NT) % NT][2] !== t[2]);
  for (let k = 0; k < NT; k++) {
    const i = (first + k) % NT, type = TO[i][2];
    let run = runs[runs.length - 1];
    if (!run || run.type !== type) { run = { type, pts: [TO_PTS[i]] }; runs.push(run); }
    run.pts.push(TO_PTS[(i + 1) % NT]);
  }
  const strip = (pts, dOut, dIn, y0, h, mat) => extrudeShape([...offsetLine(pts, dOut, TS), ...offsetLine(pts, -dIn, TS).reverse()], null, y0, h, mat, terrace);
  const trim = (pts, atEnd, d) => {
    const [a, b] = atEnd ? [pts[pts.length - 1], pts[pts.length - 2]] : [pts[0], pts[1]];
    const L = Math.hypot(b[0] - a[0], b[1] - a[1]);
    a[0] += ((b[0] - a[0]) * d) / L; a[1] += ((b[1] - a[1]) * d) / L;
  };
  runs.forEach((run, ri) => {
    const prev = runs[(ri - 1 + runs.length) % runs.length].type, next = runs[(ri + 1) % runs.length].type;
    const pts = run.pts.map((p) => p.slice());
    if (run.type === 'solid') {
      strip(pts, 0, 0.15, TERR - 0.005, 0.905, TM.parapet);
      strip(pts, 0.03, 0.18, TERR + 0.9, 0.05, TM.coping);
    } else if (run.type === 'glass') {
      if (prev === 'solid') trim(pts, false, 0.15);
      if (next === 'solid') trim(pts, true, 0.15);
      strip(pts, 0, 0.15, TERR - 0.005, 0.155, TM.parapet);
      const cl = offsetLine(pts, -0.075, TS);
      for (let i = 0; i < cl.length - 1; i++) {
        const [ax, az] = cl[i], [bx, bz] = cl[i + 1], alongX = Math.abs(bz - az) < 1e-6;
        const len = Math.hypot(bx - ax, bz - az), nP = Math.max(1, Math.ceil(len / 1.3));
        for (let p = 0; p < nP; p++) {
          const t0 = p / nP, t1 = (p + 1) / nP;
          if (alongX) { const xa = ax + (bx - ax) * t0, xb = ax + (bx - ax) * t1; fbox(B.tGlass, Math.min(xa, xb) + 0.006, az - 0.006, Math.max(xa, xb) - 0.006, az + 0.006, TERR + 0.15, TERR + 1.0, COL.railGlass); }
          else { const za = az + (bz - az) * t0, zb = az + (bz - az) * t1; fbox(B.tGlass, ax - 0.006, Math.min(za, zb) + 0.006, ax + 0.006, Math.max(za, zb) - 0.006, TERR + 0.15, TERR + 1.0, COL.railGlass); }
        }
        if (alongX) fbox(B.tMetal, Math.min(ax, bx) - 0.025, az - 0.025, Math.max(ax, bx) + 0.025, az + 0.025, TERR + 1.0, TERR + 1.04, COL.steel);
        else fbox(B.tMetal, ax - 0.025, Math.min(az, bz) - 0.025, ax + 0.025, Math.max(az, bz) + 0.025, TERR + 1.0, TERR + 1.04, COL.steel);
      }
    } else if (run.type === 'gap') {
      fbox(B.tMatte, XW, LZ0, XW + 0.15, LZ1, TERR, TERR + 0.012, COL.granite);
    }
  });
}
// stone pier rising above the terrace
new TexBox(-6.768, -4.2, TERR, TERR + 1.3, 3.9, ZF + 0.35, TM.stone, 1.4, { parent: terrace, cut: false });
fbox(B.tMatte, -6.8, 3.87, -4.17, ZF + 0.38, TERR + 1.3, TERR + 1.36, COL.charcoal);

// pergola (teak) with a jhoola swing and seating
const PG = { x0: 1.5, x1: 5.9, z0: 1.0, z1: 3.9, h: 2.3 };
{
  const T = TERR, top = T + PG.h;
  for (const x of [PG.x0, PG.x1]) for (const z of [PG.z0, PG.z1]) fbox(B.tMatte, x - 0.075, z - 0.075, x + 0.075, z + 0.075, T, top - 0.2, COL.teak);
  for (const z of [PG.z0, PG.z1]) fbox(B.tMatte, PG.x0 - 0.3, z - 0.05, PG.x1 + 0.3, z + 0.05, top - 0.2, top, COL.teak);
  for (let x = PG.x0 - 0.1; x <= PG.x1 + 0.11; x += 0.3) fbox(B.tMatte, x - 0.025, PG.z0 - 0.35, x + 0.025, PG.z1 + 0.35, top, top + 0.14, COL.teak);
  // outdoor lounge: sofa + low table
  sofa(4.7, 2.2, FACE.s, 1.9, 0.85, 3, C('#5c5f63'), C('#d9d4ca'), { b: B.tMatte, y: T, pillow: C('#3f6f73') });
  fbox(B.tMatte, 4.2, 2.95, 5.2, 3.5, T, T + 0.36, COL.teak);
  // two rattan armchairs and a small table
  sofa(2.05, 2.55, FACE.e, 0.72, 0.72, 1, COL.rattan, COL.cushion, { pillows: false, b: B.tMatte, y: T });
  sofa(3.25, 2.55, FACE.w, 0.72, 0.72, 1, COL.rattan, COL.cushion, { pillows: false, b: B.tMatte, y: T });
  cyl(2.65, 2.55, 0.3, 0.3, T + 0.5, T + 0.54, COL.teak, { parent: terrace, rough: 0.5 });
  cyl(2.65, 2.55, 0.05, 0.08, T, T + 0.5, COL.ms, { parent: terrace, rough: 0.5 });
  // string lights along the beams
  const bulbs = [];
  for (const z of [PG.z0, PG.z1]) for (let x = PG.x0 + 0.1; x < PG.x1; x += 0.36) bulbs.push([x, top - 0.28 - 0.04 * Math.sin(((x - PG.x0) / (PG.x1 - PG.x0)) * Math.PI * 4), z]);
  const im = new THREE.InstancedMesh(new THREE.SphereGeometry(0.035, 10, 8), TM.lamp, bulbs.length);
  const m4 = new THREE.Matrix4();
  bulbs.forEach((b, i) => im.setMatrixAt(i, m4.makeTranslation(b[0], b[1], b[2])));
  im.frustumCulled = false;
  terrace.add(im);
}
// planters, pots and a bougainvillea climbing the pergola
fbox(B.tMatte, XE - 0.6, -2.6, XE - 0.15, 0.6, TERR, TERR + 0.42, COL.charcoal);
for (let z = -2.4; z <= 0.45; z += 0.36) blob('terrace', XE - 0.38, TERR + 0.55, z, 0.22, GREENS[Math.round(z * 10) % 3 < 0 ? 0 : Math.round(z * 10) % 3], 0.8);
plant('terrace', XW + 0.45, LZ1 + 0.45, 1.1, TERR);
plant('terrace', XE - 0.45, ZNN + 0.45, 1.15, TERR);
plant('terrace', XE - 1.15, ZNN + 0.42, 0.95, TERR);
plant('terrace', X(694) + 0.45, ZNN + 0.45, 1.0, TERR);
plant('terrace', PG.x0 - 0.35, PG.z1 + 0.15, 1.2, TERR);
plant('terrace', -1.5, 6.05, 1.0, TERR);
plant('terrace', 1.9, 6.05, 1.0, TERR);
{ // bougainvillea: bush in a pot, climbing the corner post and spreading over the pergola
  const px = PG.x1 + 0.22, pz = PG.z1 + 0.24;
  cyl(px, pz, 0.22, 0.17, TERR, TERR + 0.45, COL.terracotta, { parent: terrace, rough: 0.85, seg: 20 });
  const mag = [C('#d6267a'), C('#e8448f'), C('#b81d63')], r = rng(91);
  const pick = (i, p) => (r() < p ? mag[i % 3] : GREENS[i % 3]);
  for (let i = 0; i < 16; i++) { const a = r() * 6.28, d = r() * 0.2; blob('terrace', px + Math.cos(a) * d, TERR + 0.5 + r() * 0.35, pz + Math.sin(a) * d, 0.07 + r() * 0.04, pick(i, 0.45)); }
  for (let i = 0; i < 26; i++) { const y = TERR + 0.8 + (i / 25) * (PG.h - 0.75), a = r() * 6.28; blob('terrace', PG.x1 + Math.cos(a) * 0.1, y, PG.z1 + Math.sin(a) * 0.1, 0.055 + r() * 0.035, pick(i, 0.5)); }
  const topY = TERR + PG.h + 0.16;
  for (let i = 0; i < 70; i++) blob('terrace', PG.x1 + 0.28 - r() * 1.75, topY + r() * 0.06, PG.z1 + 0.32 - r() * 1.4, 0.06 + r() * 0.04, pick(i, 0.6), 0.6);
  for (let i = 0; i < 12; i++) blob('terrace', PG.x1 + 0.12 - r() * 0.6, TERR + 0.95 - r() * 0.55, ZF + 0.07 + r() * 0.06, 0.045 + r() * 0.025, pick(i, 0.6));
}

}
/* =====================================================================
   SITE — compound wall & gate, driveway, lawn, garden, road
   ===================================================================== */
const PL = { x0: -8.9, x1: 8.0, z0: -5.9, z1: 11.2 };       // plot (outer faces of compound wall)
{
  const ground = new THREE.Mesh(new THREE.PlaneGeometry(600, 600), new THREE.MeshStandardMaterial({ map: (() => { const t = grassTex('#a3ac80', 12); t.repeat.set(150, 150); return t; })(), color: C('#ebe7d2'), roughness: 1 }));
  ground.rotation.x = -Math.PI / 2;
  ground.receiveShadow = true;
  site.add(ground);
  const lawnTex = grassTex('#6f9a4c', 13);
  groundRect(site, PL.x0 + 0.2, PL.z0 + 0.2, PL.x1 - 0.2, PL.z1 - 0.2, 0.006, new THREE.MeshStandardMaterial({ map: lawnTex, color: C('#d9dfcf'), roughness: 1 }), 3.5);
  const apron = graniteTex([182, 179, 172], 53);
  const apronMat = new THREE.MeshStandardMaterial({ map: apron, roughness: 0.9 });
  apron.repeat.set(1 / 1.2, 1 / 1.2);
  flatShape(offsetPoly(OUTLINE, 0.85), [offsetPoly(OUTLINE, 0.12)], 0.012, apronMat, site);
  const drive = tileTex({ base: [178, 176, 171], grout: '#6f6d69', n: 1, jitter: 6, seed: 57, size: 256 });
  const driveMat = new THREE.MeshStandardMaterial({ map: drive, roughness: 0.9 });
  groundRect(site, -1.55, PO.z1, 1.95, PL.z1 - 0.2, 0.014, driveMat, 1.2);
  const asphalt = asphaltTex();
  groundRect(site, -90, 11.9, 90, 18.9, 0.008, new THREE.MeshStandardMaterial({ map: asphalt, roughness: 0.95 }), 3);
  fbox(B.sMatte, -90, PL.z1, 90, 11.9, 0, 0.1, COL.kerb);
  for (let x = -88; x < 88; x += 3.6) fbox(B.sMatte, x, 15.33, x + 1.8, 15.47, 0.008, 0.014, C('#f0efe8'));
}
if (false) {
// compound wall with pillars
{
  const WALL_C = C('#efe8db'), CAP = COL.charcoal, T = 0.2;
  const seg = (x0, z0, x1, z1, h) => {
    fbox(B.sMatte, x0, z0, x1, z1, 0, h, WALL_C);
    const alongX = x1 - x0 > z1 - z0;
    fbox(B.sMatte, alongX ? x0 : x0 - 0.04, alongX ? z0 - 0.04 : z0, alongX ? x1 : x1 + 0.04, alongX ? z1 + 0.04 : z1, h, h + 0.05, CAP);
  };
  const pillar = (x0, z0, x1, z1, h, stone) => {
    if (stone) new TexBox(x0, x1, 0, h, z0, z1, M.stone, 1.4, { parent: site, cut: false });
    else fbox(B.sMatte, x0, z0, x1, z1, 0, h, WALL_C);
    fbox(B.sMatte, x0 - 0.05, z0 - 0.05, x1 + 0.05, z1 + 0.05, h, h + 0.07, CAP);
  };
  // front wall (low) with stone pillars and the gates
  const fz0 = PL.z1 - T, fz1 = PL.z1, pz0 = fz0 - 0.125, pz1 = fz1 + 0.125, FH = 1.2;
  const fp = [[PL.x0, -8.45, 1.45], [-5.475, -5.025, 1.45], [-2.05, -1.6, 1.9], [2.0, 2.45, 1.9], [3.45, 3.9, 1.6], [7.55, PL.x1, 1.45]];
  for (const [a, b, h] of fp) pillar(a, pz0, b, pz1, h, true);
  seg(-8.45, fz0, -5.475, fz1, FH); seg(-5.025, fz0, -2.05, fz1, FH); seg(3.9, fz0, 7.55, fz1, FH);
  // side & back walls (taller, plastered pillars)
  const SH = 1.6;
  pillar(PL.x0, PL.z0, PL.x0 + 0.45, PL.z0 + 0.45, SH + 0.15, false);
  pillar(PL.x1 - 0.45, PL.z0, PL.x1, PL.z0 + 0.45, SH + 0.15, false);
  const sideLine = (x0, x1, zs) => { let z = PL.z0 + 0.45; for (const zc of zs) { seg(x0, z, x1, zc - 0.2, SH); pillar(x0 + (x1 - x0 > 0 ? 0 : 0), zc - 0.2, x1, zc + 0.2, SH + 0.15, false); z = zc + 0.2; } seg(x0, z, x1, pz0, SH); };
  sideLine(PL.x0, PL.x0 + T, [-1.5, 2.6, 6.7]);
  sideLine(PL.x1 - T, PL.x1, [-1.5, 2.6, 6.7]);
  { let x = PL.x0 + 0.45; for (const xc of [-4.3, -0.2, 3.7]) { seg(x, PL.z0, xc - 0.2, PL.z0 + T, SH); pillar(xc - 0.2, PL.z0, xc + 0.2, PL.z0 + T, SH + 0.15, false); x = xc + 0.2; } seg(x, PL.z0, PL.x1 - 0.45, PL.z0 + T, SH); }
  // gates: charcoal frame with horizontal slats and a teak band
  const gateLeaf = (x0, x1, y0, y1) => {
    const zc = PL.z1 - T / 2, t = 0.05, d = 0.035;
    fbox(B.sMatte, x0, zc - d, x0 + t, zc + d, y0, y1, COL.ms);
    fbox(B.sMatte, x1 - t, zc - d, x1, zc + d, y0, y1, COL.ms);
    fbox(B.sMatte, x0 + t, zc - d, x1 - t, zc + d, y1 - t, y1, COL.ms);
    fbox(B.sMatte, x0 + t, zc - d, x1 - t, zc + d, y0, y0 + t, COL.ms);
    const n = 10, gap = 0.032, sh = ((y1 - y0 - 2 * t) - (n + 1) * gap) / n;
    for (let i = 0; i < n; i++) { const ya = y0 + t + gap + i * (sh + gap); fbox(B.sMatte, x0 + t, zc - 0.014, x1 - t, zc + 0.014, ya, ya + sh, i === 4 || i === 5 ? COL.teak : COL.ms); }
  };
  gateLeaf(-1.58, 0.195, 0.08, 1.55); gateLeaf(0.205, 1.98, 0.08, 1.55); gateLeaf(2.47, 3.43, 0.08, 1.42);
  // gate lamps
  for (const [a, b] of [[-2.05, -1.6], [2.0, 2.45]]) {
    const cx = (a + b) / 2, cz = PL.z1 - T / 2;
    lampBox(site, lampMat, cx - 0.1, cx + 0.1, 1.97, 2.25, cz - 0.1, cz + 0.1);
    fbox(B.sMatte, cx - 0.13, cz - 0.13, cx + 0.13, cz + 0.13, 2.25, 2.29, COL.ms);
  }
  // garden bollards along the driveway
  for (const x of [-1.8, 2.2]) for (const z of [8.3, 9.9]) {
    fbox(B.sMatte, x - 0.06, z - 0.06, x + 0.06, z + 0.06, 0.014, 0.55, COL.ms);
    lampBox(site, lampMat, x - 0.055, x + 0.055, 0.55, 0.6, z - 0.055, z + 0.055);
    fbox(B.sMatte, x - 0.07, z - 0.07, x + 0.07, z + 0.07, 0.6, 0.62, COL.ms);
  }
}
}
if (false) {
{ // low clipped hedge along the plot edge (open, no compound wall)
  const HC = C('#3f6b34'), h = 0.75, w = 0.55;
  fbox(B.sMatte, PL.x0, PL.z1 - w, -1.6, PL.z1, 0, h, HC); fbox(B.sMatte, 2.0, PL.z1 - w, PL.x1, PL.z1, 0, h, HC);
  fbox(B.sMatte, PL.x0, PL.z0, PL.x0 + w, PL.z1 - w, 0, h, HC); fbox(B.sMatte, PL.x1 - w, PL.z0, PL.x1, PL.z1 - w, 0, h, HC);
  fbox(B.sMatte, PL.x0 + w, PL.z0, PL.x1 - w, PL.z0 + w, 0, h, HC);
}
}
// trees, shrubs
const trunkMat = new THREE.MeshStandardMaterial({ map: (() => { const t = trunkTex(); t.repeat.set(2, 16); return t; })(), roughness: 0.9 });
const frondGeo = (() => {
  const L = 2.5, segs = 12, Wd = 0.78, pos = [], idx = [];
  for (let i = 0; i <= segs; i++) {
    const t = i / segs, w = Wd * Math.sin(Math.PI * Math.min(1, t * 1.08 + 0.04)) * (1 - 0.45 * t) + 0.02, z = t * L, y = -1.0 * t * t;
    pos.push(-w / 2, y + w * 0.2, z, 0, y, z, w / 2, y + w * 0.2, z);
  }
  for (let i = 0; i < segs; i++) { const a = i * 3; idx.push(a, a + 3, a + 1, a + 1, a + 3, a + 4, a + 1, a + 4, a + 2, a + 2, a + 4, a + 5); }
  const g = new THREE.BufferGeometry();
  g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3));
  g.setIndex(idx);
  g.computeVertexNormals();
  return g;
})();
const frondMat = new THREE.MeshStandardMaterial({ color: C('#4f8034'), roughness: 0.75, side: THREE.DoubleSide });
function palm(x, z, h, lx, lz) {
  const dir = new THREE.Vector3(lx, h, lz), L = dir.length();
  const tr = new THREE.Mesh(new THREE.CylinderGeometry(0.13, 0.2, L, 14), trunkMat);
  tr.position.set(x + lx / 2, L / 2 * (h / L), z + lz / 2);
  tr.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), dir.clone().normalize());
  tr.castShadow = tr.receiveShadow = true;
  site.add(tr);
  const top = new THREE.Vector3(x + lx, h, z + lz), n = 17;
  const im = new THREE.InstancedMesh(frondGeo, frondMat, n);
  const m4 = new THREE.Matrix4(), q = new THREE.Quaternion(), e = new THREE.Euler(), s = new THREE.Vector3();
  for (let i = 0; i < n; i++) {
    const ring2 = i >= 11, yaw = ((ring2 ? (i - 11) / 6 + 0.09 : i / 11) * Math.PI * 2);
    e.set(-(ring2 ? 0.02 : 0.42), yaw, 0, 'YXZ'); q.setFromEuler(e);
    im.setMatrixAt(i, m4.compose(top, q, s.setScalar(ring2 ? 0.85 : 1)));
  }
  im.castShadow = true; im.frustumCulled = false;
  site.add(im);
  for (let i = 0; i < 5; i++) { const a = i * 1.26; blob('site', top.x + Math.cos(a) * 0.17, h - 0.2, top.z + Math.sin(a) * 0.17, 0.11, C('#6d6a2e')); }
}
// trees: tapered trunk, a few branches and a crown of alpha-tested leaf cards (canvas-drawn leaves);
// card normals point out of the crown so the foliage shades like a soft volume
function leafTex(seed, hue) {
  const [c, g] = canvas(256, 256); const r = rng(seed);
  for (let i = 0; i < 180; i++) {
    const x = 14 + r() * 228, y = 14 + r() * 228, L = 8 + r() * 10, a = r() * Math.PI * 2;
    g.save(); g.translate(x, y); g.rotate(a);
    g.fillStyle = `hsl(${hue + (r() - 0.5) * 20}, ${36 + r() * 22}%, ${20 + r() * 24}%)`;
    g.beginPath(); g.ellipse(0, 0, L, L * 0.42, 0, 0, Math.PI * 2); g.fill();
    g.strokeStyle = 'rgba(20,40,10,0.35)'; g.lineWidth = 0.8; g.beginPath(); g.moveTo(-L, 0); g.lineTo(L, 0); g.stroke();
    g.restore();
  }
  const t = toTex(c); t.wrapS = t.wrapT = THREE.ClampToEdgeWrapping;
  return t;
}
const LEAF_MATS = [leafTex(201, 100), leafTex(202, 86)].map((map) => new THREE.MeshStandardMaterial({ map, alphaTest: 0.5, side: THREE.DoubleSide, roughness: 0.8 }));
function tree(x, z, h = 2.9, k = 1, seed = 1) {
  const r = rng(seed * 7919 + 13), bark = matFor(site, C('#5b4636'), 0.92);
  cyl(x, z, 0.09 * k, 0.17 * k, 0, h * 0.92, null, { parent: site, seg: 12, mat: bark });
  for (let i = 0; i < 6; i++) {
    const a = i * 2.4 + r(), len = (0.8 + r() * 0.6) * k, y0 = h * (0.5 + r() * 0.35);
    const dir = new THREE.Vector3(Math.cos(a) * 0.8, 0.55 + r() * 0.35, Math.sin(a) * 0.8).normalize();
    const br = new THREE.Mesh(new THREE.CylinderGeometry(0.022 * k, 0.05 * k, len, 6), bark);
    br.position.set(x + (dir.x * len) / 2, y0 + (dir.y * len) / 2, z + (dir.z * len) / 2);
    br.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), dir);
    br.castShadow = true; site.add(br);
  }
  const cy = h + 0.4 * k, R = 1.35 * k, RY = 1.0 * k, sets = [[], []];
  for (let i = 0; i < 96; i++) {
    let px, py, pz;
    do { px = r() * 2 - 1; py = r() * 2 - 1; pz = r() * 2 - 1; } while (px * px + py * py + pz * pz > 1);
    const sh = Math.cbrt(0.4 + 0.6 * r()), sz = (0.7 + r() * 0.5) * k;
    const q = new THREE.PlaneGeometry(sz, sz);
    q.rotateX(r() * Math.PI); q.rotateY(r() * Math.PI * 2); q.rotateZ((r() - 0.5) * 1.2);
    q.translate(x + px * R * sh, cy + py * RY * sh, z + pz * R * sh);
    const p2 = q.attributes.position, n2 = q.attributes.normal;
    for (let v = 0; v < p2.count; v++) {
      const nv = new THREE.Vector3((p2.getX(v) - x) / (R * R), (p2.getY(v) - cy + 0.3 * k) / (RY * RY), (p2.getZ(v) - z) / (R * R)).normalize();
      n2.setXYZ(v, nv.x, nv.y, nv.z);
    }
    sets[i % 2].push(q);
  }
  sets.forEach((list, j) => { const m = new THREE.Mesh(mergeGeometries(list), LEAF_MATS[j]); m.castShadow = m.receiveShadow = true; site.add(m); });
}
tree(7.0, 9.9, 2.6, 0.95, 1);
tree(6.6, -7.4, 2.8, 1.0, 2);
tree(-8.6, -6.0, 2.9, 1.05, 3);
if (false) {
{ // hibiscus shrubs along the front wall and by the house
  const r = rng(101), red = [C('#d61f2c'), C('#f04e5a')];
  const spots = [];
  for (let x = -8.2; x <= -2.6; x += 0.9) spots.push([x, PL.z1 - 0.65]);
  for (let x = 4.4; x <= 7.2; x += 0.9) spots.push([x, PL.z1 - 0.65]);
  spots.push([3.0, 7.3], [4.6, 7.3], [6.1, 7.4], [-5.6, 6.9]);
  for (const [x, z] of spots) {
    blob('site', x, 0.38, z, 0.42 + r() * 0.08, GREENS[Math.floor(r() * 3)], 0.8);
    for (let k = 0; k < 4; k++) { const a = r() * 6.28; blob('site', x + Math.cos(a) * 0.3, 0.5 + r() * 0.2, z + Math.sin(a) * 0.3, 0.06, red[k % 2]); }
  }
}
}

/* =====================================================================
   NIGHT LIGHTS (off in the day)
   ===================================================================== */
const NIGHT_LIGHTS = [];
function addPoint(x, y, z, on, dist, color = 0xffc480) {
  const l = new THREE.PointLight(color, 0, dist, 2);
  l.position.set(x, y, z);
  scene.add(l);
  NIGHT_LIGHTS.push({ l, on });
}
function addSpot(x, y, z, tx, ty, tz, on, dist, angle, color = 0xffcf8f) {
  const l = new THREE.SpotLight(color, 0, dist, angle, 0.7, 2);
  l.position.set(x, y, z);
  l.target.position.set(tx, ty, tz);
  scene.add(l, l.target);
  NIGHT_LIGHTS.push({ l, on });
}
// (the rooms themselves are lit by the baked lamp light — only the outdoor lights are real-time)
addPoint(0.2, PLINTH + 2.45, 5.4, 4, 6);
addSpot(-5.5, 0.12, ZF + 0.8, -5.5, 4.4, ZF + 0.36, 30, 8, 0.42);
addSpot(-3.0, PLINTH + 0.45, ZF + 0.035, -3.0, PLINTH + 2.6, ZF, 12, 4, 0.95);

// soft glow around every lamp (camera-facing sprites, added on top of the picture)
function buildGlows() {
  const [c, g] = canvas(128, 128), grd = g.createRadialGradient(64, 64, 0, 64, 64, 64);
  grd.addColorStop(0, 'rgba(255,250,240,1)'); grd.addColorStop(0.12, 'rgba(255,244,224,0.85)'); grd.addColorStop(0.35, 'rgba(255,236,204,0.22)'); grd.addColorStop(1, 'rgba(255,230,190,0)');
  g.fillStyle = grd; g.fillRect(0, 0, 128, 128);
  const tex = new THREE.CanvasTexture(c); tex.colorSpace = THREE.SRGBColorSpace;
  for (const [list, size, op] of [[GLOWS.small, 0.2, 0.85], [GLOWS.big, 0.7, 0.4]]) {
    if (!list.length) continue;
    const geo = new THREE.BufferGeometry();
    geo.setAttribute('position', new THREE.Float32BufferAttribute(list, 3));
    const mat = tMat(new THREE.PointsMaterial({ map: tex, size, sizeAttenuation: true, transparent: true, opacity: op, depthWrite: false, blending: THREE.AdditiveBlending, fog: false }));
    const pts = new THREE.Points(geo, mat);
    pts.frustumCulled = false; pts.renderOrder = 5;
    terrace.add(pts); GLOW_PTS.push(pts);
  }
}
/* =====================================================================
   INTERIOR FINISHES & DECOR — wood ceiling, downlights, rugs, art,
   pendants, indoor plants, accessories (for a more realistic walk-through)
   ===================================================================== */
{
  // warm wood-plank ceiling (part of the roof, so it hides with it)
  const ceilTex = plankTex();
  const ceilMat = tMat(new THREE.MeshStandardMaterial({ map: ceilTex, roughness: 0.7, side: THREE.DoubleSide }));
  ceilMat.userData.gi = 'ceil';
  const downMat = tMat(new THREE.MeshStandardMaterial({ color: C('#ffffff'), emissive: C('#fff1d6'), emissiveIntensity: 1.6 }));
  PHOTO_SWAP.set(downMat, Object.assign(downMat.clone(), { emissiveIntensity: 0.3 }));   // the real light comes from the photo spots
  const disc = new THREE.CylinderGeometry(0.055, 0.055, 0.012, 16);
  for (const room of ROOMS) {
    for (const [a, b, c, d] of room.rects) {
      const x0 = X(a), x1 = X(c), z0 = Z(b), z1 = Z(d);
      const g = new THREE.PlaneGeometry(x1 - x0, z1 - z0);
      g.rotateX(Math.PI / 2); g.translate((x0 + x1) / 2, WALL_H - 0.006, (z0 + z1) / 2);
      const uv = g.attributes.uv, pp = g.attributes.position;
      for (let i = 0; i < pp.count; i++) uv.setXY(i, pp.getX(i) / 4.2, pp.getZ(i) / 1.68);   // boards run east–west
      const m = new THREE.Mesh(g, ceilMat); m.receiveShadow = true; terrace.add(m);
      // recessed downlights on a ~1.4 m grid
      const nx = Math.max(1, Math.round((x1 - x0) / 1.4)), nz = Math.max(1, Math.round((z1 - z0) / 1.4));
      for (let i = 0; i < nx; i++) for (let j = 0; j < nz; j++) {
        const l = new THREE.Mesh(disc, downMat);
        l.position.set(x0 + ((i + 0.5) * (x1 - x0)) / nx, WALL_H - 0.012, z0 + ((j + 0.5) * (z1 - z0)) / nz);
        terrace.add(l);
        giLamp(l.position.x, WALL_H - 0.05, l.position.z, GI.LAMP);
        glow(l.position.x, WALL_H - 0.03, l.position.z);
      }
    }
  }
}
// rugs (procedural)
function rugTex(bg, line, seed, accent = null) {
  const [c, g] = canvas(512, 512); g.fillStyle = bg; g.fillRect(0, 0, 512, 512);
  const lattice = (off) => { for (let k = -512; k < 1024; k += 64) { g.beginPath(); g.moveTo(k + off, 0); g.lineTo(k + off + 512, 512); g.stroke(); g.beginPath(); g.moveTo(k + off + 512, 0); g.lineTo(k + off, 512); g.stroke(); } };
  if (accent) { g.strokeStyle = accent; g.lineWidth = 2.2; lattice(11); lattice(-11); }      // thin accent lines beside the lattice
  g.strokeStyle = line; g.lineWidth = 7; lattice(0);
  g.strokeStyle = line; g.lineWidth = 18; g.strokeRect(18, 18, 476, 476);
  addNoise(g, 512, 512, 0.06, seed);
  const t = toTex(c); t.wrapS = t.wrapT = THREE.ClampToEdgeWrapping; return t;
}
function rug(cx, cz, w, d, tex) {
  const m = new THREE.Mesh(new THREE.PlaneGeometry(w, d), new THREE.MeshStandardMaterial({ map: tex, roughness: 1, polygonOffset: true, polygonOffsetFactor: -2 }));
  m.material.userData.gi = 'floor';                 // rugs share the floor's lightmap
  m.rotation.x = -Math.PI / 2; m.position.set(cx, 0.008, cz); m.receiveShadow = true; house.add(m);
}
rug(4.75, -1.7, 2.75, 2.75, rugTex('#2f8a86', '#f2efe6', 3, '#e6bb45'));          // lounge
rug((X(508) + X(657)) / 2 - 0.3, (Z(1088) + Z(1181)) / 2, 2.9, 2.2, rugTex('#ece6da', '#cbbfaa', 4)); // dining
rug(X(830), Z(1513), 2.0, 2.4, rugTex('#8a6a55', '#e8dcc8', 5));                 // TV room
// abstract wall art (canvas paintings)
function artTex(seed, pal) {
  const [c, g] = canvas(512, 384); const r = rng(seed);
  g.fillStyle = pal[0]; g.fillRect(0, 0, 512, 384);
  for (let i = 0; i < 26; i++) { g.fillStyle = pal[1 + (i % (pal.length - 1))]; g.globalAlpha = 0.55 + r() * 0.4; g.beginPath();
    g.ellipse(r() * 512, r() * 384, 30 + r() * 120, 20 + r() * 90, r() * 3, 0, Math.PI * 2); g.fill(); }
  g.globalAlpha = 1; addNoise(g, 512, 384, 0.05, seed);
  return toTex(c);
}
function artBrushTex(seed, pal) {            // loose brush strokes in a few colours on a cream ground
  const [c, g] = canvas(768, 512); const r = rng(seed);
  g.fillStyle = pal[0]; g.fillRect(0, 0, 768, 512);
  for (let k = 0; k < 10; k++) {
    const cx = 90 + r() * 590, cy = 70 + r() * 370, col = pal[1 + (k % (pal.length - 1))], spread = 55 + r() * 100;
    for (let i = 0; i < 80; i++) {
      const x = cx + (r() + r() - 1) * spread * 1.5, y = cy + (r() + r() - 1) * spread, w = 30 + r() * 95, h = 8 + r() * 26;
      g.save(); g.translate(x, y); g.rotate((r() - 0.5) * 0.9); g.globalAlpha = 0.22 + r() * 0.5; g.fillStyle = col;
      g.beginPath(); g.ellipse(0, 0, w / 2, h / 2, 0, 0, Math.PI * 2); g.fill(); g.restore();
    }
  }
  g.globalAlpha = 1; addNoise(g, 768, 512, 0.06, seed);
  return toTex(c);
}
function art(x, y, z, w, h, ry, seed, pal, brush = false) {
  const grp = new THREE.Group();
  const fr = new THREE.Mesh(new THREE.BoxGeometry(w + 0.06, h + 0.06, 0.04), new THREE.MeshStandardMaterial({ color: C('#1e1f21'), roughness: 0.5 }));
  const cv = new THREE.Mesh(new THREE.PlaneGeometry(w, h), new THREE.MeshStandardMaterial({ map: brush ? artBrushTex(seed, pal) : artTex(seed, pal), roughness: 0.8 }));
  cv.position.z = 0.021; grp.add(fr, cv); grp.position.set(x, y, z); grp.rotation.y = ry; fr.castShadow = true; house.add(grp);
  cutMesh(grp, y - h / 2, y + h / 2, 'hide');
}
art(0.2, 1.62, Z(993) + 0.03, 1.9, 1.2, 0, 7, ['#efe8da', '#e0792b', '#c63d2a', '#2f5d3a', '#2f8a86', '#e2a93b'], true);     // dining wall
art(X(1061) - 0.03, 1.68, -1.7, 1.5, 1.05, -Math.PI / 2, 8, ['#f1ece4', '#2f8a86', '#d9a441', '#9c3d2e', '#33524a'], true);     // lounge east wall
art(X(1061) - 0.03, 1.6, Z(1513), 1.2, 0.9, -Math.PI / 2, 9, ['#efe8dc', '#5a7182', '#c98b5a', '#283044']);           // TV room
art(X(404) - 0.03, 1.6, Z(1120), 1.0, 0.75, -Math.PI / 2, 10, ['#f4efe6', '#7c9a5e', '#d6b25e', '#3d5a40']);         // kitchen (east wall)
art(X(423) + 0.03, 1.55, Z(1555), 0.9, 1.2, Math.PI / 2, 11, ['#ebe4d6', '#b8573b', '#e0b26a', '#3b3a36']);         // entrance (west wall)
// pendant lights: a cluster of white domes over the dining table, a row of glowing tubes over the kitchen island
{
  const tx = (X(508) + X(657)) / 2 - 0.3, tz = (Z(1088) + Z(1181)) / 2;
  const domeOut = new THREE.MeshStandardMaterial({ color: C('#f3f1ec'), roughness: 0.45, side: THREE.DoubleSide });
  for (const [dx, dz, y, r] of [[-0.4, 0.06, 1.95, 0.2], [0.06, -0.12, 1.72, 0.24], [0.46, 0.1, 2.08, 0.17]]) {
    const g = new THREE.Group();
    const dome = new THREE.Mesh(new THREE.SphereGeometry(r, 32, 12, 0, Math.PI * 2, 0, Math.PI / 2), domeOut); dome.castShadow = true;
    const inner = new THREE.Mesh(new THREE.SphereGeometry(r * 0.96, 32, 12, 0, Math.PI * 2, 0, Math.PI / 2), MAT.domeIn);
    const bulb = new THREE.Mesh(new THREE.SphereGeometry(0.045, 16, 8), MAT.tube); bulb.position.y = 0.05;
    const cord = new THREE.Mesh(new THREE.CylinderGeometry(0.004, 0.004, WALL_H - y - r), MAT.blackMetal); cord.position.y = r + (WALL_H - y - r) / 2;
    g.add(dome, inner, bulb, cord); g.position.set(tx + dx, y, tz + dz); house.add(g);
    cutMesh(g, y - 0.05, WALL_H, 'hide');
    glow(tx + dx, y + 0.03, tz + dz);
    giLamp(tx + dx, y - 0.02, tz + dz, 0.55, [0.05, 0.8], '#ffe9cf', 0);
  }
  const ix = -3.75, iz = -2.05, iy = 1.95, tubes = [], bar = new THREE.Group();
  const rail = new THREE.Mesh(new THREE.BoxGeometry(1.62, 0.02, 0.03), MAT.blackMetal); rail.position.y = 0.065; bar.add(rail);
  for (let k = 0; k < 5; k++) {
    const x = (k - 2) * 0.32;
    tubes.push(new THREE.CapsuleGeometry(0.03, 0.2, 6, 12).rotateZ(Math.PI / 2).translate(x, 0, 0));
    const hang = new THREE.Mesh(new THREE.CylinderGeometry(0.003, 0.003, 0.06), MAT.blackMetal); hang.position.set(x, 0.035, 0); bar.add(hang);
    glow(ix + x, iy, iz);
  }
  bar.add(new THREE.Mesh(mergeGeometries(tubes), MAT.tube));
  for (const sx of [-0.7, 0.7]) { const cable = new THREE.Mesh(new THREE.CylinderGeometry(0.003, 0.003, WALL_H - iy - 0.065), MAT.blackMetal); cable.position.set(sx, 0.065 + (WALL_H - iy - 0.065) / 2, 0); bar.add(cable); }
  bar.position.set(ix, iy, iz); house.add(bar);
  cutMesh(bar, iy - 0.04, WALL_H, 'hide');
  for (const x of [-0.5, 0, 0.5]) giLamp(ix + x, iy, iz, 0.34, null, '#fff3e0', 0);
}
// indoor plants (pots stand clear of the walls; "reach" keeps the leaves inside the room)
paradisePlant(5.8, -3.55, 1.0, 0.36); figTree(2.45, -3.6, 1.0); paradisePlant(-0.98, -2.88, 0.9, 0.36);     // living & dining
figTree(5.75, 3.8, 0.95);                              // TV room
paradisePlant(1.3, 3.72, 0.8, 0.36);                   // entrance
paradisePlant(-2.0, -2.85, 0.75, 0.3);                 // kitchen
paradisePlant(-4.35, -3.86, 0.5, 0.2);                 // balcony

buildGlows();

/* ---------- finish static geometry ---------- */
buildFoliage('house', new THREE.MeshStandardMaterial({ roughness: 0.85, flatShading: true }));
buildFoliage('terrace', TM.leaf);
buildFoliage('site', new THREE.MeshStandardMaterial({ roughness: 0.85, flatShading: true }));
for (const b of STATIC_BATCHES) b.end();
cutMesh(B.upper.mesh, 1.5, 2.3, 'hide'); cutMesh(B.upperMetal.mesh, 1.5, 2.8, 'hide');
rebuildCut(WALL_H);
const TERRACE_SHADOW = [];
terrace.traverse((o) => { if (o.isMesh && o.castShadow) TERRACE_SHADOW.push(o); });

/* =====================================================================
   BAKED GLOBAL ILLUMINATION
   The soft interior light is computed once, in the browser, right after loading. Rays are
   traced on the CPU against a small BVH of the house to find how much daylight comes in
   through every window, how much lamp light reaches every surface, and how that light
   bounces between floor, walls, ceiling and furniture. The result is stored in
     · a lightmap atlas (floor 5 cm, wall faces 8 cm, ceiling 12 cm per texel), and
     · a 3D grid of light probes (every 0.35 m, six directions each) for furniture and decor.
   The sun stays a real-time light (crisp sun patches); its bounce is part of the bake.
   The work is cut into a few milliseconds per frame, so the view stays responsive while the
   lighting refines from a quick estimate to the final result.
   ===================================================================== */

/* ---------- 1. which materials take part ---------- */
// indoor materials read the baked light; anything in the garden or on the roof keeps the ordinary sky light
for (const t of TERRACE_MATS) if (!t.m.userData.gi) t.m.userData.gi = 'off';
{
  const indoor = new Set(), copies = new Map();
  house.traverse((o) => { if (o.isMesh) for (const m of [].concat(o.material)) indoor.add(m); });
  const outdoor = (m) => {
    if (!m.isMeshStandardMaterial) return m;
    if (!indoor.has(m)) { m.userData.gi = 'off'; return m; }
    if (!copies.has(m)) {                    // shared with the house → the garden gets its own copy
      const c = m.clone(); c.userData.gi = 'off';
      if (LAMP_MATS.includes(m)) LAMP_MATS.push(c);
      copies.set(m, c);
    }
    return copies.get(m);
  };
  site.traverse((o) => { if (o.isMesh) o.material = Array.isArray(o.material) ? o.material.map(outdoor) : outdoor(o.material); });
}

/* ---------- 2. shader hook: every indoor MeshStandardMaterial samples the baked light ---------- */
const VOL = { h: 0.35, ny: 9, x0: XW - 0.2, y0: PLINTH + 0.1, z0: ZB - 0.2 };     // light-probe grid
VOL.nx = Math.ceil((XE - XW + 0.4) / VOL.h) + 1; VOL.nz = Math.ceil((ZF - ZB + 0.4) / VOL.h) + 1; VOL.hy = (WALL_H - 0.2) / (VOL.ny - 1);
const GIU = {
  giAtlas: { value: null }, giVol: { value: null },
  giMix: { value: 0 },                                   // 0 = ordinary lighting, 1 = baked lighting
  giDayK: { value: new THREE.Vector3(1, 1, 1) }, giLampK: { value: 1 }, giSpec: { value: 0.25 },
  giV0: { value: new THREE.Vector3(VOL.x0, VOL.y0, VOL.z0) }, giVs: { value: new THREE.Vector3(1 / VOL.h, 1 / VOL.hy, 1 / VOL.h) },
  giVn: { value: new THREE.Vector3(VOL.nx, VOL.ny, VOL.nz) },
  // world x,z → atlas uv of the floor / ceiling map
  giMapF: { value: new THREE.Vector4(1 / (LM.floor.res * LM.W), 1 / (LM.floor.res * LM.H * 2), (LM.floor.x - LM.floor.x0 / LM.floor.res) / LM.W, (LM.floor.y - LM.floor.z0 / LM.floor.res) / (LM.H * 2)) },
  giMapC: { value: new THREE.Vector4(1 / (LM.ceil.res * LM.W), 1 / (LM.ceil.res * LM.H * 2), (LM.ceil.x - LM.ceil.x0 / LM.ceil.res) / LM.W, (LM.ceil.y - LM.ceil.z0 / LM.ceil.res) / (LM.H * 2)) },
};
{ // 1-texel placeholders until the first results are in
  const a = new THREE.DataTexture(new Uint16Array(4), 1, 1, THREE.RGBAFormat, THREE.HalfFloatType); a.needsUpdate = true;
  const v = new THREE.Data3DTexture(new Uint16Array(4), 1, 1, 1); v.format = THREE.RGBAFormat; v.type = THREE.HalfFloatType; v.needsUpdate = true;
  GIU.giAtlas.value = a; GIU.giVol.value = v;
}
const GI_PARS = `
uniform sampler2D giAtlas;
uniform highp sampler3D giVol;
uniform float giMix, giLampK, giSpec;
uniform vec3 giDayK, giV0, giVs, giVn;
uniform vec4 giMapF, giMapC;
#ifdef GI_WALL
	varying vec2 vGiUv;
#endif
// light probes: six colours per probe (one per axis direction), blended by the surface normal
vec3 giProbe( vec3 g, vec3 n, float layer ) {
	vec3 w = n * n;
	vec3 f = vec3( n.x < 0.0 ? 1.0 : 0.0, n.y < 0.0 ? 3.0 : 2.0, n.z < 0.0 ? 5.0 : 4.0 ) + layer;
	float gx = clamp( g.x, 0.5, giVn.x - 0.5 );
	vec2 yz = g.yz / giVn.yz;
	float k = 1.0 / ( 12.0 * giVn.x );
	return w.x * texture( giVol, vec3( ( gx + f.x * giVn.x ) * k, yz ) ).rgb
		+ w.y * texture( giVol, vec3( ( gx + f.y * giVn.x ) * k, yz ) ).rgb
		+ w.z * texture( giVol, vec3( ( gx + f.z * giVn.x ) * k, yz ) ).rgb;
}
`;
const GI_MAIN = `
{
	vec3 giP = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
	vec3 giN = normalize( ( vec4( geometryNormal, 0.0 ) * viewMatrix ).xyz );
	vec3 giD, giL;
	#if defined( GI_FLOOR ) || defined( GI_CEIL )
		#ifdef GI_FLOOR
			vec2 giUv = giP.xz * giMapF.xy + giMapF.zw;
		#else
			vec2 giUv = giP.xz * giMapC.xy + giMapC.zw;
		#endif
		giD = texture2D( giAtlas, giUv ).rgb;
		giL = texture2D( giAtlas, giUv + vec2( 0.0, 0.5 ) ).rgb;
	#else
		bool giProbes = true;
		#ifdef GI_WALL
			if ( vGiUv.x >= 0.0 ) {
				giD = texture2D( giAtlas, vGiUv ).rgb;
				giL = texture2D( giAtlas, vGiUv + vec2( 0.0, 0.5 ) ).rgb;
				giProbes = false;
			}
		#endif
		if ( giProbes ) {
			vec3 g = ( giP + giN * 0.18 - giV0 ) * giVs + 0.5;
			giD = giProbe( g, giN, 0.0 );
			giL = giProbe( g, giN, 6.0 );
		}
	#endif
	vec3 giE = giD * giDayK + giL * giLampK;
	irradiance = mix( irradiance, giE, giMix );
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_PMREM )
		iblIrradiance *= 1.0 - giMix;
		#if defined( RE_IndirectSpecular )
			// reflections are only as bright as the light that is around here
			radiance *= mix( 1.0, clamp( dot( giE, vec3( 0.3, 0.5, 0.2 ) ) * giSpec, 0.03, 1.5 ), giMix );
		#endif
	#endif
}
`;
THREE.MeshStandardMaterial.prototype.onBeforeCompile = function (shader) {
  const mode = this.userData.gi || 'probe';
  if (mode === 'off' || GI.failed) return;
  Object.assign(shader.uniforms, GIU);
  const def = mode === 'floor' ? '#define GI_FLOOR\n' : mode === 'ceil' ? '#define GI_CEIL\n' : mode === 'wall' ? '#define GI_WALL\n' : '';
  shader.fragmentShader = shader.fragmentShader
    .replace('#include <common>', def + '#include <common>\n' + GI_PARS)
    .replace('#include <lights_fragment_maps>', '#include <lights_fragment_maps>\n' + GI_MAIN);
  if (mode === 'wall') {
    shader.vertexShader = shader.vertexShader
      .replace('#include <common>', '#include <common>\nvarying vec2 vGiUv;')
      .replace('#include <project_vertex>', '#include <project_vertex>\n\tvGiUv = uv;');
  }
};
THREE.MeshStandardMaterial.prototype.customProgramCacheKey = function () { return 'gi-' + (GI.failed ? 'off' : this.userData.gi || 'probe'); };

/* ---------- 3. ray tracing: a compact triangle BVH (binned SAH build, stack traversal) ---------- */
class RayScene {
  // T: 9 floats per triangle (corner, edge 1, edge 2)
  constructor(T, n) {
    this.T = T; this.n = n; this.t = 0; this.rays = 0;
    const tb = new Float32Array(n * 6), cen = new Float32Array(n * 3), idx = this.idx = new Uint32Array(n);
    for (let i = 0; i < n; i++) {
      idx[i] = i;
      for (let k = 0; k < 3; k++) {
        const a = T[i * 9 + k], b = a + T[i * 9 + 3 + k], c = a + T[i * 9 + 6 + k];
        const lo = Math.min(a, b, c), hi = Math.max(a, b, c);
        tb[i * 6 + k] = lo; tb[i * 6 + 3 + k] = hi; cen[i * 3 + k] = (lo + hi) / 2;
      }
    }
    const cap = Math.max(2 * n, 2);
    const B = this.B = new Float32Array(cap * 6);         // node bounds
    const L = this.L = new Int32Array(cap);               // inner node: left child (right = left + 1); leaf: first triangle slot
    const C = this.C = new Int32Array(cap);               // inner node: -(split axis + 1); leaf: number of triangles
    const NB = 12, bc = new Int32Array(NB), bb = new Float32Array(NB * 6), lA = new Float32Array(NB), lN = new Int32Array(NB);
    const m = new Float32Array(6), cm = new Float32Array(6);
    const half = (b) => { const x = b[3] - b[0], y = b[4] - b[1], z = b[5] - b[2]; return x * y + y * z + z * x; };
    let nodes = 1;
    const stack = [0, 0, n];
    while (stack.length) {
      const hi = stack.pop(), lo = stack.pop(), nd = stack.pop();
      m[0] = m[1] = m[2] = cm[0] = cm[1] = cm[2] = Infinity; m[3] = m[4] = m[5] = cm[3] = cm[4] = cm[5] = -Infinity;
      for (let i = lo; i < hi; i++) {
        const t = idx[i];
        for (let k = 0; k < 3; k++) {
          if (tb[t * 6 + k] < m[k]) m[k] = tb[t * 6 + k];
          if (tb[t * 6 + 3 + k] > m[3 + k]) m[3 + k] = tb[t * 6 + 3 + k];
          const c = cen[t * 3 + k];
          if (c < cm[k]) cm[k] = c;
          if (c > cm[3 + k]) cm[3 + k] = c;
        }
      }
      B.set(m, nd * 6);
      const cnt = hi - lo;
      if (cnt <= 4) { L[nd] = lo; C[nd] = cnt; continue; }
      let bAx = -1, bSplit = 0, bCost = Infinity, bC0 = 0, bK = 0;
      for (let ax = 0; ax < 3; ax++) {
        const c0 = cm[ax], c1 = cm[3 + ax];
        if (c1 - c0 < 1e-5) continue;
        const k = NB / (c1 - c0);
        bc.fill(0);
        for (let b = 0; b < NB; b++) { bb[b * 6] = bb[b * 6 + 1] = bb[b * 6 + 2] = Infinity; bb[b * 6 + 3] = bb[b * 6 + 4] = bb[b * 6 + 5] = -Infinity; }
        for (let i = lo; i < hi; i++) {
          const t = idx[i];
          let b = ((cen[t * 3 + ax] - c0) * k) | 0;
          if (b >= NB) b = NB - 1;
          bc[b]++;
          for (let q = 0; q < 3; q++) {
            if (tb[t * 6 + q] < bb[b * 6 + q]) bb[b * 6 + q] = tb[t * 6 + q];
            if (tb[t * 6 + 3 + q] > bb[b * 6 + 3 + q]) bb[b * 6 + 3 + q] = tb[t * 6 + 3 + q];
          }
        }
        m[0] = m[1] = m[2] = Infinity; m[3] = m[4] = m[5] = -Infinity;      // sweep from the left …
        let cl = 0;
        for (let b = 0; b < NB - 1; b++) {
          for (let q = 0; q < 3; q++) { if (bb[b * 6 + q] < m[q]) m[q] = bb[b * 6 + q]; if (bb[b * 6 + 3 + q] > m[3 + q]) m[3 + q] = bb[b * 6 + 3 + q]; }
          cl += bc[b]; lN[b] = cl; lA[b] = cl ? half(m) : 0;
        }
        m[0] = m[1] = m[2] = Infinity; m[3] = m[4] = m[5] = -Infinity;      // … and from the right
        let cr = 0;
        for (let b = NB - 1; b > 0; b--) {
          for (let q = 0; q < 3; q++) { if (bb[b * 6 + q] < m[q]) m[q] = bb[b * 6 + q]; if (bb[b * 6 + 3 + q] > m[3 + q]) m[3 + q] = bb[b * 6 + 3 + q]; }
          cr += bc[b];
          if (!cr || !lN[b - 1]) continue;
          const cost = lA[b - 1] * lN[b - 1] + half(m) * cr;
          if (cost < bCost) { bCost = cost; bAx = ax; bSplit = b - 1; bC0 = c0; bK = k; }
        }
      }
      if (bAx < 0) { L[nd] = lo; C[nd] = cnt; continue; }                    // all centres coincide
      let i = lo, j = hi - 1;
      while (i <= j) {
        const t = idx[i];
        let b = ((cen[t * 3 + bAx] - bC0) * bK) | 0;
        if (b >= NB) b = NB - 1;
        if (b <= bSplit) i++; else { idx[i] = idx[j]; idx[j] = t; j--; }
      }
      const left = nodes; nodes += 2;
      L[nd] = left; C[nd] = -(bAx + 1);
      stack.push(left, lo, i, left + 1, i, hi);
    }
    this.nodes = nodes;
    this.st = new Int32Array(256); this.sd = new Float64Array(256);
  }
  // nearest triangle along origin + t·dir for t in (tmin, tmax): returns its index (or -1); the distance is left in this.t
  hit(ox, oy, oz, dx, dy, dz, tmin, tmax) {
    const B = this.B, L = this.L, C = this.C, idx = this.idx, T = this.T, st = this.st, sd = this.sd;
    const ix = 1 / (dx || 1e-12), iy = 1 / (dy || 1e-12), iz = 1 / (dz || 1e-12);
    const nx = dx < 0 ? 3 : 0, ny = dy < 0 ? 4 : 1, nz = dz < 0 ? 5 : 2, fx = 3 - nx, fy = 5 - ny, fz = 7 - nz;   // near / far plane of a box per axis
    let best = -1, tb = tmax, sp = 0;
    this.rays++;
    st[0] = 0; sd[0] = tmin; sp = 1;
    while (sp) {
      const nd = st[--sp];
      if (sd[sp] > tb) continue;                                   // a nearer hit was found meanwhile
      const c = C[nd];
      if (c >= 0) {
        for (let i = L[nd], e = i + c; i < e; i++) {
          const t = idx[i], q = t * 9;
          const e1x = T[q + 3], e1y = T[q + 4], e1z = T[q + 5], e2x = T[q + 6], e2y = T[q + 7], e2z = T[q + 8];
          const px = dy * e2z - dz * e2y, py = dz * e2x - dx * e2z, pz = dx * e2y - dy * e2x;
          const det = e1x * px + e1y * py + e1z * pz;
          if (det > -1e-12 && det < 1e-12) continue;
          const inv = 1 / det, sx = ox - T[q], sy = oy - T[q + 1], sz = oz - T[q + 2];
          const u = (sx * px + sy * py + sz * pz) * inv;
          if (u < 0 || u > 1) continue;
          const qx = sy * e1z - sz * e1y, qy = sz * e1x - sx * e1z, qz = sx * e1y - sy * e1x;
          const v = (dx * qx + dy * qy + dz * qz) * inv;
          if (v < 0 || u + v > 1) continue;
          const tt = (e2x * qx + e2y * qy + e2z * qz) * inv;
          if (tt > tmin && tt < tb) { tb = tt; best = t; }
        }
        continue;
      }
      // test both children here; push the ones that are hit, the nearer one on top
      const l = L[nd], o1 = l * 6, o2 = o1 + 6;
      let a1 = (B[o1 + nx] - ox) * ix, b1 = (B[o1 + fx] - ox) * ix, t = (B[o1 + ny] - oy) * iy;
      if (t > a1) a1 = t;
      t = (B[o1 + fy] - oy) * iy; if (t < b1) b1 = t;
      t = (B[o1 + nz] - oz) * iz; if (t > a1) a1 = t;
      t = (B[o1 + fz] - oz) * iz; if (t < b1) b1 = t;
      if (a1 < tmin) a1 = tmin;
      const h1 = a1 <= b1 && a1 <= tb;
      let a2 = (B[o2 + nx] - ox) * ix, b2 = (B[o2 + fx] - ox) * ix;
      t = (B[o2 + ny] - oy) * iy; if (t > a2) a2 = t;
      t = (B[o2 + fy] - oy) * iy; if (t < b2) b2 = t;
      t = (B[o2 + nz] - oz) * iz; if (t > a2) a2 = t;
      t = (B[o2 + fz] - oz) * iz; if (t < b2) b2 = t;
      if (a2 < tmin) a2 = tmin;
      const h2 = a2 <= b2 && a2 <= tb;
      if (h1 && h2) {
        if (a1 < a2) { st[sp] = l + 1; sd[sp++] = a2; st[sp] = l; sd[sp++] = a1; } else { st[sp] = l; sd[sp++] = a1; st[sp] = l + 1; sd[sp++] = a2; }
      } else if (h1) { st[sp] = l; sd[sp++] = a1; } else if (h2) { st[sp] = l + 1; sd[sp++] = a2; }
    }
    this.t = tb;
    return best;
  }
  // is anything in the way between tmin and tmax?
  occluded(ox, oy, oz, dx, dy, dz, tmin, tmax) {
    const B = this.B, L = this.L, C = this.C, idx = this.idx, T = this.T, st = this.st;
    const ix = 1 / (dx || 1e-12), iy = 1 / (dy || 1e-12), iz = 1 / (dz || 1e-12);
    const nx = dx < 0 ? 3 : 0, ny = dy < 0 ? 4 : 1, nz = dz < 0 ? 5 : 2, fx = 3 - nx, fy = 5 - ny, fz = 7 - nz;
    let sp = 1;
    this.rays++;
    st[0] = 0;
    while (sp) {
      const nd = st[--sp], c = C[nd];
      if (c >= 0) {
        for (let i = L[nd], e = i + c; i < e; i++) {
          const q = idx[i] * 9;
          const e1x = T[q + 3], e1y = T[q + 4], e1z = T[q + 5], e2x = T[q + 6], e2y = T[q + 7], e2z = T[q + 8];
          const px = dy * e2z - dz * e2y, py = dz * e2x - dx * e2z, pz = dx * e2y - dy * e2x;
          const det = e1x * px + e1y * py + e1z * pz;
          if (det > -1e-12 && det < 1e-12) continue;
          const inv = 1 / det, sx = ox - T[q], sy = oy - T[q + 1], sz = oz - T[q + 2];
          const u = (sx * px + sy * py + sz * pz) * inv;
          if (u < 0 || u > 1) continue;
          const qx = sy * e1z - sz * e1y, qy = sz * e1x - sx * e1z, qz = sx * e1y - sy * e1x;
          const v = (dx * qx + dy * qy + dz * qz) * inv;
          if (v < 0 || u + v > 1) continue;
          const tt = (e2x * qx + e2y * qy + e2z * qz) * inv;
          if (tt > tmin && tt < tmax) return true;
        }
        continue;
      }
      const l = L[nd], o1 = l * 6, o2 = o1 + 6;
      let a = (B[o1 + nx] - ox) * ix, b = (B[o1 + fx] - ox) * ix, t = (B[o1 + ny] - oy) * iy;
      if (t > a) a = t;
      t = (B[o1 + fy] - oy) * iy; if (t < b) b = t;
      t = (B[o1 + nz] - oz) * iz; if (t > a) a = t;
      t = (B[o1 + fz] - oz) * iz; if (t < b) b = t;
      if ((a > tmin ? a : tmin) <= (b < tmax ? b : tmax)) st[sp++] = l;
      a = (B[o2 + nx] - ox) * ix; b = (B[o2 + fx] - ox) * ix;
      t = (B[o2 + ny] - oy) * iy; if (t > a) a = t;
      t = (B[o2 + fy] - oy) * iy; if (t < b) b = t;
      t = (B[o2 + nz] - oz) * iz; if (t > a) a = t;
      t = (B[o2 + fz] - oz) * iz; if (t < b) b = t;
      if ((a > tmin ? a : tmin) <= (b < tmax ? b : tmax)) st[sp++] = l + 1;
    }
    return false;
  }
}

/* ---------- 4. the house as triangles (with the colour each one reflects) ---------- */
function giCollect() {
  const P = [], A = [], texCol = new Map(), k3 = new THREE.Color();
  const avgTex = (tex) => {                    // average colour of a (canvas) texture
    if (!tex || !tex.image) return [1, 1, 1];
    if (!texCol.has(tex)) {
      let c = [1, 1, 1];
      try {
        const g = canvas(8, 8)[1];
        g.drawImage(tex.image, 0, 0, 8, 8);
        const d = g.getImageData(0, 0, 8, 8).data;
        c = [0, 0, 0];
        for (let i = 0; i < d.length; i += 4) { k3.setRGB(d[i] / 255, d[i + 1] / 255, d[i + 2] / 255, THREE.SRGBColorSpace); c[0] += k3.r / 64; c[1] += k3.g / 64; c[2] += k3.b / 64; }
      } catch (e) { c = [0.7, 0.7, 0.7]; }
      texCol.set(tex, c);
    }
    return texCol.get(tex);
  };
  const v0 = new THREE.Vector3(), v1 = new THREE.Vector3(), v2 = new THREE.Vector3(), m4 = new THREE.Matrix4(), ic = new THREE.Color();
  const addRange = (geo, mat, matrix, start, end, tr, tg, tb) => {
    const pos = geo.attributes.position, idx = geo.index, col = mat.vertexColors ? geo.attributes.color : null;
    const t = avgTex(mat.map), r0 = mat.color.r * t[0] * tr, g0 = mat.color.g * t[1] * tg, b0 = mat.color.b * t[2] * tb;
    for (let i = start; i + 2 < end; i += 3) {
      const ia = idx ? idx.getX(i) : i, ib = idx ? idx.getX(i + 1) : i + 1, ic2 = idx ? idx.getX(i + 2) : i + 2;
      v0.fromBufferAttribute(pos, ia).applyMatrix4(matrix); v1.fromBufferAttribute(pos, ib).applyMatrix4(matrix); v2.fromBufferAttribute(pos, ic2).applyMatrix4(matrix);
      P.push(v0.x, v0.y, v0.z, v1.x - v0.x, v1.y - v0.y, v1.z - v0.z, v2.x - v0.x, v2.y - v0.y, v2.z - v0.z);
      if (col) A.push(r0 * col.getX(ia), g0 * col.getY(ia), b0 * col.getZ(ia)); else A.push(r0, g0, b0);
    }
  };
  house.traverseVisible((o) => {
    if (!o.isMesh) return;
    const geo = o.geometry, total = geo.index ? geo.index.count : geo.attributes.position.count;
    const d0 = geo.drawRange.start, d1 = Math.min(total, d0 + geo.drawRange.count);
    const parts = Array.isArray(o.material)
      ? geo.groups.map((g) => [o.material[g.materialIndex], Math.max(g.start, d0), Math.min(g.start + g.count, d1)])
      : [[o.material, d0, d1]];
    for (const [mat, s, e] of parts) {
      if (!mat || !mat.isMeshStandardMaterial || e <= s) continue;
      if (mat.transparent && mat.opacity < 0.9) continue;                 // glass lets the light through
      if (mat.emissive && mat.emissive.getHex() !== 0) continue;         // lamps are light sources, not blockers
      if (mat.userData.noBake) continue;
      if (o.isInstancedMesh) {
        for (let k = 0; k < o.count; k++) {
          o.getMatrixAt(k, m4); m4.premultiply(o.matrixWorld);
          if (o.instanceColor) o.getColorAt(k, ic); else ic.setRGB(1, 1, 1);
          addRange(geo, mat, m4, s, e, ic.r, ic.g, ic.b);
        }
      } else addRange(geo, mat, o.matrixWorld, s, e, 1, 1, 1);
    }
  });
  return { tris: new Float32Array(P), alb: new Float32Array(A), n: P.length / 9 };
}
const GI_ROOMS = [];                           // x0, z0, x1, z1, room index (flat, for speed)
ROOMS.forEach((r, k) => r.rects.forEach(([a, b, c, d]) => GI_ROOMS.push(X(a), Z(b), X(c), Z(d), k)));
function giRoomAt(x, z) {
  for (let i = 0; i < GI_ROOMS.length; i += 5) if (x >= GI_ROOMS[i] && x <= GI_ROOMS[i + 2] && z >= GI_ROOMS[i + 1] && z <= GI_ROOMS[i + 3]) return GI_ROOMS[i + 4];
  return -1;
}
const GI_OUT = new Float64Array(OUTLINE.flat());
function giInside(x, z) {                      // inside the building outline? (world x, z)
  let c = false;
  for (let i = 0, j = GI_OUT.length - 2; i < GI_OUT.length; j = i, i += 2) {
    const zi = GI_OUT[i + 1], zj = GI_OUT[j + 1];
    if ((zi > z) !== (zj > z) && x < ((GI_OUT[j] - GI_OUT[i]) * (z - zi)) / (zj - zi) + GI_OUT[i]) c = !c;
  }
  return c;
}
const giIndoors = (x, y, z) => y > PLINTH - 0.02 && y < PLINTH + WALL_H + 0.02 && giInside(x, z);

/* ---------- 5. the bake (a generator, so it can pause every few milliseconds) ---------- */
function* giBake() {
  const T0 = performance.now();
  // the house in its complete state: roof on, walls at full height, front door open
  const rk = roofK;
  GI.collecting = true; roofK = 1; applyRoof(); rebuildCut(WALL_H); scene.updateMatrixWorld(true);
  const geo = giCollect();
  GI.collecting = false; roofK = rk; applyRoof(); rebuildCut(cutH);
  const rs = new RayScene(geo.tris, geo.n), TRI = geo.tris, ALB = geo.alb;
  GI.tris = geo.n;
  yield;

  /* ----- sun, sky, the openings that let daylight in ----- */
  const sd = sun.position.clone().sub(sun.target.position).normalize(), sdx = sd.x, sdy = sd.y, sdz = sd.z;
  const SUN = [DAY.sunC.r * DAY.sun, DAY.sunC.g * DAY.sun, DAY.sunC.b * DAY.sun];
  const LS = 0.6 * GI.SKY, top = SKY_DAY.top, hor = SKY_DAY.horizon, PI = Math.PI;
  const GND = [0.30, 0.33, 0.24].map((a, k) => (a / PI) * (SUN[k] * sdy + PI * 0.36) * GI.SKY);     // sunlit lawn & paving seen through a window
  const smooth = (t) => (t <= 0 ? 0 : t >= 1 ? 1 : t * t * (3 - 2 * t));
  const L3 = new Float32Array(3);
  const skyRad = (dy) => {                       // radiance of the sky (or the ground) in a direction with height dy
    if (dy > 0) { const t = Math.pow(smooth(dy / 0.55), 0.8); L3[0] = (hor.r + (top.r - hor.r) * t) * LS; L3[1] = (hor.g + (top.g - hor.g) * t) * LS; L3[2] = (hor.b + (top.b - hor.b) * t) * LS; }
    else { const t = smooth(-dy / 0.2); L3[0] = hor.r * LS * (1 - t) + GND[0] * t; L3[1] = hor.g * LS * (1 - t) + GND[1] * t; L3[2] = hor.b * LS * (1 - t) + GND[2] * t; }
  };
  let Nx = 0, Ny = 0, Nz = 0;
  const triN = (t, dx, dy, dz) => {              // normal of triangle t, turned towards a ray arriving along (dx,dy,dz)
    const q = t * 9;
    let x = TRI[q + 4] * TRI[q + 8] - TRI[q + 5] * TRI[q + 7], y = TRI[q + 5] * TRI[q + 6] - TRI[q + 3] * TRI[q + 8], z = TRI[q + 3] * TRI[q + 7] - TRI[q + 4] * TRI[q + 6];
    const l = Math.sqrt(x * x + y * y + z * z) || 1;
    x /= l; y /= l; z /= l;
    if (x * dx + y * dy + z * dz > 0) { x = -x; y = -y; z = -z; }
    Nx = x; Ny = y; Nz = z;
  };
  const facesUp = (t) => TRI[t * 9 + 5] * TRI[t * 9 + 6] - TRI[t * 9 + 3] * TRI[t * 9 + 8] > 0;
  const extRad = (t, dx, dy, dz) => {            // an outdoor surface (balcony wall, canopy…) seen through an opening
    triN(t, dx, dy, dz);
    const sc = Math.max(0, Nx * sdx + Ny * sdy + Nz * sdz) * 0.5, up = 0.5 + 0.5 * Ny;
    for (let k = 0; k < 3; k++) L3[k] = (ALB[t * 3 + k] / PI) * ((SUN[k] * sc + PI * 0.36 * up) * GI.SKY + PI * GND[k] * (1 - up));
  };
  const portals = OPENINGS.filter((o) => o.kind === 'window' || o.main).map((o) => {
    const [px0, py0, px1, py1] = o.r, horiz = px1 - px0 >= py1 - py0;
    const lo = (o.kind === 'window' ? SILL : 0) + PLINTH, hi = (o.kind === 'window' ? HEAD : DOOR_H) + PLINTH;
    const a0 = horiz ? X(px0) : Z(py0), a1 = horiz ? X(px1) : Z(py1), c = horiz ? Z((py0 + py1) / 2) : X((px0 + px1) / 2);
    const neg = horiz ? inOutline((px0 + px1) / 2, py0 - 6) : inOutline(px0 - 6, (py0 + py1) / 2);     // indoors lies on the negative side?
    const w = a1 - a0, h = hi - lo;
    const grid = (nu, nv) => {
      const s = [];
      for (let i = 0; i < nu; i++) for (let j = 0; j < nv; j++) { const a = a0 + ((i + 0.5) / nu) * w, y = lo + ((j + 0.5) / nv) * h; s.push(horiz ? a : c, y, horiz ? c : a); }
      return { s: new Float32Array(s), dA: (w * h) / (nu * nv) };
    };
    return {
      nx: horiz ? 0 : neg ? -1 : 1, nz: horiz ? (neg ? -1 : 1) : 0, cx: horiz ? (a0 + a1) / 2 : c, cy: (lo + hi) / 2, cz: horiz ? c : (a0 + a1) / 2, size: Math.max(w, h),
      coarse: grid(2, 3), fine: grid(Math.max(3, Math.round(w / 0.3)), Math.max(4, Math.round(h / 0.42))),
    };
  });
  const acc = new Float32Array(18), tmp = new Float32Array(18);
  // light through one opening. cube = false: onto a surface with normal n → tmp[0..2]; cube = true: onto the six faces of a probe → tmp[0..17].
  // returns 0 (nothing arrives), 1 (every sample arrives) or 2 (partly shadowed)
  const samplePortal = (P, set, ox, oy, oz, nx, ny, nz, cube) => {
    const s = set.s, dA = set.dA;
    let vis = 0, cnt = 0;
    for (let i = 0; i < s.length; i += 3) {
      let wx = s[i] - ox, wy = s[i + 1] - oy, wz = s[i + 2] - oz;
      const d2 = wx * wx + wy * wy + wz * wz, d = Math.sqrt(d2);
      wx /= d; wy /= d; wz /= d;
      const cosP = -(P.nx * wx + P.nz * wz);
      if (cosP <= 0.02) continue;
      const cosX = cube ? 1 : nx * wx + ny * wy + nz * wz;
      if (cosX <= 0) continue;
      cnt++;
      const t = rs.hit(ox, oy, oz, wx, wy, wz, 1e-3, 1e9);
      if (t >= 0 && rs.t < d - 0.03) continue;                         // blocked on the way to the opening
      vis++;
      if (t < 0) skyRad(wy); else extRad(t, wx, wy, wz);
      const g = (cosP * dA) / Math.max(d2, 0.03);
      if (cube) {
        const fx = wx > 0 ? 0 : 3, fy = wy > 0 ? 6 : 9, fz = wz > 0 ? 12 : 15, ax = Math.abs(wx) * g, ay = Math.abs(wy) * g, az = Math.abs(wz) * g;
        tmp[fx] += L3[0] * ax; tmp[fx + 1] += L3[1] * ax; tmp[fx + 2] += L3[2] * ax;
        tmp[fy] += L3[0] * ay; tmp[fy + 1] += L3[1] * ay; tmp[fy + 2] += L3[2] * ay;
        tmp[fz] += L3[0] * az; tmp[fz + 1] += L3[1] * az; tmp[fz + 2] += L3[2] * az;
      } else { const k = cosX * g; tmp[0] += L3[0] * k; tmp[1] += L3[1] * k; tmp[2] += L3[2] * k; }
    }
    return cnt === 0 || vis === 0 ? 0 : vis === cnt ? 1 : 2;
  };
  // which openings can light which part of a room (a coarse map per room and opening; saves most of the rays)
  const CELL = 0.45;
  const roomPortals = ROOMS.map((room) => {
    let x0 = Infinity, z0 = Infinity, x1 = -Infinity, z1 = -Infinity;
    for (const [a, b, c, d] of room.rects) { x0 = Math.min(x0, X(a)); z0 = Math.min(z0, Z(b)); x1 = Math.max(x1, X(c)); z1 = Math.max(z1, Z(d)); }
    x0 -= 0.2; z0 -= 0.2;
    const gw = Math.ceil((x1 + 0.2 - x0) / CELL), gh = Math.ceil((z1 + 0.2 - z0) / CELL), list = [];
    for (const P of portals) {
      const s = P.coarse.s, raw = new Uint8Array(gw * gh);
      let any = false;
      for (let j = 0; j < gh; j++) for (let i = 0; i < gw; i++) {
        const x = x0 + (i + 0.5) * CELL, z = z0 + (j + 0.5) * CELL;
        if (!giInside(x, z)) continue;
        cell: for (const y of [0.06, 1.2, 2.6]) for (let k = 0; k < s.length; k += 3) {
          const wx = s[k] - x, wy = s[k + 1] - (PLINTH + y), wz = s[k + 2] - z, dd = Math.sqrt(wx * wx + wy * wy + wz * wz);
          if (!rs.occluded(x, PLINTH + y, z, wx / dd, wy / dd, wz / dd, 1e-3, dd - 0.03)) { raw[j * gw + i] = 1; any = true; break cell; }
        }
      }
      if (!any) continue;
      const mask = new Uint8Array(gw * gh);                      // … grown by one cell to be on the safe side
      for (let j = 0; j < gh; j++) for (let i = 0; i < gw; i++) {
        if (!raw[j * gw + i]) continue;
        for (let v = Math.max(0, j - 1); v <= Math.min(gh - 1, j + 1); v++) for (let u = Math.max(0, i - 1); u <= Math.min(gw - 1, i + 1); u++) mask[v * gw + u] = 1;
      }
      list.push({ P, mask });
    }
    return { x0, z0, gw, gh, list };
  });
  const skyAt = (room, ox, oy, oz, nx, ny, nz, cube, out) => {
    const n = cube ? 18 : 3, RP = room >= 0 ? roomPortals[room] : null, count = RP ? RP.list.length : portals.length;
    let cell = 0;
    if (RP) { const gi = Math.floor((ox - RP.x0) / CELL), gj = Math.floor((oz - RP.z0) / CELL); cell = gi < 0 || gj < 0 || gi >= RP.gw || gj >= RP.gh ? -1 : gj * RP.gw + gi; }
    for (let k = 0; k < count; k++) {
      let P = null;
      if (RP) { if (cell < 0 || !RP.list[k].mask[cell]) continue; P = RP.list[k].P; } else P = portals[k];
      const ex = ox - P.cx, ey = oy - P.cy, ez = oz - P.cz;
      if (ex * P.nx + ez * P.nz < 0.03) continue;                       // behind the opening
      const d2 = ex * ex + ey * ey + ez * ez;
      if (d2 > 200) continue;
      tmp.fill(0, 0, n);
      // probes: a few samples are enough. Surfaces: a few samples when far away and evenly lit (or evenly dark), otherwise the fine grid
      if (cube) samplePortal(P, P.coarse, ox, oy, oz, nx, ny, nz, true);
      else if (d2 < 1.7 || samplePortal(P, P.coarse, ox, oy, oz, nx, ny, nz, false) === 2) { tmp.fill(0, 0, n); samplePortal(P, P.fine, ox, oy, oz, nx, ny, nz, false); }
      for (let c = 0; c < n; c++) out[c] += tmp[c];
    }
  };

  /* ----- lamps ----- */
  const NL = GI_LAMPS.length, lampRoom = GI_LAMPS.map((l) => giRoomAt(l.x, l.z));
  const lw = new Float32Array(NL), ldx = new Float32Array(NL), ldy = new Float32Array(NL), ldz = new Float32Array(NL), ld = new Float32Array(NL), lv = new Float32Array(NL);
  const lid = new Int32Array(NL), lsame = new Uint8Array(NL), lt = new Uint8Array(NL);
  const lampsAt = (room, ox, oy, oz, nx, ny, nz, cube, out) => {
    let m = 0;
    for (let k = 0; k < NL; k++) {
      const l = GI_LAMPS[k], same = room >= 0 && lampRoom[k] === room;
      let wx = l.x - ox, wy = l.y - oy, wz = l.z - oz;
      const d2 = wx * wx + wy * wy + wz * wz;
      if (d2 > (same ? 64 : 9)) continue;                               // other rooms: only lamps within 3 m (light through a doorway)
      const d = Math.sqrt(d2);
      wx /= d; wy /= d; wz /= d;
      let w = 1;
      if (l.cone) { w = smooth((wy - l.cone[0]) / (l.cone[1] - l.cone[0])) + l.spill * smooth(wy / 0.2); if (w <= 0) continue; }
      if (!cube) { const c = nx * wx + ny * wy + nz * wz; if (c <= 0) continue; w *= c; }
      w /= Math.max(d2, 0.02);
      if (w < 0.003) continue;
      lw[m] = w; ldx[m] = wx; ldy[m] = wy; ldz[m] = wz; ld[m] = d; lid[m] = k; lsame[m] = same ? 1 : 0; m++;
    }
    // shadow rays: every lamp of another room and the three strongest of this room; the rest follow their average
    for (let i = 0; i < m; i++) lt[i] = lsame[i] ? 0 : 1;
    for (let pass = 0; pass < 3; pass++) {
      let b = -1, bw = 0;
      for (let i = 0; i < m; i++) if (!lt[i] && lw[i] > bw) { bw = lw[i]; b = i; }
      if (b < 0) break;
      lt[b] = 1;
    }
    let seen = 0, tot = 0;
    for (let i = 0; i < m; i++) {
      if (!lt[i]) continue;
      lv[i] = rs.occluded(ox, oy, oz, ldx[i], ldy[i], ldz[i], 1e-3, ld[i] - 0.03) ? 0 : 1;
      if (lsame[i]) { tot += lw[i]; seen += lw[i] * lv[i]; }
    }
    const f = tot > 0 ? seen / tot : 1;
    for (let i = 0; i < m; i++) {
      const w = lw[i] * (lt[i] ? lv[i] : f);
      if (w <= 0) continue;
      const l = GI_LAMPS[lid[i]];
      if (cube) {
        const fx = ldx[i] > 0 ? 0 : 3, fy = ldy[i] > 0 ? 6 : 9, fz = ldz[i] > 0 ? 12 : 15, ax = Math.abs(ldx[i]) * w, ay = Math.abs(ldy[i]) * w, az = Math.abs(ldz[i]) * w;
        out[fx] += l.r * ax; out[fx + 1] += l.g * ax; out[fx + 2] += l.b * ax;
        out[fy] += l.r * ay; out[fy + 1] += l.g * ay; out[fy + 2] += l.b * ay;
        out[fz] += l.r * az; out[fz + 1] += l.g * az; out[fz + 2] += l.b * az;
      } else { out[0] += l.r * w; out[1] += l.g * w; out[2] += l.b * w; }
    }
  };

  /* ----- light probes: direct light ----- */
  const NPX = VOL.nx, NPY = VOL.ny, NPZ = VOL.nz, NP = NPX * NPY * NPZ;
  const Vst = new Uint8Array(NP);                               // 0 = inside a solid, 1 = indoors, 2 = outdoors
  const Vsky = new Float32Array(NP * 18), Vlamp = new Float32Array(NP * 18), Vsun = new Float32Array(NP * 6);
  let ViD = new Float32Array(NP * 18), ViL = new Float32Array(NP * 18);
  const FN = [[1, 0, 0], [-1, 0, 0], [0, 1, 0], [0, -1, 0], [0, 0, 1], [0, 0, -1]];
  const indoorProbes = [];
  let steps = 0;
  for (let iz = 0; iz < NPZ; iz++) for (let iy = 0; iy < NPY; iy++) for (let ix = 0; ix < NPX; ix++) {
    const p = (iz * NPY + iy) * NPX + ix, x = VOL.x0 + ix * VOL.h, y = VOL.y0 + iy * VOL.hy, z = VOL.z0 + iz * VOL.h;
    if (!giInside(x, z)) {                                      // outdoors: the open sky, as the ordinary lighting has it
      Vst[p] = 2;
      for (let f = 0; f < 6; f++) { const up = 0.5 + 0.5 * FN[f][1]; Vsky[p * 18 + f * 3] = (DAY.gnd.r + (DAY.sky.r - DAY.gnd.r) * up) * DAY.hemi * 1.25; Vsky[p * 18 + f * 3 + 1] = (DAY.gnd.g + (DAY.sky.g - DAY.gnd.g) * up) * DAY.hemi * 1.25; Vsky[p * 18 + f * 3 + 2] = (DAY.gnd.b + (DAY.sky.b - DAY.gnd.b) * up) * DAY.hemi * 1.25; }
      continue;
    }
    const t = rs.hit(x, y, z, 0, 1, 0, 1e-4, 1e9);
    if (t >= 0 && facesUp(t)) continue;                         // inside something solid (a wall, a cabinet …)
    Vst[p] = 1; indoorProbes.push(p);
    const room = giRoomAt(x, z);
    acc.fill(0); skyAt(room, x, y, z, 0, 0, 0, true, acc); Vsky.set(acc, p * 18);
    acc.fill(0); lampsAt(room, x, y, z, 0, 0, 0, true, acc); Vlamp.set(acc, p * 18);
    if (!rs.occluded(x, y, z, sdx, sdy, sdz, 1e-3, 1e9)) for (let f = 0; f < 6; f++) Vsun[p * 6 + f] = Math.max(0, FN[f][0] * sdx + FN[f][1] * sdy + FN[f][2] * sdz);
    if ((++steps & 7) === 0) { GI.progress = 0.02 + 0.06 * (p / NP); yield; }
  }
  // probes inside solids borrow from their neighbours (indoor neighbours count most)
  const spread = (arr, stride) => {
    const done = new Uint8Array(NP);
    for (let p = 0; p < NP; p++) done[p] = Vst[p] ? 1 : 0;
    const NB6 = [1, -1, NPX, -NPX, NPX * NPY, -NPX * NPY], v = new Float32Array(stride);
    for (let pass = 0; pass < 3; pass++) {
      const add = [];
      for (let iz = 0; iz < NPZ; iz++) for (let iy = 0; iy < NPY; iy++) for (let ix = 0; ix < NPX; ix++) {
        const p = (iz * NPY + iy) * NPX + ix;
        if (done[p]) continue;
        let ws = 0; v.fill(0);
        for (let k = 0; k < 6; k++) {
          if ((k === 0 && ix === NPX - 1) || (k === 1 && ix === 0) || (k === 2 && iy === NPY - 1) || (k === 3 && iy === 0) || (k === 4 && iz === NPZ - 1) || (k === 5 && iz === 0)) continue;
          const q = p + NB6[k];
          if (!done[q]) continue;
          const w = Vst[q] === 2 ? 0.02 : 1;
          ws += w;
          for (let c = 0; c < stride; c++) v[c] += arr[q * stride + c] * w;
        }
        if (ws > 0) { for (let c = 0; c < stride; c++) arr[p * stride + c] = v[c] / ws; add.push(p); }
      }
      for (const p of add) done[p] = 1;
    }
  };
  spread(Vsky, 18); spread(Vlamp, 18); spread(Vsun, 6);
  yield;

  /* ----- light probes: bounced light ----- */
  const E6 = new Float32Array(6);
  const probeE = (x, y, z, nx, ny, nz, useInd) => {             // light arriving at a point (nearest probe) → E6 = day rgb, lamp rgb
    let ix = Math.round((x - VOL.x0) / VOL.h), iy = Math.round((y - VOL.y0) / VOL.hy), iz = Math.round((z - VOL.z0) / VOL.h);
    ix = ix < 0 ? 0 : ix >= NPX ? NPX - 1 : ix; iy = iy < 0 ? 0 : iy >= NPY ? NPY - 1 : iy; iz = iz < 0 ? 0 : iz >= NPZ ? NPZ - 1 : iz;
    const p = (iz * NPY + iy) * NPX + ix, o = p * 18, wx = nx * nx, wy = ny * ny, wz = nz * nz;
    const fx = nx > 0 ? 0 : 1, fy = ny > 0 ? 2 : 3, fz = nz > 0 ? 4 : 5;
    const sw = Vsun[p * 6 + fx] * wx + Vsun[p * 6 + fy] * wy + Vsun[p * 6 + fz] * wz;
    for (let k = 0; k < 3; k++) {
      const a = o + fx * 3 + k, b = o + fy * 3 + k, c = o + fz * 3 + k;
      E6[k] = Vsky[a] * wx + Vsky[b] * wy + Vsky[c] * wz + SUN[k] * sw;
      E6[3 + k] = Vlamp[a] * wx + Vlamp[b] * wy + Vlamp[c] * wz;
      if (useInd) { E6[k] += ViD[a] * wx + ViD[b] * wy + ViD[c] * wz; E6[3 + k] += ViL[a] * wx + ViL[b] * wy + ViL[c] * wz; }
    }
  };
  const NG = 48, FIB = new Float32Array(NG * 3);
  for (let i = 0; i < NG; i++) { const y = 1 - (2 * (i + 0.5)) / NG, r = Math.sqrt(1 - y * y), a = i * 2.39996323; FIB[i * 3] = Math.cos(a) * r; FIB[i * 3 + 1] = y; FIB[i * 3 + 2] = Math.sin(a) * r; }
  const NI = indoorProbes.length, hT = new Int32Array(NI * NG), hD = new Float32Array(NI * NG);
  for (let a = 0; a < NI; a++) {
    const p = indoorProbes[a], ix = p % NPX, iy = ((p / NPX) | 0) % NPY, iz = (p / (NPX * NPY)) | 0;
    const x = VOL.x0 + ix * VOL.h, y = VOL.y0 + iy * VOL.hy, z = VOL.z0 + iz * VOL.h;
    for (let i = 0; i < NG; i++) { hT[a * NG + i] = rs.hit(x, y, z, FIB[i * 3], FIB[i * 3 + 1], FIB[i * 3 + 2], 1e-3, 1e9); hD[a * NG + i] = rs.t; }
    if ((a & 3) === 3) { GI.progress = 0.08 + 0.24 * (a / NI); yield; }
  }
  const bounce = (useInd) => {                                   // one more bounce for every indoor probe
    const oD = new Float32Array(NP * 18), oL = new Float32Array(NP * 18), k4 = (4 * PI) / NG;
    for (let a = 0; a < NI; a++) {
      const p = indoorProbes[a], ix = p % NPX, iy = ((p / NPX) | 0) % NPY, iz = (p / (NPX * NPY)) | 0, o = p * 18;
      const x = VOL.x0 + ix * VOL.h, y = VOL.y0 + iy * VOL.hy, z = VOL.z0 + iz * VOL.h;
      for (let i = 0; i < NG; i++) {
        const t = hT[a * NG + i];
        if (t < 0) continue;
        const dx = FIB[i * 3], dy = FIB[i * 3 + 1], dz = FIB[i * 3 + 2], d = hD[a * NG + i], hx = x + dx * d, hy = y + dy * d, hz = z + dz * d;
        if (!giIndoors(hx, hy, hz)) continue;                    // light from outside is already counted through the openings
        triN(t, dx, dy, dz);
        probeE(hx + Nx * 0.18, hy + Ny * 0.18, hz + Nz * 0.18, Nx, Ny, Nz, useInd);
        const r = ALB[t * 3] / PI, g = ALB[t * 3 + 1] / PI, b = ALB[t * 3 + 2] / PI;
        const fx = dx > 0 ? 0 : 3, fy = dy > 0 ? 6 : 9, fz = dz > 0 ? 12 : 15, ax = Math.abs(dx) * k4, ay = Math.abs(dy) * k4, az = Math.abs(dz) * k4;
        oD[o + fx] += E6[0] * r * ax; oD[o + fx + 1] += E6[1] * g * ax; oD[o + fx + 2] += E6[2] * b * ax;
        oD[o + fy] += E6[0] * r * ay; oD[o + fy + 1] += E6[1] * g * ay; oD[o + fy + 2] += E6[2] * b * ay;
        oD[o + fz] += E6[0] * r * az; oD[o + fz + 1] += E6[1] * g * az; oD[o + fz + 2] += E6[2] * b * az;
        oL[o + fx] += E6[3] * r * ax; oL[o + fx + 1] += E6[4] * g * ax; oL[o + fx + 2] += E6[5] * b * ax;
        oL[o + fy] += E6[3] * r * ay; oL[o + fy + 1] += E6[4] * g * ay; oL[o + fy + 2] += E6[5] * b * ay;
        oL[o + fz] += E6[3] * r * az; oL[o + fz + 1] += E6[4] * g * az; oL[o + fz + 2] += E6[5] * b * az;
      }
    }
    spread(oD, 18); spread(oL, 18);
    return [oD, oL];
  };
  for (let b = 0; b < 3; b++) { [ViD, ViL] = bounce(b > 0); GI.progress = 0.32 + 0.01 * b; yield; }

  const half = THREE.DataUtils.toHalfFloat, hv = (v) => half(v > 0 ? (v < 60000 ? v : 60000) : 0), ONE = half(1);
  { // probes → 3D texture: 12 slabs side by side (6 directions × daylight / lamp light)
    const W = 12 * NPX, data = new Uint16Array(W * NPY * NPZ * 4);
    for (let iz = 0; iz < NPZ; iz++) for (let iy = 0; iy < NPY; iy++) for (let ix = 0; ix < NPX; ix++) {
      const p = (iz * NPY + iy) * NPX + ix;
      for (let f = 0; f < 6; f++) {
        const i = ((iz * NPY + iy) * W + f * NPX + ix) * 4, j = i + 6 * NPX * 4, o = p * 18 + f * 3;
        for (let k = 0; k < 3; k++) { data[i + k] = hv(Vsky[o + k] + ViD[o + k]); data[j + k] = hv(Vlamp[o + k] + ViL[o + k]); }
        data[i + 3] = data[j + 3] = ONE;
      }
    }
    const tex = new THREE.Data3DTexture(data, W, NPY, NPZ);
    tex.format = THREE.RGBAFormat; tex.type = THREE.HalfFloatType; tex.minFilter = tex.magFilter = THREE.LinearFilter; tex.needsUpdate = true;
    GIU.giVol.value = tex;
  }
  yield;

  /* ----- lightmap atlas ----- */
  const AW = LM.W, AH = LM.H, NT = AW * AH;
  const Ast = new Uint8Array(NT);                                // 0 = unused, 1 = lit, 2 = inside a solid
  const AD = new Float32Array(NT * 3), AL = new Float32Array(NT * 3), AiD = new Float32Array(NT * 3), AiL = new Float32Array(NT * 3);
  const regions = [
    { ...LM.floor, K: 5, n: [0, 1, 0], pos: (i, j, o) => { o[0] = LM.floor.x0 + (i + 0.5) * LM.floor.res; o[1] = PLINTH + 0.02; o[2] = LM.floor.z0 + (j + 0.5) * LM.floor.res; } },
    ...LM.strips.map((sp) => ({
      ...sp, K: 4, n: sp.horiz ? [0, 0, sp.sign] : [sp.sign, 0, 0],
      pos: (i, j, o) => { const a = sp.a0 + (i + 0.5) * sp.res, c = sp.c + sp.sign * 0.003; o[0] = sp.horiz ? a : c; o[1] = PLINTH + (j + 0.5) * sp.res; o[2] = sp.horiz ? c : a; },
    })),
    { ...LM.ceil, K: 3, n: [0, -1, 0], pos: (i, j, o) => { o[0] = LM.ceil.x0 + (i + 0.5) * LM.ceil.res; o[1] = PLINTH + WALL_H - 0.014; o[2] = LM.ceil.z0 + (j + 0.5) * LM.ceil.res; } },
  ];
  const V12 = new Float32Array(12);
  const probeLerp = (x, y, z, nx, ny, nz) => {                  // smooth probe lookup → V12 = sky, lamp, bounced day, bounced lamp
    let gx = (x - VOL.x0) / VOL.h, gy = (y - VOL.y0) / VOL.hy, gz = (z - VOL.z0) / VOL.h;
    gx = gx < 0 ? 0 : gx > NPX - 1.001 ? NPX - 1.001 : gx; gy = gy < 0 ? 0 : gy > NPY - 1.001 ? NPY - 1.001 : gy; gz = gz < 0 ? 0 : gz > NPZ - 1.001 ? NPZ - 1.001 : gz;
    const ix = gx | 0, iy = gy | 0, iz = gz | 0, tx = gx - ix, ty = gy - iy, tz = gz - iz;
    const wx = nx * nx, wy = ny * ny, wz = nz * nz, fx = (nx > 0 ? 0 : 1) * 3, fy = (ny > 0 ? 2 : 3) * 3, fz = (nz > 0 ? 4 : 5) * 3;
    V12.fill(0);
    for (let c = 0; c < 8; c++) {
      const w = (c & 1 ? tx : 1 - tx) * (c & 2 ? ty : 1 - ty) * (c & 4 ? tz : 1 - tz);
      if (w <= 0) continue;
      const o = (((iz + (c >> 2)) * NPY + iy + ((c >> 1) & 1)) * NPX + ix + (c & 1)) * 18;
      for (let k = 0; k < 3; k++) {
        const a = o + fx + k, b = o + fy + k, d = o + fz + k;
        V12[k] += w * (Vsky[a] * wx + Vsky[b] * wy + Vsky[d] * wz);
        V12[3 + k] += w * (Vlamp[a] * wx + Vlamp[b] * wy + Vlamp[d] * wz);
        V12[6 + k] += w * (ViD[a] * wx + ViD[b] * wy + ViD[d] * wz);
        V12[9 + k] += w * (ViL[a] * wx + ViL[b] * wy + ViL[d] * wz);
      }
    }
  };
  const q = new Float32Array(3);
  // a first estimate for every texel straight from the probes (soft, but already the right brightness and colour)
  for (const R of regions) {
    const [nx, ny, nz] = R.n;
    for (let j = 0; j < R.nv; j++) {
      for (let i = 0; i < R.nu; i++) {
        R.pos(i, j, q);
        const a = (R.y + j) * AW + R.x + i;
        if (q[1] > PLINTH + WALL_H - 0.005 || !giInside(q[0] + nx * 0.03, q[2] + nz * 0.03)) continue;
        const t = rs.hit(q[0] + nx * 0.004, q[1] + ny * 0.004, q[2] + nz * 0.004, 0, 1, 0, 1e-4, 1e9);
        if (t >= 0 && facesUp(t)) { Ast[a] = 2; continue; }
        Ast[a] = 1;
        probeLerp(q[0] + nx * 0.18, q[1] + ny * 0.18, q[2] + nz * 0.18, nx, ny, nz);
        for (let k = 0; k < 3; k++) { AD[a * 3 + k] = V12[k]; AL[a * 3 + k] = V12[3 + k]; AiD[a * 3 + k] = V12[6 + k]; AiL[a * 3 + k] = V12[9 + k]; }
      }
      if ((j & 3) === 3) yield;
    }
  }
  let atlasTex = null, atlasData = null;
  const upload = () => {
    const D = new Float32Array(NT * 3), Lm = new Float32Array(NT * 3), ok = new Uint8Array(NT);
    for (let a = 0; a < NT; a++) {
      if (Ast[a] !== 1) continue;
      ok[a] = 1;
      for (let k = 0; k < 3; k++) { D[a * 3 + k] = AD[a * 3 + k] + AiD[a * 3 + k]; Lm[a * 3 + k] = AL[a * 3 + k] + AiL[a * 3 + k]; }
    }
    for (let pass = 0; pass < 3; pass++) {                       // texels hidden inside solids / in the gutters copy their lit neighbours
      const add = [];
      for (let y = 1; y < AH - 1; y++) for (let x = 1; x < AW - 1; x++) {
        const a = y * AW + x;
        if (ok[a]) continue;
        let c = 0, d0 = 0, d1 = 0, d2 = 0, l0 = 0, l1 = 0, l2 = 0;
        for (let v = -1; v <= 1; v++) for (let u = -1; u <= 1; u++) {
          const b = a + v * AW + u;
          if (!ok[b]) continue;
          c++; d0 += D[b * 3]; d1 += D[b * 3 + 1]; d2 += D[b * 3 + 2]; l0 += Lm[b * 3]; l1 += Lm[b * 3 + 1]; l2 += Lm[b * 3 + 2];
        }
        if (c) { D[a * 3] = d0 / c; D[a * 3 + 1] = d1 / c; D[a * 3 + 2] = d2 / c; Lm[a * 3] = l0 / c; Lm[a * 3 + 1] = l1 / c; Lm[a * 3 + 2] = l2 / c; add.push(a); }
      }
      for (const a of add) ok[a] = 1;
    }
    if (!atlasData) atlasData = new Uint16Array(NT * 8);
    for (let a = 0; a < NT; a++) {
      const i = a * 4, j = (a + NT) * 4;
      atlasData[i] = hv(D[a * 3]); atlasData[i + 1] = hv(D[a * 3 + 1]); atlasData[i + 2] = hv(D[a * 3 + 2]); atlasData[i + 3] = ONE;
      atlasData[j] = hv(Lm[a * 3]); atlasData[j + 1] = hv(Lm[a * 3 + 1]); atlasData[j + 2] = hv(Lm[a * 3 + 2]); atlasData[j + 3] = ONE;
    }
    if (!atlasTex) {
      atlasTex = new THREE.DataTexture(atlasData, AW, AH * 2, THREE.RGBAFormat, THREE.HalfFloatType);
      atlasTex.minFilter = atlasTex.magFilter = THREE.LinearFilter;
      GIU.giAtlas.value = atlasTex;
    }
    atlasTex.needsUpdate = true;
    requestRender();
  };
  upload();
  GI.target = 1;                                                 // fade the baked light in; from here on it only gets sharper
  GI.envWanted++;
  GI.progress = 0.36;
  yield;

  // exact direct light for the texels: daylight through the openings + lamps.
  // Every second texel is computed first; a texel between neighbours that agree is simply their average
  // (smooth wall or floor), all others — shadow edges, pools of lamp light — are computed too.
  const direct = (R, i, j, a) => {
    const nx = R.n[0], ny = R.n[1], nz = R.n[2];
    R.pos(i, j, q);
    const room = giRoomAt(q[0] + nx * 0.03, q[2] + nz * 0.03), ox = q[0] + nx * 0.004, oy = q[1] + ny * 0.004, oz = q[2] + nz * 0.004;
    acc.fill(0, 0, 3); skyAt(room, ox, oy, oz, nx, ny, nz, false, acc);
    AD[a * 3] = acc[0]; AD[a * 3 + 1] = acc[1]; AD[a * 3 + 2] = acc[2];
    acc.fill(0, 0, 3); lampsAt(room, ox, oy, oz, nx, ny, nz, false, acc);
    AL[a * 3] = acc[0]; AL[a * 3 + 1] = acc[1]; AL[a * 3 + 2] = acc[2];
  };
  const lum = (a) => AD[a * 3] + AD[a * 3 + 1] + AD[a * 3 + 2] + AL[a * 3] + AL[a * 3 + 1] + AL[a * 3 + 2];
  const nb = new Int32Array(4);
  let texels = 0, doneTexels = 0;
  for (let a = 0; a < NT; a++) if (Ast[a] === 1) texels++;
  for (let r = 0; r < regions.length; r++) {
    const R = regions[r];
    for (let j = 0; j < R.nv; j += 2) for (let i = 0; i < R.nu; i += 2) {
      const a = (R.y + j) * AW + R.x + i;
      if (Ast[a] !== 1) continue;
      direct(R, i, j, a);
      if ((++doneTexels & 15) === 0) { GI.progress = 0.36 + 0.48 * (doneTexels / texels); yield; }
    }
    for (let j = 0; j < R.nv; j++) for (let i = 0; i < R.nu; i++) {
      if (!((i | j) & 1)) continue;
      const a = (R.y + j) * AW + R.x + i;
      if (Ast[a] !== 1) continue;
      doneTexels++;
      // the already computed neighbours on the even lattice
      let n = 0, okAll = true;
      for (let v = j & 1 ? -1 : 0; v <= (j & 1 ? 1 : 0) && okAll; v += 2) for (let u = i & 1 ? -1 : 0; u <= (i & 1 ? 1 : 0); u += 2) {
        const ii = i + u, jj = j + v;
        if (ii < 0 || jj < 0 || ii >= R.nu || jj >= R.nv || Ast[(R.y + jj) * AW + R.x + ii] !== 1) { okAll = false; break; }
        nb[n++] = (R.y + jj) * AW + R.x + ii;
      }
      if (okAll) {
        let lo = Infinity, hi = 0;
        for (let k = 0; k < n; k++) { const l = lum(nb[k]); if (l < lo) lo = l; if (l > hi) hi = l; }
        if (hi - lo <= 0.05 * hi + 0.01) {
          for (let c = 0; c < 3; c++) { let s = 0, t = 0; for (let k = 0; k < n; k++) { s += AD[nb[k] * 3 + c]; t += AL[nb[k] * 3 + c]; } AD[a * 3 + c] = s / n; AL[a * 3 + c] = t / n; }
          continue;
        }
      }
      direct(R, i, j, a);
      if ((doneTexels & 15) === 0) { GI.progress = 0.36 + 0.48 * (doneTexels / texels); yield; }
    }
    if (r === 0 || r === regions.length - 2 || r === regions.length - 1) { upload(); yield; }       // after the floor, the walls, the ceiling
  }

  // bounced light for the texels: gathered at every K-th texel, then interpolated
  const cosSets = {};
  const cosSet = (n) => {
    const key = n.join();
    if (!cosSets[key]) {
      const d = new Float32Array(48 * 3), rr = rng(4711);
      const t1 = n[1] ? [1, 0, 0] : [0, 1, 0], t2 = n[2] ? [1, 0, 0] : [0, 0, 1];
      for (let a = 0; a < 6; a++) for (let b = 0; b < 8; b++) {
        const u1 = (a + rr()) / 6, u2 = (b + rr()) / 8, r = Math.sqrt(u1), ph = 2 * PI * u2, lx = r * Math.cos(ph), ly = r * Math.sin(ph), lz = Math.sqrt(1 - u1), o = (a * 8 + b) * 3;
        for (let k = 0; k < 3; k++) d[o + k] = t1[k] * lx + (n[1] && k === 2 ? ly : n[1] ? 0 : t2[k] * ly) + n[k] * lz;
      }
      cosSets[key] = d;
    }
    return cosSets[key];
  };
  let blocks = 0, doneBlocks = 0;
  for (const R of regions) blocks += Math.ceil(R.nu / R.K) * Math.ceil(R.nv / R.K);
  for (const R of regions) {
    const K = R.K, cu = Math.ceil(R.nu / K), cv = Math.ceil(R.nv / K), [nx, ny, nz] = R.n, dirs = cosSet(R.n);
    const cD = new Float32Array(cu * cv * 3), cL = new Float32Array(cu * cv * 3), cOK = new Int8Array(cu * cv);   // cOK: 0 = no sample, else room + 2
    for (let bj = 0; bj < cv; bj++) {
      for (let bi = 0; bi < cu; bi++) {
        doneBlocks++;
        let si = -1, sj = -1, bd = 1e9;                           // a lit texel as close to the middle of the block as possible
        for (let j = bj * K; j < Math.min(R.nv, bj * K + K); j++) for (let i = bi * K; i < Math.min(R.nu, bi * K + K); i++) {
          if (Ast[(R.y + j) * AW + R.x + i] !== 1) continue;
          const d = (i - bi * K - (K - 1) / 2) ** 2 + (j - bj * K - (K - 1) / 2) ** 2;
          if (d < bd) { bd = d; si = i; sj = j; }
        }
        if (si < 0) continue;
        R.pos(si, sj, q);
        const ox = q[0] + nx * 0.004, oy = q[1] + ny * 0.004, oz = q[2] + nz * 0.004;
        let s0 = 0, s1 = 0, s2 = 0, s3 = 0, s4 = 0, s5 = 0;
        for (let k = 0; k < 48; k++) {
          const dx = dirs[k * 3], dy = dirs[k * 3 + 1], dz = dirs[k * 3 + 2], t = rs.hit(ox, oy, oz, dx, dy, dz, 1e-3, 1e9);
          if (t < 0) continue;
          const hx = ox + dx * rs.t, hy = oy + dy * rs.t, hz = oz + dz * rs.t;
          if (!giIndoors(hx, hy, hz)) continue;
          triN(t, dx, dy, dz);
          probeE(hx + Nx * 0.18, hy + Ny * 0.18, hz + Nz * 0.18, Nx, Ny, Nz, true);
          const r = ALB[t * 3] / PI, g = ALB[t * 3 + 1] / PI, b = ALB[t * 3 + 2] / PI;
          s0 += E6[0] * r; s1 += E6[1] * g; s2 += E6[2] * b; s3 += E6[3] * r; s4 += E6[4] * g; s5 += E6[5] * b;
        }
        const c = (bj * cu + bi) * 3, kk = PI / 48;
        cD[c] = s0 * kk; cD[c + 1] = s1 * kk; cD[c + 2] = s2 * kk; cL[c] = s3 * kk; cL[c + 1] = s4 * kk; cL[c + 2] = s5 * kk;
        cOK[bj * cu + bi] = giRoomAt(q[0] + nx * 0.03, q[2] + nz * 0.03) + 2;
        if ((doneBlocks & 3) === 0) { GI.progress = 0.84 + 0.15 * (doneBlocks / blocks); yield; }
      }
    }
    for (let j = 0; j < R.nv; j++) for (let i = 0; i < R.nu; i++) {        // interpolate (only between samples of the same room — no light leaks through walls)
      const a = (R.y + j) * AW + R.x + i;
      if (Ast[a] !== 1) continue;
      R.pos(i, j, q);
      const room = giRoomAt(q[0] + nx * 0.03, q[2] + nz * 0.03) + 2;
      const fx = (i + 0.5) / K - 0.5, fy = (j + 0.5) / K - 0.5, i0 = Math.floor(fx), j0 = Math.floor(fy), tx = fx - i0, ty = fy - j0;
      let ws = 0, d0 = 0, d1 = 0, d2 = 0, l0 = 0, l1 = 0, l2 = 0;
      for (let c = 0; c < 4; c++) {
        const ci = Math.min(cu - 1, Math.max(0, i0 + (c & 1))), cj = Math.min(cv - 1, Math.max(0, j0 + (c >> 1)));
        const cr = cOK[cj * cu + ci];
        if (!cr || (cr !== room && cr !== 1 && room !== 1)) continue;
        const w = (c & 1 ? tx : 1 - tx) * (c >> 1 ? ty : 1 - ty) + 1e-4, o = (cj * cu + ci) * 3;
        ws += w; d0 += cD[o] * w; d1 += cD[o + 1] * w; d2 += cD[o + 2] * w; l0 += cL[o] * w; l1 += cL[o + 1] * w; l2 += cL[o + 2] * w;
      }
      if (ws > 0) { AiD[a * 3] = d0 / ws; AiD[a * 3 + 1] = d1 / ws; AiD[a * 3 + 2] = d2 / ws; AiL[a * 3] = l0 / ws; AiL[a * 3 + 1] = l1 / ws; AiL[a * 3 + 2] = l2 / ws; }
    }
  }
  upload();
  GI.envWanted++;
  GI.progress = 1; GI.rays = rs.rays; GI.ms = Math.round(performance.now() - T0);
}

/* ---------- 6. reflections: a snapshot of the lit interior replaces the studio environment indoors ---------- */
// Polished floors, marble and glass then mirror the real room — windows, lamps, the timber ceiling.
const giCubeRT = new THREE.WebGLCubeRenderTarget(256, { type: THREE.HalfFloatType });
const giCubeCam = new THREE.CubeCamera(0.05, 60, giCubeRT);
function giReflections() {
  const rk = roofK;
  GI.collecting = true; roofK = 1; applyRoof(); rebuildCut(WALL_H);          // complete house, front door open
  for (const p of GLOW_PTS) p.visible = false;
  giCubeCam.position.set(2.15, PLINTH + 1.35, -1.25);                          // the middle of the living room
  giCubeCam.update(renderer, scene);
  for (const p of GLOW_PTS) p.visible = true;
  GI.collecting = false; roofK = rk; applyRoof(); rebuildCut(cutH);
  const env = pmrem.fromCubemap(giCubeRT.texture).texture;
  house.traverse((o) => {
    if (!o.isMesh) return;
    for (const m of [].concat(o.material)) if (m.isMeshStandardMaterial && m.userData.gi !== 'off') { m.envMap = env; m.envMapIntensity = 1; }
  });
  if (GI.env) GI.env.dispose();
  GI.env = env;
  GIU.giSpec.value = 1;
  requestRender();
}

/* ---------- 7. scheduling, fade-in and exposure ---------- */
const giCap = document.getElementById('giCap');
function giExposure() {
  // indoors the "camera" opens up, like eyes adapting when you walk inside
  renderer.toneMappingExposure = lerp(DAY.exp, NIGHT.exp, ease(nightK)) * lerp(1, GI.EXP_IN, GI.inside * GI.fade);
}
function giFrame(dt, busy) {
  let changed = false;
  if (GI.task) {
    const t0 = performance.now(), budget = busy ? 6 : 13;
    try {
      do { if (GI.task.next().done) { GI.task = null; break; } } while (performance.now() - t0 < budget);
    } catch (e) { console.warn('Lighting bake stopped', e); GI.task = null; GI.error = String(e); }
    giCap.hidden = !GI.task;
    if (GI.task) giCap.firstElementChild.textContent = Math.round(GI.progress * 100) + '%';
  }
  if (GI.fade !== GI.target) { GI.fade += Math.sign(GI.target - GI.fade) * Math.min(Math.abs(GI.target - GI.fade), dt / 1.2); GIU.giMix.value = GI.fade; changed = true; }
  const p = camera.position, inside = walking && level === 0 && p.y < PLINTH + WALL_H && inOutline(p.x / S + OX, p.z / S + OY) ? 1 : 0;
  if (GI.inside !== inside) { GI.inside += Math.sign(inside - GI.inside) * Math.min(Math.abs(inside - GI.inside), dt * 1.5); changed = true; }
  // refresh the reflection snapshot when the baked light has arrived / improved, and after day ↔ night
  const settled = nightK === 0 || nightK === 1;
  if (GI.envDone && settled && GI.envNight !== nightK) GI.envWanted++;
  if (GI.envWanted !== GI.envDone && GI.fade === 1 && settled && !GI.failed) {
    GI.envDone = GI.envWanted; GI.envNight = nightK;
    try { giReflections(); } catch (e) { console.warn('Reflection snapshot skipped', e); }
    changed = true;
  }
  if (changed) giExposure();
  return changed;
}

/* =====================================================================
   CAMERA, CONTROLS & VIEWS
   ===================================================================== */
const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;
controls.dampingFactor = 0.08;
controls.maxPolarAngle = deg(87);
controls.minDistance = 2.5;
controls.maxDistance = 95;
controls.screenSpacePanning = true;
controls.autoRotateSpeed = 2.4;

const V3 = (x, y, z) => new THREE.Vector3(x, y, z);
const boxPts = (x0, x1, y0, y1, z0, z1) => { const a = []; for (const x of [x0, x1]) for (const y of [y0, y1]) for (const z of [z0, z1]) a.push(V3(x, y, z)); return a; };
// key points of the building's silhouette (tighter framing than a bounding box)
const FOOT = [[-8.0, -4.6], [6.8, -4.6], [6.8, 4.9], [2.4, 7.15], [-1.6, 7.15], [-8.0, 4.4]];
const ROOFLINE = [[-6.8, -4.5], [6.5, -4.2], [6.5, 4.5], [2.35, 6.45], [-1.95, 6.45], [-6.8, 4.85]];
const EXT_PTS = [
  ...FOOT.map(([x, z]) => V3(x, 0, z)), ...ROOFLINE.map(([x, z]) => V3(x, 4.3, z)), V3(-6.8, 4.65, 4.85),
  ...[[1.2, 0.65], [6.2, 0.65], [1.2, 4.25], [6.2, 4.25]].map(([x, z]) => V3(x, 5.75, z)),
  V3(-5.95, 5.45, -3.3), V3(-4.75, 5.45, -2.05),
];
const FIT = {
  ext: EXT_PTS,
  front: [...EXT_PTS, ...boxPts(PL.x0, PL.x1, 0, 2.3, PL.z1 - 0.3, PL.z1 + 0.15)],
  house: [...FOOT.map(([x, z]) => V3(x, 0, z)), ...ROOFLINE.map(([x, z]) => V3(x, 3.35, z))],
  siteTop: boxPts(PL.x0 - 0.1, PL.x1 + 0.1, 0, 1.0, PL.z0 - 0.1, PL.z1 + 0.2),
  houseTop: boxPts(-8.05, 6.8, 0, 0.5, -4.6, 7.2),
};
const VIEWS = {
  front: { theta: 0, phi: deg(77), target: V3(-0.45, 1.9, 3.2) },
  back: { theta: Math.PI, phi: deg(68), target: V3(-0.62, 1.8, 1.0) },
  left: { theta: -Math.PI / 2, phi: deg(68), target: V3(-0.62, 1.8, 1.3) },
  right: { theta: Math.PI / 2, phi: deg(68), target: V3(-0.62, 1.8, 1.3) },
  top: { theta: 0, phi: 0.0005 },
  iso: { theta: deg(-32), phi: deg(61), target: V3(-0.62, 1.55, 1.3) },
};
function viewSpec(name) {
  const v = VIEWS[name];
  if (name === 'top') return state.terrace ? { ...v, target: V3((PL.x0 + PL.x1) / 2, 0.3, (PL.z0 + PL.z1) / 2), pts: FIT.siteTop } : { ...v, target: V3(-0.62, 0.3, 1.3), pts: FIT.houseTop };
  if (name === 'front') return { ...v, pts: FIT.front };
  return { ...v, pts: state.terrace ? FIT.ext : FIT.house };
}

const _cam = new THREE.PerspectiveCamera();
const _v = new THREE.Vector3();
function safeFrac() {
  const H = window.innerHeight || 1;
  const bar = document.getElementById('bar').getBoundingClientRect();
  const top = document.querySelector('.top').getBoundingClientRect();
  return { bottom: Math.min(0.45, (H - bar.top + 8) / H), top: Math.min(0.3, (top.bottom + 6) / H) };
}
function fitRadius(theta, phi, target, pts, margin = camera.aspect < 0.8 ? 0.985 : 0.94) {
  _cam.copy(camera); _cam.updateProjectionMatrix();
  const { bottom, top } = safeFrac();
  const yMin = -1 + bottom * 2, yMax = 1 - top * 2;
  const dir = new THREE.Vector3().setFromSphericalCoords(1, phi, theta);
  let lo = 0.5, hi = 400;
  for (let i = 0; i < 28; i++) {
    const r = (lo + hi) / 2;
    _cam.position.copy(target).addScaledVector(dir, r);
    _cam.lookAt(target);
    _cam.updateMatrixWorld(true);
    let ok = true;
    for (const p of pts) {
      _v.copy(p).project(_cam);
      if (_v.z > 1 || Math.abs(_v.x) > margin || _v.y < yMin || _v.y > yMax) { ok = false; break; }
    }
    if (ok) hi = r; else lo = r;
  }
  return hi;
}
let anim = null;
function flyTo(spec, dur = 1000) {
  const target = spec.target.clone();
  const radius = spec.radius ?? fitRadius(spec.theta, spec.phi, target, spec.pts);
  const sph = new THREE.Spherical().setFromVector3(camera.position.clone().sub(controls.target));
  let dT = spec.theta - sph.theta;
  dT = Math.atan2(Math.sin(dT), Math.cos(dT));
  const to = { target, r: radius, theta: sph.theta + dT, phi: spec.phi };
  if (dur <= 0) { applyCam(to.target, to.r, to.theta, to.phi); anim = null; requestRender(); return; }
  anim = { t0: performance.now(), dur, from: { target: controls.target.clone(), r: sph.radius, theta: sph.theta, phi: sph.phi }, to };
  requestRender();
}
function applyCam(target, r, theta, phi) {
  controls.target.copy(target);
  camera.position.setFromSphericalCoords(r, phi, theta).add(target);
  camera.lookAt(target);
}
function stepAnim(now) {
  if (!anim) return false;
  const k = Math.min(1, (now - anim.t0) / anim.dur), e = ease(k), f = anim.from, t = anim.to;
  _v.lerpVectors(f.target, t.target, e);
  applyCam(_v.clone(), f.r + (t.r - f.r) * e, f.theta + (t.theta - f.theta) * e, f.phi + (t.phi - f.phi) * e);
  if (k >= 1) { anim = null; controls.update(); }
  return true;
}

/* =====================================================================
   LABELS
   ===================================================================== */
const labelBox = document.getElementById('labels');
const labels = ROOMS.map((room) => {
  const el = document.createElement('button');
  el.className = 'label';
  el.textContent = room.name;
  labelBox.appendChild(el);
  const xs = [], zs = [];
  for (const [a, b, c, d] of room.rects) { xs.push(X(a), X(c)); zs.push(Z(b), Z(d)); }
  const L = { room, el, pos: new THREE.Vector3(X(room.at[0]), PLINTH + 0.9, Z(room.at[1])), box: [Math.min(...xs), Math.min(...zs), Math.max(...xs), Math.max(...zs)] };
  el.addEventListener('click', (ev) => { ev.stopPropagation(); focusRoom(L); });
  return L;
});
const labelOrder = labels.slice().sort((p, q) => {
  const area = (L) => L.room.rects.reduce((s, [a, b, c, d]) => s + (c - a) * (d - b), 0);
  return area(q) - area(p);
});
const _sph = new THREE.Spherical();
function updateLabels() {
  _sph.setFromVector3(_v.subVectors(camera.position, controls.target));
  const fade = THREE.MathUtils.clamp((deg(71) - _sph.phi) / deg(7), 0, 1);
  const show = !walking && !state.photo && state.labels && roofK < 0.05 && fade > 0.02;
  labelBox.style.opacity = show ? fade.toFixed(3) : '0';
  if (!show) { for (const L of labels) L.el.style.display = 'none'; return; }
  const W = window.innerWidth, H = window.innerHeight, placed = [];
  for (const L of labelOrder) {
    _v.copy(L.pos).project(camera);
    if (_v.z > 1 || Math.abs(_v.x) > 1.05 || Math.abs(_v.y) > 1.05) { L.el.style.display = 'none'; continue; }
    const x = ((_v.x + 1) / 2) * W, y = ((1 - _v.y) / 2) * H;
    if (!L.w) { L.el.style.display = ''; L.w = L.el.offsetWidth; L.h = L.el.offsetHeight; }
    const r = [x - L.w / 2 - 3, y - L.h / 2 - 2, x + L.w / 2 + 3, y + L.h / 2 + 2];
    if (placed.some((p) => r[0] < p[2] && r[2] > p[0] && r[1] < p[3] && r[3] > p[1])) { L.el.style.display = 'none'; continue; }
    placed.push(r);
    L.el.style.display = '';
    L.el.style.transform = `translate(${x.toFixed(1)}px, ${y.toFixed(1)}px) translate(-50%, -50%)`;
  }
}
function focusRoom(L) {
  const [x0, z0, x1, z1] = L.box;
  const target = new THREE.Vector3((x0 + x1) / 2, PLINTH + 0.4, (z0 + z1) / 2);
  const pts = [];
  for (const x of [x0, x1]) for (const z of [z0, z1]) for (const y of [PLINTH, PLINTH + 1.2]) pts.push(new THREE.Vector3(x, y, z));
  const sph = new THREE.Spherical().setFromVector3(camera.position.clone().sub(controls.target));
  const phi = deg(38);
  setActiveView(null);
  flyTo({ theta: sph.theta, phi, target, radius: Math.max(4, fitRadius(sph.theta, phi, target, pts, 0.8)) }, 900);
}

/* =====================================================================
   UI STATE — Terrace / Inside / Night / Labels / Spin
   ===================================================================== */
const state = { terrace: true, low: false, night: false, labels: true, spin: false, photo: false };
let cutH = WALL_H, roofK = 1, nightK = 0;
function syncButtons() {
  document.querySelectorAll('[data-toggle]').forEach((b) => {
    const on = !!state[b.dataset.toggle];
    b.classList.toggle('on', on);
    b.setAttribute('aria-pressed', on);
  });
}
function setToggle(key, val, instant = false) {
  if (key === 'photo') { setPhoto(val); return; }
  state[key] = val;
  if (key === 'terrace' && val) state.low = false;
  if (key === 'low' && val) state.terrace = false;
  controls.autoRotate = state.spin;
  if (instant) {
    cutH = state.low ? CUT_H : WALL_H; rebuildCut(cutH);
    roofK = state.terrace ? 1 : 0; applyRoof();
    nightK = state.night ? 1 : 0; applyNight(nightK);
  }
  syncButtons();
  requestRender();
}
function applyRoof() {
  const e = ease(roofK);
  terrace.visible = e > 0.002;
  terrace.position.y = (1 - e) * 3.2;
  for (const t of TERRACE_MATS) {
    const tr = t.trans || e < 0.999;
    if (t.m.transparent !== tr) { t.m.transparent = tr; t.m.needsUpdate = true; }
    t.m.opacity = t.base * e;
    if (!t.trans) t.m.depthWrite = e > 0.5;
  }
  for (const m of TERRACE_SHADOW) m.castShadow = e > 0.6;
}
const DAY = { sun: 2.35, hemi: 1.15, env: 0.55, exp: 1.0, sunC: C('#fff2e0'), sky: C('#eaf2ff'), gnd: C('#bcae94') };
const NIGHT = { sun: 0.24, hemi: 0.14, env: 0.06, exp: 1.0, sunC: C('#9fb3ff'), sky: C('#56669e'), gnd: C('#2a2830') };
function applyNight(k) {
  const e = ease(k);
  sun.intensity = lerp(DAY.sun, NIGHT.sun, e); sun.color.copy(DAY.sunC).lerp(NIGHT.sunC, e);
  hemi.intensity = lerp(DAY.hemi, NIGHT.hemi, e); hemi.color.copy(DAY.sky).lerp(NIGHT.sky, e); hemi.groundColor.copy(DAY.gnd).lerp(NIGHT.gnd, e);
  scene.environmentIntensity = lerp(DAY.env, NIGHT.env, e);
  giExposure();
  GIU.giDayK.value.set(lerp(1, 0.045, e), lerp(1, 0.055, e), lerp(1, 0.09, e));     // baked daylight fades to moonlight
  GIU.giLampK.value = lerp(GI.LAMP_DAY, 1, e);
  for (const key of ['top', 'horizon', 'ground']) skyU[key].value.copy(SKY_DAY[key]).lerp(SKY_NIGHT[key], e);
  scene.fog.color.copy(skyU.horizon.value);
  starMat.opacity = e; stars.visible = e > 0.01;
  for (const { l, on } of NIGHT_LIGHTS) l.intensity = on * e;
  matGlass.emissiveIntensity = 0.85 * e;
  for (const m of LAMP_MATS) m.emissiveIntensity = 2.6 * e;
}
function stepToggles(dt) {
  let changed = false;
  const tH = state.low ? CUT_H : WALL_H;
  if (cutH !== tH) {
    cutH += (tH - cutH) * (1 - Math.exp(-dt * 10));
    if (Math.abs(cutH - tH) < 0.005) cutH = tH;
    rebuildCut(cutH);
    changed = true;
  }
  const tR = state.terrace ? 1 : 0;
  if (roofK !== tR) { roofK += Math.sign(tR - roofK) * Math.min(Math.abs(tR - roofK), dt * 1.9); applyRoof(); changed = true; }
  const tN = state.night ? 1 : 0;
  if (nightK !== tN) { nightK += Math.sign(tN - nightK) * Math.min(Math.abs(tN - nightK), dt * 1.5); applyNight(nightK); changed = true; }
  if (changed) photo.dirty = true;          // the path-traced copy of the scene must be rebuilt
  return changed;
}
function setActiveView(name) {
  document.querySelectorAll('[data-view]').forEach((b) => b.classList.toggle('on', b.dataset.view === name));
}
document.querySelectorAll('[data-view]').forEach((b) => b.addEventListener('click', () => {
  setActiveView(b.dataset.view);
  flyTo(viewSpec(b.dataset.view));
}));
document.querySelectorAll('[data-toggle]').forEach((b) => b.addEventListener('click', () => setToggle(b.dataset.toggle, !state[b.dataset.toggle])));
const coarse = window.matchMedia && window.matchMedia('(pointer: coarse)').matches;
document.getElementById('hint').textContent = coarse
  ? 'Drag to rotate · Pinch to zoom · Tap a room'
  : 'Drag to rotate · Scroll to zoom · Right-drag to pan · Click a room to zoom in';

/* =====================================================================
   RENDER LOOP (renders only when something changes)
   ===================================================================== */
let needsRender = true;
function requestRender() { needsRender = true; }
controls.addEventListener('start', () => { anim = null; setActiveView(null); });
controls.addEventListener('change', () => {
  const t = controls.target, before = t.clone();
  t.x = THREE.MathUtils.clamp(t.x, -10, 9);
  t.z = THREE.MathUtils.clamp(t.z, -7, 12);
  t.y = THREE.MathUtils.clamp(t.y, 0, 5);
  camera.position.add(_v.subVectors(t, before));
  needsRender = true;
});
const composer = new EffectComposer(renderer);
composer.addPass(new RenderPass(scene, camera));
const gtao = new GTAOPass(scene, camera, window.innerWidth, window.innerHeight);
gtao.updateGtaoMaterial({ radius: 0.5, distanceExponent: 1.5, thickness: 1.0, scale: 1.2 });
composer.addPass(gtao);
composer.addPass(new OutputPass());
window.addEventListener('resize', () => {
  composer.setSize(window.innerWidth, window.innerHeight);
  for (const L of labels) L.w = 0;
  camera.aspect = window.innerWidth / window.innerHeight;
  if (walking) camera.fov = walkFov();
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight);
  requestRender();
});
// walk mode on a desktop adds screen-space ambient occlusion; phones skip that pass (the baked light already carries the soft shadows)
function renderRaster() { if (walking && !coarse) composer.render(); else renderer.render(scene, camera); }
let last = performance.now();
function loop(now) {
  requestAnimationFrame(loop);
  const dt = Math.min(0.05, (now - last) / 1000);
  last = now;
  if (touring) { tourT += dt; tourAt(tourT); if (tourT >= TOUR_LEN) stopTour(); }
  let changed = walking ? (touring || stepWalk(dt)) : stepAnim(now);
  if (!walking && !changed && controls.update(dt)) changed = true;
  const toggling = stepToggles(dt);
  if (toggling) changed = true;
  if (giFrame(dt, changed || state.photo)) changed = true;
  if (state.photo && !touring) { photoFrame(changed || needsRender, toggling); needsRender = false; return; }
  if (changed || needsRender) {
    renderRaster();
    drawMinimap();
    updateLabels();
    needsRender = false;
  }
}

/* =====================================================================
   WALK MODE — first-person tour through the rooms
   ===================================================================== */
var walking = false; let walkSpin = false; let yaw = 0, pitch = 0, level = 0;
const keys = {}, joy = { x: 0, y: 0 };
const EYE = 1.6, RAD = 22; // eye height (m), body radius (plan px)
const DOOR_RECTS = OPENINGS.filter((o) => o.kind === 'door').map((o) => o.r);
const inFurniture = (x, z, m = 0.16) => FURN_FP.some((f) => x > f[0] - m && x < f[2] + m && z > f[1] - m && z < f[3] + m);
function blocked(x, z, furn = true) {
  if (level > 0) { // terrace: stay inside the parapets (incl. portico top)
    const m = 0.35;
    if (x < XW + m || x > XE - m || z < ZN + m) return true;
    if (z > ZF - m) return !(x > PO.x0 + m && x < PO.x1 - m && z < PO.z1 - m);
    if (x > -6.8 && x < -4.2 + m && z > 3.9 - m) return true; // stone pier
    return false;
  }
  const px = x / S + OX, py = z / S + OY;
  if (px < -900 || px > 1900 || py < 700 || py > 2400) return true;
  for (const w of WALLS) {
    if (px < w[0] - RAD || px > w[2] + RAD || py < w[1] - RAD || py > w[3] + RAD) continue;
    const thr = DOOR_RECTS.some((d) => {
      const horiz = d[2] - d[0] > d[3] - d[1];
      return horiz ? px > d[0] + RAD * 0.7 && px < d[2] - RAD * 0.7 && py > d[1] - RAD - 2 && py < d[3] + RAD + 2
        : py > d[1] + RAD * 0.7 && py < d[3] - RAD * 0.7 && px > d[0] - RAD - 2 && px < d[2] + RAD + 2;
    });
    if (!thr) return true;
  }
  return furn && inFurniture(x, z);
}
const SPOTS = [
  { name: 'Garden', x: 0.2, z: 10.0, yaw: 0 },
  { name: 'Porch', x: 0.2, z: 7.6, yaw: 0 },
  ...ROOMS.map((r) => {
    const [a, b, c, d] = r.rects[0];
    const own = { Balcony: { x: X(216), z: Z(950), yaw: 0 }, Kitchen: { x: -2.6, z: -0.45, yaw: 0.6 }, 'TV Room': { x: 3.5, z: 3.85, yaw: 1.2 }, 'Living & Dining': { x: 0.3, z: -0.1, yaw: -1.15 } }[r.name];
    return { name: r.name, x: X((a + c) / 2), z: Z(d) - 0.7, yaw: 0, ...own };
  }),
];

// minimap: a blueprint of the plan (drawn once) with the camera position and view cone on top
const mini = document.getElementById('minimap'), mctx = mini.getContext('2d');
const MINI = { k: 0, ox: 0, oy: 0, base: null };
function miniBase() {
  const W = mini.width, H = mini.height, [c, g] = canvas(W, H);
  const k = MINI.k = Math.min((W - 28) / 1082, (H - 28) / 750), ox = MINI.ox = (W - 1082 * k) / 2, oy = MINI.oy = (H - 750 * k) / 2;
  const P = (px, py) => [ox + (px - 0) * k, oy + (py - 894) * k];
  const bg = g.createLinearGradient(0, 0, 0, H); bg.addColorStop(0, '#23447c'); bg.addColorStop(1, '#1a3463');
  g.fillStyle = bg; g.fillRect(0, 0, W, H);
  g.strokeStyle = 'rgba(255,255,255,0.045)'; g.lineWidth = 1;                // faint drawing grid
  for (let x = 0; x < W; x += 16) { g.beginPath(); g.moveTo(x + 0.5, 0); g.lineTo(x + 0.5, H); g.stroke(); }
  for (let y = 0; y < H; y += 16) { g.beginPath(); g.moveTo(0, y + 0.5); g.lineTo(W, y + 0.5); g.stroke(); }
  g.fillStyle = 'rgba(235,242,255,0.2)'; g.strokeStyle = 'rgba(240,246,255,0.95)'; g.lineWidth = 1.2;
  for (const [a, b, c2, d] of WALLS) { const [x, y] = P(a, b); g.fillRect(x, y, (c2 - a) * k, (d - b) * k); g.strokeRect(x, y, (c2 - a) * k, (d - b) * k); }
  for (const o of OPENINGS) {                                                // openings: gap + door swing / window line
    const [a, b, c2, d] = o.r, [x, y] = P(a, b), w = (c2 - a) * k, h = (d - b) * k, horiz = c2 - a >= d - b;
    g.fillStyle = '#1f3c70'; g.fillRect(x - (horiz ? 0 : 1), y - (horiz ? 1 : 0), w + (horiz ? 0 : 2), h + (horiz ? 2 : 0));
    g.strokeStyle = 'rgba(240,246,255,0.9)'; g.lineWidth = 1;
    if (o.kind === 'window' || o.slide) { g.beginPath(); if (horiz) { g.moveTo(x, y + h / 2); g.lineTo(x + w, y + h / 2); } else { g.moveTo(x + w / 2, y); g.lineTo(x + w / 2, y + h); } g.stroke(); continue; }
    if (o.main) continue;
    const len = (horiz ? w : h), hx = horiz ? (o.hinge === 'a' ? x : x + w) : (o.swing < 0 ? x : x + w), hy = horiz ? (o.swing < 0 ? y : y + h) : (o.hinge === 'a' ? y : y + h);
    const a0 = horiz ? (o.hinge === 'a' ? 0 : Math.PI) : (o.hinge === 'a' ? Math.PI / 2 : -Math.PI / 2);
    const a1 = horiz ? (o.swing < 0 ? -Math.PI / 2 : Math.PI / 2) : (o.swing < 0 ? Math.PI : 0);
    g.strokeStyle = 'rgba(240,246,255,0.55)';
    g.beginPath(); g.moveTo(hx, hy); g.lineTo(hx + Math.cos(a1) * len, hy + Math.sin(a1) * len); g.stroke();
    let d0 = a0, d1 = a1; if (Math.abs(d1 - d0) > Math.PI) { if (d1 > d0) d0 += 2 * Math.PI; else d1 += 2 * Math.PI; }
    g.beginPath(); g.arc(hx, hy, len, Math.min(d0, d1), Math.max(d0, d1)); g.stroke();
  }
  g.fillStyle = 'rgba(240,246,255,0.92)'; g.textAlign = 'center'; g.textBaseline = 'middle';
  g.font = `600 ${Math.max(8, Math.round(W / 46))}px system-ui, sans-serif`;
  for (const r of ROOMS) { const [x, y] = P(r.at[0], r.at[1] - (r.name === 'Balcony' ? 0 : 60)); g.fillText(r.name.toUpperCase(), x, y); }
  return c;
}
function drawMinimap() {
  if (!walking) return;
  if (!MINI.base) MINI.base = miniBase();
  const W = mini.width, H = mini.height, k = MINI.k;
  mctx.drawImage(MINI.base, 0, 0);
  const px = camera.position.x / S + OX, py = camera.position.z / S + OY, cx = MINI.ox + px * k, cy = MINI.oy + (py - 894) * k;
  const room = ROOMS.find((r) => r.rects.some(([a, b, c, d]) => px >= a && px <= c && py >= b && py <= d));
  if (room) {                                                                // the room you are in is highlighted
    mctx.fillStyle = 'rgba(190,210,245,0.2)';
    for (const [a, b, c, d] of room.rects) mctx.fillRect(MINI.ox + a * k, MINI.oy + (b - 894) * k, (c - a) * k, (d - b) * k);
  }
  const dir = new THREE.Vector3(); camera.getWorldDirection(dir);
  const a = Math.atan2(dir.z, dir.x), R = W * 0.16;
  const cone = mctx.createRadialGradient(cx, cy, 0, cx, cy, R); cone.addColorStop(0, 'rgba(255,196,92,0.75)'); cone.addColorStop(1, 'rgba(255,196,92,0)');
  mctx.fillStyle = cone; mctx.beginPath(); mctx.moveTo(cx, cy); mctx.arc(cx, cy, R, a - 0.55, a + 0.55); mctx.closePath(); mctx.fill();
  mctx.shadowColor = 'rgba(255,170,40,0.95)'; mctx.shadowBlur = W * 0.03;
  mctx.fillStyle = '#ffab2e'; mctx.beginPath(); mctx.arc(cx, cy, W * 0.017, 0, Math.PI * 2); mctx.fill();
  mctx.shadowBlur = 0;
  mctx.fillStyle = '#fff3d6'; mctx.beginPath(); mctx.arc(cx, cy, W * 0.007, 0, Math.PI * 2); mctx.fill();
}
function placeCam() {
  camera.position.set(walkX, PLINTH + (level ? TERR : 0) + EYE, walkZ);
  camera.rotation.set(pitch, yaw, 0, 'YXZ');
}
let walkX = 0, walkZ = 0;
function goSpot(s) { walkX = s.x; walkZ = s.z; yaw = s.yaw; pitch = -0.05; level = s.level || 0; placeCam(); requestRender();
  document.querySelectorAll('#walkui .spot').forEach((b) => b.classList.toggle('on', b.textContent === s.name)); }
// a wide lens indoors; wider still on an upright phone, so that enough of the room fits across the screen
function walkFov() { const a = window.innerWidth / window.innerHeight; return a >= 1.25 ? 70 : a >= 0.85 ? 78 : 84; }
function enterWalk() {
  walking = true; anim = null; controls.enabled = false;
  setToggle('terrace', true, true); setToggle('low', false, true); setToggle('spin', false);
  camera.fov = walkFov(); camera.updateProjectionMatrix();
  if (coarse) document.getElementById('walkhint').textContent = 'Drag to look · Joystick to walk' + (window.innerHeight > window.innerWidth ? ' · Turn the phone sideways for a wider view' : '');
  document.body.classList.add('walking');
  applyNight(nightK);
  rebuildCut(WALL_H);
  photo.dirty = true;
  goSpot(SPOTS[0]);
}
function exitWalk() {
  stopTour();
  walkSpin = false; document.getElementById('walkSpin').classList.remove('on');
  walking = false; controls.enabled = true;
  camera.fov = 40; camera.updateProjectionMatrix(); camera.up.set(0, 1, 0);
  document.body.classList.remove('walking');
  applyNight(nightK);
  rebuildCut(cutH);
  photo.dirty = true;
  const spec = viewSpec('iso'); controls.target.copy(spec.target);
  flyTo(spec, 1000); setActiveView('iso');
}
function stepWalk(dt) {
  let f = joy.y, r = joy.x;
  if (keys.KeyW || keys.ArrowUp) f += 1; if (keys.KeyS || keys.ArrowDown) f -= 1;
  if (keys.KeyD) r += 1; if (keys.KeyA) r -= 1;
  if (keys.ArrowLeft) yaw += dt * 1.8; if (keys.ArrowRight) yaw -= dt * 1.8;
  if (walkSpin) yaw += dt * 0.45;
  const turning = keys.ArrowLeft || keys.ArrowRight || walkSpin;
  if (!f && !r && !turning) return false;
  const sp = 1.6 * dt, sn = Math.sin(yaw), cs = Math.cos(yaw);
  const dx = (-sn * f + cs * r) * sp, dz = (-cs * f - sn * r) * sp;
  const furn = !inFurniture(walkX, walkZ);      // starting inside a piece (e.g. after a jump)? let the user walk out
  if (!blocked(walkX + dx, walkZ, furn)) walkX += dx;
  if (!blocked(walkX, walkZ + dz, furn)) walkZ += dz;
  placeCam();
  return true;
}
addEventListener('keydown', (e) => { if (walking) { keys[e.code] = true; if (e.code === 'Escape') exitWalk(); } });
addEventListener('keyup', (e) => { keys[e.code] = false; });
{ // drag to look around
  let drag = null;
  const el = renderer.domElement;
  el.addEventListener('pointerdown', (e) => { if (touring) stopTour(); if (walking) { drag = { x: e.clientX, y: e.clientY, id: e.pointerId }; } });
  addEventListener('pointermove', (e) => {
    if (!walking || !drag || e.pointerId !== drag.id) return;
    yaw -= (e.clientX - drag.x) * 0.0045; pitch -= (e.clientY - drag.y) * 0.0045;
    pitch = Math.max(-1.2, Math.min(1.2, pitch)); drag.x = e.clientX; drag.y = e.clientY;
    placeCam(); requestRender();
  });
  addEventListener('pointerup', (e) => { if (drag && e.pointerId === drag.id) drag = null; });
}
{ // walk UI: room chips, joystick, exit
  const ui = document.getElementById('walkui'), chips = document.getElementById('spots');
  for (const s of SPOTS) { const b = document.createElement('button'); b.className = 'spot'; b.textContent = s.name; b.onclick = () => goSpot(s); chips.appendChild(b); }
  document.getElementById('walkBtn').onclick = enterWalk;
  document.getElementById('exitWalk').onclick = exitWalk;
  document.getElementById('tourBtn').onclick = startTour;
  document.getElementById('tourBtn2').onclick = () => (touring ? stopTour() : startTour());
  const sb = document.getElementById('walkSpin');
  sb.onclick = () => { walkSpin = !walkSpin; sb.classList.toggle('on', walkSpin); requestRender(); };
  const pad = document.getElementById('joy'), knob = pad.firstElementChild;
  let jid = null;
  const set = (e) => {
    const r = pad.getBoundingClientRect(), R = r.width / 2;
    let x = (e.clientX - r.left - R) / R, y = (e.clientY - r.top - R) / R; const m = Math.hypot(x, y);
    if (m > 1) { x /= m; y /= m; }
    joy.x = x; joy.y = -y; knob.style.transform = `translate(${x * R * 0.6}px, ${y * R * 0.6}px)`;
  };
  pad.addEventListener('pointerdown', (e) => { jid = e.pointerId; pad.setPointerCapture(jid); set(e); e.stopPropagation(); });
  pad.addEventListener('pointermove', (e) => { if (e.pointerId === jid) set(e); });
  const end = () => { jid = null; joy.x = joy.y = 0; knob.style.transform = ''; };
  pad.addEventListener('pointerup', end); pad.addEventListener('pointercancel', end);
  ui.addEventListener('pointerdown', (e) => e.stopPropagation());
}

/* =====================================================================
   GUIDED TOUR — automatic walk through every room
   ===================================================================== */
// tour route: [x, z, lookX, lookY, lookZ] — passes through doorways and visits every room,
// then walks out and around the garden (the balcony has no door, so it is seen from outside)
const ROUTE = [
  [0.2, 9.2, 0.2, 1.9, 4.3], [0.2, 6.6, 0.2, 1.9, 4.0], [0.21, 5.0, 0.2, 1.8, 2.0], [0.21, 3.7, 0.2, 1.7, 1.0],
  // entrance
  [0.4, 2.6, 1.6, 1.4, 1.7], [0.1, 2.0, -1.6, 1.6, 1.8], [-0.75, 1.6, -0.75, 1.6, -1.0],
  // living & dining
  [-0.75, 0.20, 0.97, 1.30, -2.26], [0.40, -0.20, 3.12, 1.15, -1.47], [1.95, -0.30, 4.67, 1.10, -1.57], [1.98, -1.60, 4.98, 1.15, -1.60],
  [1.98, -2.70, 4.28, 1.35, -4.63], [1.98, -2.70, 0.95, 1.40, -5.52], [1.98, -2.60, -1.02, 1.10, -2.60], [1.98, -1.20, -0.84, 1.05, -0.17],
  [2.30, -0.15, 3.33, 1.30, 2.67], [2.30, -0.15, 5.12, 1.40, 0.88], [3.60, -0.05, 5.10, 1.00, -2.65], [4.90, -0.20, 3.87, 1.10, -3.02],
  [4.20, -0.08, 1.25, 1.30, -0.60], [2.95, -0.10, 1.92, 1.40, 2.72], [2.80, 0.20, 2.80, 1.50, 3.20],
  // TV room
  [2.8, 1.6, 2.1, 1.2, 3.0], [3.55, 2.05, 5.6, 1.2, 3.0], [4.0, 2.75, 4.2, 1.2, 4.6], [3.98, 3.85, 2.0, 1.3, 3.0],
  [4.0, 2.75, 2.8, 1.5, 1.0], [3.6, 2.0, 2.8, 1.6, 0.0],
  [2.8, 0.88, 2.8, 1.6, -0.8], [2.6, 0.0, -0.75, 1.6, 0.3], [0.4, 0.2, -0.75, 1.6, 1.5], [-0.75, 0.5, -0.75, 1.6, 2.0],
  // passage → kitchen
  [-0.6, 1.85, -2.5, 1.6, 1.85], [-1.53, 1.85, -3.0, 1.6, 1.85], [-2.4, 1.7, -2.55, 1.6, -0.5], [-2.55, 0.9, -2.55, 1.6, -1.0],
  [-2.55, 0.16, -3.5, 1.4, -1.5], [-2.6, -0.65, -4.4, 1.2, -2.0], [-2.3, -2.0, -4.4, 1.2, -2.6], [-3.0, -2.95, -4.0, 1.4, -3.6],
  [-3.7, -3.3, -3.7, 1.5, -4.6], [-3.7, -3.88, -4.3, 1.2, -4.3], [-3.75, -3.0, -5.6, 1.3, -2.0], [-5.25, -2.75, -5.9, 1.2, -1.2],
  [-5.25, -0.85, -4.0, 1.2, -0.2], [-4.0, -0.75, -2.0, 1.4, -0.5], [-3.3, -0.7, -2.3, 1.5, 0.3], [-2.8, -1.1, -2.4, 1.5, 0.8],
  [-2.5, -0.2, -2.55, 1.6, 1.5],
  // bathroom
  [-2.55, 1.0, -2.98, 1.6, 2.8], [-2.98, 2.0, -2.98, 1.5, 3.5], [-2.98, 2.62, -2.98, 1.4, 3.8], [-2.9, 3.3, -3.8, 1.0, 4.1],
  [-2.75, 3.25, -2.0, 1.3, 3.9], [-2.6, 3.1, -1.95, 1.4, 2.8], [-2.98, 2.9, -2.98, 1.6, 1.5],
  // store room
  [-2.98, 2.0, -3.6, 1.6, 1.42], [-3.1, 1.42, -4.5, 1.6, 1.42], [-3.66, 1.42, -5.0, 1.5, 1.8], [-4.6, 1.6, -6.0, 1.3, 1.0],
  [-5.0, 2.8, -6.1, 1.2, 1.0], [-4.75, 2.1, -4.0, 1.5, 1.2], [-4.7, 1.55, -3.0, 1.6, 1.42], [-3.66, 1.45, -2.4, 1.6, 1.45],
  // back out through the passage and entrance
  [-2.6, 1.55, -1.0, 1.6, 1.85], [-1.53, 1.85, 0.0, 1.6, 2.5], [-0.3, 2.6, 0.21, 1.6, 4.4], [0.21, 3.7, 0.21, 1.8, 6.0],
  // garden walk around the house (balcony seen through its window)
  [0.21, 5.2, -3.0, 1.7, 7.0], [-1.6, 7.3, -7.0, 1.6, 6.0], [-7.6, 6.0, -7.6, 1.8, 0.0], [-7.6, -1.0, -5.0, 1.6, -5.0],
  [-6.8, -5.4, -3.9, 1.6, -4.3], [-3.9, -5.6, -3.9, 1.4, -3.9], [0.0, -5.5, 4.0, 1.5, -4.2], [4.2, -5.4, 4.2, 1.6, -4.0],
  [7.4, -3.6, 7.4, 1.6, 3.0], [7.4, 4.0, 0.0, 1.8, 6.5], [4.0, 7.6, 0.0, 1.8, 4.4], [0.2, 9.2, 0.2, 2.0, 4.4],
];
// time per leg from walking distance and how far the view turns
const KF = [];
{
  let t = 0;
  ROUTE.forEach((w, i) => {
    if (i) {
      const v = ROUTE[i - 1], dist = Math.hypot(w[0] - v[0], w[1] - v[1]);
      const a0 = Math.atan2(v[2] - v[0], v[4] - v[1]), a1 = Math.atan2(w[2] - w[0], w[4] - w[1]);
      const turn = Math.abs(Math.atan2(Math.sin(a1 - a0), Math.cos(a1 - a0)));
      t += Math.max(dist / 0.9, turn / 0.8, 1.3);
    }
    KF.push([t, ...w]);
  });
}
const TOUR_LEN = KF[KF.length - 1][0];
const tourCurve = new THREE.CatmullRomCurve3(KF.map((k) => new THREE.Vector3(k[1], 0, k[2])), false, 'centripetal');
const _look = new THREE.Vector3(), _tl = new THREE.Vector3();
function tourAt(t) {
  t = Math.max(0, Math.min(TOUR_LEN, t));
  let i = 0; while (i < KF.length - 2 && t > KF[i + 1][0]) i++;
  const a = KF[i], b = KF[i + 1], u = (t - a[0]) / (b[0] - a[0]);
  const p = tourCurve.getPoint((i + u) / (KF.length - 1));
  const e = u * u * (3 - 2 * u);
  _tl.set(lerp(a[3], b[3], e), lerp(a[4], b[4], e), lerp(a[5], b[5], e));
  walkX = p.x; walkZ = p.z; level = 0;
  camera.position.set(p.x, PLINTH + EYE, p.z);
  _look.lerp(_tl, t === 0 ? 1 : 0.25);
  camera.lookAt(_tl);
  const d = new THREE.Vector3(); camera.getWorldDirection(d);
  yaw = Math.atan2(-d.x, -d.z); pitch = Math.asin(d.y);
  const room = ROOMS.find((r) => r.rects.some(([x0, y0, x1, y1]) => { const px = p.x / S + OX, py = p.z / S + OY; return px > x0 && px < x1 && py > y0 && py < y1; }));
  const cap = document.getElementById('tourCap');
  const indoors = inOutline(p.x / S + OX, p.z / S + OY) || (p.x > X(145) && p.x < X(288) && p.z > Z(915) && p.z < Z(993));
  if (cap) cap.textContent = room ? room.name : indoors ? cap.textContent : Math.abs(p.x - 0.2) < 2.2 && p.z > 4.4 && p.z < 7.2 ? 'Porch' : 'Garden';
  requestRender();
}
let touring = false, tourT = 0;
function startTour() { if (!walking) enterWalk(); touring = true; tourT = 0; walkSpin = false; document.body.classList.add('touring'); }
function stopTour() { touring = false; document.body.classList.remove('touring'); requestRender(); }

/* =====================================================================
   PHOTO MODE — progressive path tracing. While the camera is still, every
   frame adds samples of real light transport: sunlight and sky light
   coming through the glass, light bouncing between walls, floor and
   ceiling, soft contact shadows and true reflections in polished floors,
   marble and glass. Moving the camera shows the normal (fast) view again.
   ===================================================================== */
const photo = {
  pt: null, scene: null, dirty: true, building: false, still: 0, lightKey: '', exposure: 1,
  max: coarse ? 260 : 600, sky: null, instMats: new Map(), denoise: null, wantSave: false, failed: false,
};
const photoCap = document.getElementById('photoCap'), photoTxt = document.getElementById('photoTxt');
// clear glass for the path tracer (the raster view uses a tinted translucent stand-in)
const PHOTO_GLASS = new THREE.MeshPhysicalMaterial({ color: C('#f3f9f8'), transmission: 1, roughness: 0.02, ior: 1.5, metalness: 0, thickness: 0.01 });
PHOTO_GLASS.castShadow = false;           // path tracer flag: sun & sky light pass straight through the panes (faster, less noise)
// warm recessed downlights (photo mode only): small soft spots that shine down from the ceiling,
// so floors and furniture are lit and the walls get the familiar soft scallops
const ROOM_LIGHTS = [
  [-3.75, -2.05, 11], [0.5, -1.6, 11], [3.8, -1.9, 11], [2.6, -0.2, 7], [4.0, 2.7, 11], [0.25, 2.9, 9],
  [-2.6, 1.0, 7], [-2.85, 3.45, 7], [-4.95, 2.4, 7], [-4.6, -0.75, 8], [5.4, -3.2, 6], [-5.4, -2.6, 7],
].map(([x, z, k], i) => {
  const light = new PhysicalSpotLight(C('#ffe6c8'), 0, 0, 1.0, 0.7, 2);
  light.radius = 0.04;
  light.position.set(x, PLINTH + WALL_H - 0.03, z);
  light.target.position.set(x + 0.001, PLINTH, z);
  return { i, x, z, k, light };
});

function setPhotoText(t, canSave = false) {
  photoTxt.textContent = t;
  photoCap.hidden = !t;
  document.getElementById('photoSave').hidden = !canSave;
}
function syncPhotoButtons() {
  syncButtons();
  document.getElementById('photoBtn').classList.toggle('on', state.photo);
}
function setPhoto(on) {
  if (on && !initPathTracer()) {
    state.photo = false; syncPhotoButtons();
    setPhotoText('Photo mode needs WebGL 2 with float textures — not available here');
    setTimeout(() => setPhotoText(''), 3500);
    return;
  }
  state.photo = on;
  if (on) { photo.dirty = true; photo.still = 0; setPhotoText('Preparing photo mode…'); }
  else setPhotoText('');
  syncPhotoButtons();
  updateLabels();
  requestRender();
}
function initPathTracer() {
  if (photo.pt) return true;
  if (photo.failed || !renderer.capabilities.isWebGL2 || !renderer.extensions.has('EXT_color_buffer_float')) return false;
  try {
    const pt = new WebGLPathTracer(renderer);
    pt.tiles.set(coarse ? 3 : 2, coarse ? 3 : 2);        // spread each sample over several frames (stays responsive)
    pt.bounces = 5;
    pt.transmissiveBounces = 6;
    pt.filterGlossyFactor = 1.0;                          // tames fireflies from glossy bounces
    pt.minSamples = coarse ? 10 : 16;                     // keep the clean raster view until the photo is less noisy
    pt.renderDelay = 150;
    pt.fadeDuration = 450;
    pt.renderScale = Math.min(1, (coarse ? 1.0 : 1.3) / renderer.getPixelRatio());
    pt.textureSize.set(coarse ? 512 : 1024, coarse ? 512 : 1024);
    pt.rasterizeSceneCallback = () => renderRaster();
    // show the accumulated image through a gentle edge-aware denoiser that fades out as samples add up
    const dm = new DenoiseMaterial({ transparent: true });
    // extra step before the edge-aware blur: a lone pixel far brighter than all 8 neighbours is a firefly
    // (a rare light path that has not averaged out yet) — replace it with the neighbourhood mean
    dm.fragmentShader = dm.fragmentShader.replace('vec4 centrPx = texture2D( tex, uv );', `vec4 centrPx = texture2D( tex, uv );
					{
						vec2 texel = 1.0 / vec2( textureSize( tex, 0 ) );
						vec3 nMax = vec3( 0.0 ), nSum = vec3( 0.0 );
						for ( int j = - 1; j <= 1; j ++ ) {
							for ( int i = - 1; i <= 1; i ++ ) {
								if ( i == 0 && j == 0 ) continue;
								vec3 c = texture2D( tex, uv + vec2( float( i ), float( j ) ) * texel ).rgb;
								nMax = max( nMax, c );
								nSum += c;
							}
						}
						vec3 lumaW = vec3( 0.2126, 0.7152, 0.0722 );
						if ( dot( centrPx.rgb, lumaW ) > dot( nMax, lumaW ) * 1.5 + 0.02 ) centrPx.rgb = nSum / 8.0;
					}`);
    photo.denoise = new FullScreenQuad(dm);
    pt.renderToCanvasCallback = (target, r, quad) => {
      const n = pt.samples;
      dm.map = target.texture;
      dm.opacity = quad.material.opacity;
      dm.blending = quad.material.blending === THREE.NoBlending ? THREE.NoBlending : THREE.NormalBlending;
      dm.sigma = THREE.MathUtils.clamp(3.2 - n * 0.012, 1.2, 3.2);
      dm.threshold = THREE.MathUtils.clamp(0.2 - n * 0.001, 0.04, 0.2);
      const exp = r.toneMappingExposure, ac = r.autoClear;
      r.toneMappingExposure = photo.exposure; r.autoClear = false;
      photo.denoise.render(r);
      r.toneMappingExposure = exp; r.autoClear = ac;
    };
    photo.pt = pt;
    return true;
  } catch (e) {
    console.warn('Photo mode unavailable', e);
    photo.failed = true;
    return false;
  }
}
// copy of a batched geometry without its unused tail (the path tracer ignores draw ranges)
function trimGeometry(g) {
  const n = g.drawRange.count;
  if (!g.index || n === Infinity || n >= g.index.count) return g;
  if (n <= 0) return null;
  const idx = g.index.array.slice(g.drawRange.start, g.drawRange.start + n);
  let maxV = 0;
  for (let i = 0; i < idx.length; i++) if (idx[i] > maxV) maxV = idx[i];
  const out = new THREE.BufferGeometry();
  for (const k in g.attributes) {
    const a = g.attributes[k];
    out.setAttribute(k, new THREE.BufferAttribute(a.array.slice(0, (maxV + 1) * a.itemSize), a.itemSize, a.normalized));
  }
  out.setIndex(new THREE.BufferAttribute(idx, 1));
  return out;
}
function splitGroups(g, mats) {
  return g.groups.map((grp) => {
    const sub = new THREE.BufferGeometry();
    if (g.index) {
      for (const k in g.attributes) sub.setAttribute(k, g.attributes[k]);
      sub.setIndex(new THREE.BufferAttribute(g.index.array.slice(grp.start, grp.start + grp.count), 1));
    } else {
      for (const k in g.attributes) {
        const a = g.attributes[k];
        sub.setAttribute(k, new THREE.BufferAttribute(a.array.slice(grp.start * a.itemSize, (grp.start + grp.count) * a.itemSize), a.itemSize, a.normalized));
      }
    }
    return [sub, mats[grp.materialIndex]];
  });
}
function toRGBA(c) {
  const n = c.count, a = new Float32Array(n * 4);
  for (let i = 0; i < n; i++) { a[i * 4] = c.getX(i); a[i * 4 + 1] = c.getY(i); a[i * 4 + 2] = c.getZ(i); a[i * 4 + 3] = 1; }
  return new THREE.BufferAttribute(a, 4);
}
// instanced foliage → one merged mesh (instance colours become vertex colours)
function bakeInstances(im) {
  const parts = [], m4 = new THREE.Matrix4(), col = new THREE.Color();
  for (let i = 0; i < im.count; i++) {
    im.getMatrixAt(i, m4);
    m4.premultiply(im.matrixWorld);
    const g = im.geometry.clone().applyMatrix4(m4);
    if (im.instanceColor) {
      im.getColorAt(i, col);
      const n = g.attributes.position.count, a = new Float32Array(n * 4);
      for (let k = 0; k < n; k++) { a[k * 4] = col.r; a[k * 4 + 1] = col.g; a[k * 4 + 2] = col.b; a[k * 4 + 3] = 1; }
      g.setAttribute('color', new THREE.BufferAttribute(a, 4));
    }
    parts.push(g);
  }
  if (!parts.length) return null;
  let mat = im.material;
  if (im.instanceColor) {
    if (!photo.instMats.has(mat)) { const c2 = mat.clone(); c2.vertexColors = true; photo.instMats.set(mat, c2); }
    mat = photo.instMats.get(mat);
  }
  return new THREE.Mesh(mergeGeometries(parts), mat);
}
// sky for the path tracer: same gradient as the live sky plus a bright halo around the sun
function photoSky() {
  const tex = photo.sky || (photo.sky = new ProceduralEquirectTexture(512, 256));
  const top = skyU.top.value.clone(), hor = skyU.horizon.value.clone(), gnd = skyU.ground.value.clone();
  const sunDir = sun.position.clone().sub(sun.target.position).normalize();
  const day = 1 - ease(nightK), dir = new THREE.Vector3();
  tex.generationCallback = (polar, uv, coord, color) => {
    dir.setFromSpherical(polar);
    const h = dir.y;
    if (h > 0) color.copy(hor).lerp(top, Math.pow(THREE.MathUtils.smoothstep(h, 0, 0.55), 0.8));
    else color.copy(hor).lerp(gnd, THREE.MathUtils.smoothstep(-h, 0, 0.2)).multiplyScalar(0.6);
    const d = Math.max(0, dir.dot(sunDir)), halo = (Math.pow(d, 12) * 0.9 + Math.pow(d, 220) * 6) * day;
    color.r += halo; color.g += halo * 0.92; color.b += halo * 0.8;
  };
  tex.update();
  return tex;
}
function buildPhotoScene() {
  const ps = new THREE.Scene();
  scene.updateMatrixWorld(true);
  const swap = (m) => PHOTO_SWAP.get(m) || (m === matGlass || m === TM.glass ? PHOTO_GLASS : m);
  scene.traverseVisible((o) => {
    if (o.userData.noPhoto) return;
    if (o.isInstancedMesh) { const m = bakeInstances(o); if (m) ps.add(m); return; }
    if (!o.isMesh) return;
    let g = trimGeometry(o.geometry);
    if (!g) return;
    if (g.attributes.color && g.attributes.color.itemSize === 3) {   // the path tracer merges colours as RGBA
      if (g === o.geometry) g = g.clone();
      g.setAttribute('color', toRGBA(g.attributes.color));
    }
    // one material per mesh: the path tracer's merge mis-numbers materials of multi-material meshes
    const parts = Array.isArray(o.material) ? splitGroups(g, o.material) : [[g, o.material]];
    for (const [pg, pm] of parts) {
      const mat = swap(pm);
      if (!mat || !mat.isMeshStandardMaterial || (mat.transparent && mat.opacity < 0.02)) continue;
      const m = new THREE.Mesh(pg, mat);
      o.matrixWorld.decompose(m.position, m.quaternion, m.scale);
      ps.add(m);
    }
  });
  // the sun, plus whatever night lights are currently on
  const sl = new THREE.DirectionalLight(sun.color, sun.intensity * lerp(1.6, 1, ease(nightK)));   // crisper sun patches
  sl.position.copy(sun.position); sl.target.position.copy(sun.target.position);
  ps.add(sl, sl.target);
  NIGHT_LIGHTS.forEach(({ l }) => {
    if (l.intensity < 0.05) return;
    const c = l.clone();
    if (l.isSpotLight) { c.target = new THREE.Object3D(); c.target.position.copy(l.target.position); ps.add(c.target); }
    ps.add(c);
  });
  const sky = photoSky(), night = ease(nightK);
  ps.environment = sky; ps.background = sky;
  ps.environmentIntensity = ps.backgroundIntensity = lerp(1.0, 1.0, night);
  return ps;
}
// pick the room lights near the camera (cheap: no rebuild, only the light list changes)
function selectLights() {
  const ps = photo.scene;
  if (!ps) return;
  const cx = camera.position.x, cz = camera.position.z;
  const inside = terrace.visible && inOutline(cx / S + OX, cz / S + OY) && camera.position.y < PLINTH + WALL_H;
  const night = nightK > 0.5;
  photo.exposure = inside ? (night ? 1.8 : 3.0) : (night ? 1.5 : 0.72);   // interiors get a longer 'exposure', like a real photo
  const here = roomAt(cx, cz);               // lights in this room, plus any close by (through a doorway)
  // inside: this room's lights (+ any within a doorway's reach); outside at night: every room, so the windows glow
  const want = ROOM_LIGHTS.filter((L) => (inside ? (here && roomAt(L.x, L.z) === here) || Math.hypot(L.x - cx, L.z - cz) < 3.0 : night));
  const key = want.map((L) => L.i).join(',') + (night ? 'n' : 'd');
  if (key === photo.lightKey) return;
  photo.lightKey = key;
  for (const L of ROOM_LIGHTS) { L.light.removeFromParent(); L.light.target.removeFromParent(); }
  for (const L of want) { L.light.intensity = L.k * (night ? 1.4 : 0.9); ps.add(L.light, L.light.target); }
  ps.updateMatrixWorld(true);
  photo.pt.updateLights();
}
function roomAt(x, z) {
  const px = x / S + OX, py = z / S + OY;
  return ROOMS.find((r) => r.rects.some(([a, b, c, d]) => px > a && px < c && py > b && py < d)) || null;
}
function rebuildPhoto() {
  photo.building = true;
  setPhotoText('Preparing photo mode…');
  setTimeout(() => {                        // let the message paint before the (blocking) BVH build
    try {
      photo.scene = buildPhotoScene();
      photo.lightKey = '';
      photo.pt.setScene(photo.scene, camera);
      selectLights();
      photo.dirty = false;
    } catch (e) {
      console.error('Photo mode failed', e);
      photo.failed = true; photo.pt = null;
      setPhoto(false);
      setPhotoText('Photo mode could not start on this device');
      setTimeout(() => setPhotoText(''), 3500);
    }
    photo.building = false;
    requestRender();
  }, 40);
}
function photoFrame(moving, toggling) {
  const pt = photo.pt;
  if (!pt) return;
  if (moving) photo.still = 0; else photo.still++;
  if (photo.building || photo.dirty) {      // raster view until the path-traced copy is ready
    if (moving) { renderRaster(); drawMinimap(); updateLabels(); }
    if (photo.dirty && !photo.building && !moving && !toggling && photo.still > 2) rebuildPhoto();
    if (!photo.building && moving) setPhotoText('Hold still to render the photo');
    return;
  }
  if (moving) pt.updateCamera();            // restart accumulation from the new viewpoint
  if (photo.still === 2) selectLights();
  const n = Math.floor(pt.samples);
  if (moving || n < photo.max || photo.wantSave) {
    pt.renderSample();
    drawMinimap();
    if (moving) updateLabels();
  }
  if (photo.wantSave && n >= 1) { photo.wantSave = false; saveImage(); }
  if (moving) setPhotoText('Hold still to render the photo');
  else if (pt.isCompiling || n < 1) setPhotoText('Preparing photo mode…');
  else if (n < photo.max) setPhotoText(`Photo · refining ${Math.round((100 * n) / photo.max)}%`, true);
  else setPhotoText('Photo ready ✓', true);
}
function saveImage() {
  renderer.domElement.toBlob((blob) => {
    if (!blob) return;
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'home-photo.png';
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 4000);
  }, 'image/png');
}
document.getElementById('photoBtn').onclick = () => setPhoto(!state.photo);
document.getElementById('photoSave').onclick = (e) => { e.stopPropagation(); photo.wantSave = true; requestRender(); };

applyRoof();
applyNight(0);
// start computing the baked lighting shortly after the first frame is on screen
setTimeout(() => { GI.task = giBake(); requestRender(); }, 350);
// safety net: if a device cannot compile the lighting shader, fall back to the ordinary lighting
setTimeout(() => {
  if (!renderer.info.programs || !renderer.info.programs.some((p) => p.diagnostics && !p.diagnostics.runnable)) return;
  GI.failed = true; GI.task = null; GI.target = GI.fade = 0; GIU.giMix.value = 0; giCap.hidden = true;
  scene.traverse((o) => { if (o.isMesh) for (const m of [].concat(o.material)) m.needsUpdate = true; });
  giExposure(); requestRender();
}, 2500);
{ // intro: glide into the 3D elevation view
  const spec = viewSpec('iso');
  const r = fitRadius(spec.theta, spec.phi, spec.target, spec.pts);
  applyCam(spec.target, r * 1.6, spec.theta - 0.9, deg(32));
  flyTo(spec, 1700);
}
syncButtons();
requestAnimationFrame(loop);

window.home3d = {
  gi: () => ({ progress: GI.progress, baking: !!GI.task, ready: GI.fade, triangles: GI.tris, rays: GI.rays, ms: GI.ms, failed: GI.failed, error: GI.error }),
  view: (name, instant = false) => { setActiveView(name); flyTo(viewSpec(name), instant ? 0 : 1000); },
  set: (key, val, instant = true) => setToggle(key, val, instant),
  room: (name) => { const L = labels.find((l) => l.room.name === name); if (L) focusRoom(L); },
  walk: (name) => { if (!walking) enterWalk(); const sp = SPOTS.find((x) => x.name === name); if (sp) goSpot(sp); },
  tourAt: (t) => { if (!walking) enterWalk(); tourAt(t); },
  tourLen: TOUR_LEN,
  photo: (on = true) => setPhoto(on),
  photoSamples: () => (photo.pt ? photo.pt.samples : -1),
  photoState: () => ({ on: state.photo, dirty: photo.dirty, building: photo.building, samples: photo.pt ? photo.pt.samples : -1, compiling: photo.pt ? photo.pt.isCompiling : null, lights: photo.lightKey, exposure: photo.exposure }),
  cam: (x, z, yawDeg, pitchDeg = -6) => { if (!walking) enterWalk(); walkX = x; walkZ = z; yaw = deg(yawDeg); pitch = deg(pitchDeg); level = 0; placeCam(); requestRender(); },
  look: (p, t) => { anim = null; controls.target.set(t[0], t[1], t[2]); camera.position.set(p[0], p[1], p[2]); camera.lookAt(controls.target); setActiveView(null); requestRender(); },
};
