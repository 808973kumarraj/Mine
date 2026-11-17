# 3D Home — a single-file 3D viewer built from the owner's floor plan

This project turns one floor plan (`reference/floor-plan.jpg`) into **one self-contained HTML file**
(`dist/Home_3D_Minimalist_Concrete.html`). The file shows the house in 3D from every side, lets you
walk through each room, plays a guided tour, has a day/night switch, baked realistic interior lighting
and an optional path-traced "Photo" mode. Everything is drawn with code (three.js); there are no
model, image or texture files.

The owner opens the HTML on a **phone**. Judge every change in a phone-sized, upright (portrait)
viewport first, then in landscape.

## Rules from the owner — do not break these

1. **No Indian traditional or village-style elements.** No toran, rangoli, tulsi vrindavan, jhoola,
   jaali, Sintex tank, carved pillars, compound-wall-and-gate look, and so on. The owner rejected these
   several times. Keep everything modern / contemporary. The chosen style is **minimalist concrete**.
2. **The guided tour lives inside the same HTML file.** Never deliver it as a separate file.
3. **The HTML is plain, readable code.** No minified bundles, no base64, no embedded binary blobs,
   nothing downloaded at runtime (no CDN, no fetch). The libraries are bundled **unminified** into the
   file — the owner explicitly chose "readable three.js inside" over a CDN link.
4. **The floor plan is fixed.** Wall, door and window positions come from the owner's plan. Restyle
   freely, but do not move walls or openings unless the owner asks.
5. **Target look = the owner's reference video** (not stored here; ask the owner to attach it if you
   need to see it). It is a bright, photoreal interior walkthrough:
   - pale timber-board ceiling with many small glowing downlights; warm-white walls with soft light
     scallops; large cream floor tiles; black window frames
   - terracotta sofa with cream pillows, a console table behind it carrying two lit table lamps
   - chunky cream armchairs with teal pillows on a teal lattice rug (cream lines, thin yellow accents)
   - dark low coffee table with books, vases and succulents; fluted side table; black hourglass table
   - marble kitchen island with light-oak fronts, place settings, lemons, flowers, cream stools and a
     row of glowing tube pendants
   - abstract brush-stroke art, a black sunburst wall piece, bird-of-paradise and fig plants in black pots
   - TV wall with a linear fireplace
   - a navy "blueprint" minimap in the top-left with an orange dot and view cone
   The owner keeps asking for "more real, more exact like the video" — realism of light and materials
   matters more than adding features.
6. Write for a non-programmer owner: explain results in plain words, say what could not be tested.

## Build

```
npm install        # .npmrc sets legacy-peer-deps (three-gpu-pathtracer has an unused peer dependency)
npm run build      # → dist/Home_3D_Minimalist_Concrete.html  (about 2 MB, ~56,000 readable lines)
```

`build.mjs` does two things:
- bundles the libraries (three, three-gpu-pathtracer, three-mesh-bvh) with esbuild, **unminified**,
  into one script that exposes only what the app uses as `HOME3D_LIBS`, and turns the libraries'
  long one-line shader strings back into multi-line template literals;
- inserts `src/main.js` **verbatim** (comments included) after it, inside `src/template.html` at
  `/*__BUNDLE__*/`.

Because `main.js` is shipped verbatim, its comments are what the owner reads. Keep them accurate.
Library versions are pinned on purpose: the lighting hook patches three.js r187 shader chunks by name
(`#include <common>`, `<lights_fragment_maps>`, `<project_vertex>`).

## Files

- `src/main.js` — the whole app (one file, sections in the order below).
- `src/template.html` — page shell: CSS and the controls (view buttons, toggles, walk UI, minimap canvas).
- `build.mjs` — the readable single-file build.
- `tools/check.py` — headless-browser check (loads the page, waits for the lighting, tries the controls).
- `tools/shots.py` — screenshots from chosen camera positions, phone or desktop layout.
- `reference/floor-plan.jpg` — the owner's plan.

## Coordinates

- Plan data is in **pixels of the plan image**. `S = 0.012` m per pixel (doors are 66 px wide).
  `X(px) = (px - 541) * S`, `Z(py) = (py - 1269) * S`. +x is east, +z is south (the front / road side).
- The `house` group sits on a plinth: world y = house y + `PLINTH` (0.3 m). Floors are at house y = 0.
- `WALL_H = 2.8`, door height 2.1, window sill 0.12, window head 2.6.
- `WALLS` (16 rectangles), `OPENINGS` (doors, windows, the kitchen→balcony glass slider, the main door)
  and `ROOMS` (Kitchen, Living & Dining, Store Room, Passage, Bathroom, Entrance, TV Room, Balcony)
  are near the top of `main.js`.

## Sections of `src/main.js`

1. **Floor plan data, palette, renderer / sky / sun** — NeutralToneMapping, PCF shadows, a procedural sky dome.
2. **Batched geometry** — `Batch` packs thousands of vertex-coloured quads/boxes into one draw call
   (`B.wall`, `B.furn`, …). `fbox`/`pbox`/`cyl`/`soft`/`texBox`/`lathe` are the building helpers.
3. **Procedural textures** — every texture is drawn on a canvas (concrete, tiles, marble, timber, fabric, rugs, art).
4. **Walls** — meshed as a grid of solid/empty cells so no hidden faces exist. The "cut" system
   (`rebuildCut`, `cutMesh`, `TexBox`) re-cuts walls for the *Inside* (cut-away) toggle.
5. **Furniture and decor** — per room. Every solid piece registers a footprint in `FURN_FP`
   (walk-mode collision and the tour check use it).
6. **Site, night lights, interior finishes** — ceiling boards, downlights, rugs, art, pendants, plants.
7. **Baked global illumination** (see below).
8. **Camera/views, labels, UI state, render loop** — the loop renders only when something changed.
9. **Walk mode** — first person, joystick + drag (phone), WASD (desktop), collision against walls,
   door leaves and furniture, room "spots", the minimap.
10. **Guided tour** — `ROUTE` is a list of `[x, z, lookX, lookY, lookZ]` waypoints; timing is derived
    from distance and turn angle; about 163 s; visits every room, the balcony and the garden.
11. **Photo mode** — three-gpu-pathtracer (`WebGLPathTracer`) renders progressively while the camera is still.

Some blocks are switched off with `if (false) { … }` (stone pier/jaali, pergola, compound wall, hedge,
external stair, old terrace items). They are leftovers from rejected styles and never run.
**Delete them rather than re-enable them.**

## Lighting (the part that makes it look real)

All interior light is computed in the browser right after load — `giBake()` in the
*BAKED GLOBAL ILLUMINATION* section. It is a generator that runs a few milliseconds per frame.

- `giCollect()` turns the house into triangles (roof on, walls full height, front door open);
  `RayScene` is a small BVH ray tracer written for this (binned SAH build, stack traversal).
- **Daylight** enters through "portals" (windows + open main door), sampled with shadow rays.
  **Lamps** are listed in `GI_LAMPS` via `giLamp(x, y, z, candela, cone, colour, spill)` — every
  fixture you add needs one, or it will glow without lighting anything.
- Results: a **lightmap atlas** (`LM`: floor 5 cm, walls 8 cm, ceiling 12 cm per texel; daylight in the
  lower half of the texture, lamp light in the upper half) and a **3D grid of light probes**
  (`VOL`: 0.35 m, six directions per probe) for furniture and decor.
- The sun stays a real-time shadow-mapped light; only its bounce is baked.
- A hook on `MeshStandardMaterial.prototype.onBeforeCompile` makes every indoor material read the
  baked light. `material.userData.gi` selects the mode: `'floor'`, `'ceil'`, `'wall'` (lightmap),
  default = probes, `'off'` = ordinary sky light (outdoor things). Outdoor materials are switched to
  `'off'` automatically at the start of that section.
- `giReflections()` renders a cube snapshot of the lit living room and uses it as the reflection map
  indoors (refreshed after the bake and after day ↔ night).
- Exposure rises indoors (`GI.EXP_IN`) — the camera "adapts" when you walk in.
- Tuning knobs are in the `GI` object: `SKY` (daylight boost), `LAMP`, `LAMP_DAY`, `EXP_IN`.
- Foliage is excluded from baked shadows (`userData.noBake`) because it looks blotchy at lightmap resolution.
- If a device cannot compile the lighting shader, a safety net falls back to ordinary lighting.

Cost today: about 2 million rays, roughly 4 s of CPU on a desktop. A "Refining light %" pill shows
progress. Keep an eye on this number when adding geometry or lamps — the owner's phone is slower.

## Checking your work

```
pip install playwright            # once; the scripts use the system Chrome or Playwright's Chromium
python3 tools/check.py            # no page errors, lighting finishes, controls + tour + night work
python3 tools/shots.py m living:0.3,-0.1,-66,-8 kitchen:-2.5,-0.7,60,-10     # phone-layout screenshots
```

`window.home3d` is a small scripting API used by the tools: `view(name)`, `set(toggle, on)`,
`walk(room)`, `cam(x, z, yawDeg, pitchDeg)`, `tourAt(seconds)`, `tourLen`, `photo(on)`, `gi()`.
Yaw 0 looks north (−z), −90 looks east.

Always look at the screenshots yourself before telling the owner something looks right. In headless
Chrome without a GPU (software rendering) everything is slow — a full check takes a minute or two.

After moving furniture or changing `ROUTE`, verify the tour does not pass through anything: sample
`tourAt(t)` every 0.25 s and test the camera position against `FURN_FP`. Temporarily add to `window.home3d`:

```js
__dbg: () => ({ x: camera.position.x, z: camera.position.z, hit: inFurniture(camera.position.x, camera.position.z, 0.12) }),
__spots: () => SPOTS.filter((sp) => blocked(sp.x, sp.z)).map((sp) => sp.name),
```

When turning the camera in the tour, give the look target a steady distance (~3 m) and rotate it step
by step; a target that passes through the camera position makes the view swing wildly.

## Known gaps

- Never tested on a real phone — only in a phone-sized headless browser. Bake time and frame rate on
  the owner's device are unknown.
- It is closer to the reference video but not identical: the plan fixes room shapes and window sizes,
  and the furniture is built from simple coded shapes.
- The living room faces north, so it gets no direct sun; the TV room and entrance do.
- An older MP4 recording of the tour exists outside this project and shows the previous look.
