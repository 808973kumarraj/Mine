---
name: motion-canvas-html
description: Make code-driven animations in the style of Motion Canvas (generator scenes, signals, tweens, yield* flow) and output them as ONE standalone .html file that plays offline, with play/pause, scrubbing and WebM recording. No npm, Vite or build step needed. Use when the user asks for a Motion Canvas animation, a programmatic/explainer animation, an animated diagram, or "animation as a single HTML file".
---

# Motion Canvas → standalone HTML

Motion Canvas (github.com/motion-canvas/motion-canvas) describes animations as
TypeScript generator functions run inside a Vite editor. This skill keeps the same
way of writing scenes but swaps the toolchain for a small runtime that lives inside
one HTML file (`templates/template.html`). The output opens in any browser with no
server and no internet.

## Workflow

1. Copy `templates/template.html` to the output path (e.g. `my-scene.html`).
2. Replace only the code between `SCENE START` and `SCENE END`. Never edit the runtime.
3. Plan the scene as beats (each beat = one `yield*`). Aim for 5–30 s.
4. Check it: open the file in headless Chromium, read `#time` for the length, seek
   with `#seek` to 3–4 frames, take screenshots, and confirm there are no `pageerror`s.
5. Give the user the file path. Add `?autoplay` to the URL to start playing on load.

## The scene

```js
const { Rect, Circle, Txt, Line, Group, all, any, sequence, chain, delay, waitFor, loop, easing, signal } = MC;

MC.createPlayer({
  title: 'My Scene',      // page + download name
  fps: 60,                // optional
  maxSeconds: 60,         // cap for infinite loops
  scene: function* (view) {
    view.fill('#141414');                       // background
    const box = new Rect({ width: 200, height: 200, fill: '#e6a700', radius: 16 });
    view.add(box);
    yield* box.x(400, 1);                        // tween x to 400 over 1 s
    yield* all(box.rotation(90, 1), box.fill('#ff6470', 1));
  },
});
```

Canvas is 1920×1080 and **(0,0) is the centre**, +y goes down (as in Motion Canvas).

## Translating Motion Canvas code

| Motion Canvas | This runtime |
|---|---|
| `makeScene2D(function* (view) {…})` | `MC.createPlayer({ scene: function* (view) {…} })` |
| `const r = createRef<Rect>(); view.add(<Rect ref={r} …/>)` | `const r = new Rect({…}); view.add(r)` |
| `r().position.x(300, 1)` | `r.x(300, 1)` |
| `r().position([x,y], 1)` | `r.position([x,y], 1)` |
| `<Node><Circle/></Node>` | `new Group({ children: [new Circle({…})] })` or `g.add(c)` |
| `createSignal(0)` / `createSignal(() => a()*2)` | `signal(0)` / `signal(() => a()*2)` |
| `all, any, chain, delay, sequence, loop, waitFor` | same names, same meaning |
| `easeOutBack` etc. from `@motion-canvas/core` | `easing.easeOutBack` (see list below) |
| `r().size` on Circle | `size` (diameter) |
| `<Txt text="…"/>` typing effect | `t.text('New text', 1, easing.linear)` (erases then types) |
| `r().end(1, 1)` on Line/Circle | same: `start`/`end` from 0–1 draw the path or arc |
| `r().remove()` | `r.remove()` |

Not included (rebuild by hand if needed): flexbox `Layout`, `Code` blocks, `Latex`,
`Img`/`Video`, audio sync, camera, scene transitions, multiple scenes. For several
scenes, `yield*` them one after another inside the one generator.

## API

Every property is a **signal**:
- `node.prop()` → read
- `node.prop(v)` → set now
- `yield* node.prop(v, seconds, ease?)` → tween (default `easeInOutCubic`)

Numbers, `#hex` colours, arrays (`points`, `position`, `[sx,sy]` scale) and strings
(typewriter) all tween.

Shared props: `x, y, rotation (deg), scale (number or [sx,sy]), opacity, children`.

| Node | Props (defaults) |
|---|---|
| `Rect` | `width 100, height 100, fill '#fff', stroke, lineWidth 0, radius 0` |
| `Circle` | `size 100, fill '#fff', stroke, lineWidth 0, start 0, end 1` (pie/arc) |
| `Txt` | `text, fontSize 64, fill '#fff', fontFamily, fontWeight 600, align 'center'` (`\n` = new line) |
| `Line` | `points [[x,y],…], stroke '#fff', lineWidth 8, start 0, end 1, arrow false, closed false, fill` |
| `Group` | transform-only container |

Flow (all return generators; use with `yield*`):
- `all(...a)` run together, end when all end · `any(...a)` end when the first ends
- `chain(...a)` one after another · `delay(s, a)` · `sequence(gap, ...a)` staggered starts
- `waitFor(s)` · `loop(n, i => gen)`

Easings: `linear, easeInCubic, easeOutCubic, easeInOutCubic, easeInOutSine, easeOutBack, easeOutElastic, easeOutBounce`, or any `t => number`.

Custom node: `class Star extends MC.Node { constructor(p){ super(p, { r: 50, fill: '#fff' }); } draw(ctx){ ctx.beginPath(); /* path */ this.paint(ctx); } }`

## Rules

- Scenes must be **deterministic** (seeking replays from frame 0). For random values use a seeded RNG, never `Math.random()` inside the generator without a seed.
- Create nodes before you tween them; a tween captures its start value when it starts.
- Pass a generator (`box.x(1,1)`), not a function, to `all/sequence`; `chain` accepts both.
- Keep text ≥ 40 px and colour pairs high-contrast; do not rely on colour alone to carry meaning.
- Keep the file self-contained: no external scripts, images or fonts unless the user asks.
