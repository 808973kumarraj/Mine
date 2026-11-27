---
name: motion-canvas-html
description: Write animations with the REAL Motion Canvas library (@motion-canvas/core + @motion-canvas/2d, TSX scenes, makeScene2D, createRef, signals, yield* flow, Layout, Code, Latex) and output them as ONE standalone .html file that plays offline with play/pause, scrubbing and WebM recording. No Vite project or editor needed. Use when the user asks for a Motion Canvas animation, a programmatic/explainer animation, an animated diagram or equation, or "animation as a single HTML file".
---

# Motion Canvas → one standalone HTML file

This skill runs the real Motion Canvas v3 engine (github.com/motion-canvas/motion-canvas).
You write normal Motion Canvas scenes in `.tsx`. `scripts/build.mjs` bundles them with
esbuild, together with `@motion-canvas/core` and `@motion-canvas/2d`, into one HTML
file with a built-in player. The file needs no server and no internet.

## Setup (once per machine)

```bash
cd <skill-dir>/scripts && npm install
```

## Workflow

1. Write one or more scene files (`.tsx`) using the normal Motion Canvas API.
   Start from `examples/hello.tsx` (basics) or `examples/features.tsx` (Layout, Code, Latex).
2. Build:
   ```bash
   node <skill-dir>/scripts/build.mjs intro.tsx main.tsx --out my-video.html \
     --title "My Video" --width 1920 --height 1080 --fps 60 --background "#141414"
   ```
   Several scene files play in order, the same as `makeProject({scenes:[…]})`.
   Scene transitions (`slideTransition`, `fadeTransition`…) work at the start of a scene.
3. Check: open the HTML in headless Chromium, wait ~1 s, read `#time` (shows the length),
   set `#seek` to a few frames and dispatch `input`, take screenshots, and confirm
   there are no `pageerror` or console errors. `window.mcPlayer` is the core `Player`.
4. Give the user the file path. Add `?autoplay` to the URL to start playing on load.

## Scene example

```tsx
import { makeScene2D, Circle, Rect, Txt } from '@motion-canvas/2d';
import { all, createRef, easeOutBack, waitFor } from '@motion-canvas/core';

export default makeScene2D(function* (view) {
  const box = createRef<Rect>();
  view.add(<Rect ref={box} width={0} height={240} radius={24} fill="#e6a700" />);
  yield* box().width(240, 0.8, easeOutBack);
  yield* all(box().rotation(180, 1), box().fill('#ff6470', 1));
  yield* waitFor(0.5);
});
```

All of the normal Motion Canvas API is available: every node from `@motion-canvas/2d`
(`Rect, Circle, Line, Spline, Polygon, Txt, Layout, Code, Latex, Img, Video, Icon, Path, Grid, Ray, Bezier…`)
and from `@motion-canvas/core` (`createRef, createSignal, all, any, chain, delay,
sequence, loop, waitFor, waitUntil, tween, easings, Vector2, Color, transitions, useRandom…`).
Follow motioncanvas.io/docs for the API.

## Differences from a normal Motion Canvas project

| Feature | In this skill |
|---|---|
| Editor UI (timeline, inspector) | Not included; the HTML has its own simple player |
| Project settings (`project.meta`, size, fps, background) | Use the `build.mjs` flags |
| Image/video/audio files (`import img from './a.png'`) | Not set up in the bundler. Use a `data:` URL or a full https URL in `src`. A full URL means the file is no longer fully offline |
| Audio track / voice-over sync | Not supported |
| `Code` syntax colours | Plain by default. For colours, `npm i @lezer/javascript` (or another Lezer parser) in `scripts/` and set `Code.defaultHighlighter = new LezerHighlighter(parser)` |
| `waitUntil('event')` time events | Work, but no editor to drag them, so they use their default time |
| Rendering to MP4 | Use the player's **● Rec** button (WebM, recorded in real time) |

## Size and rules

- A basic scene builds to ~200 KB. Using `Latex` adds MathJax and grows the file to ~2 MB.
- Keep scenes deterministic: use `useRandom()`, not `Math.random()`, so seeking is stable.
- Text ≥ 40 px, high-contrast colours; don't use colour alone to carry meaning.
- Never edit `templates/player.html` per scene; change only the `.tsx` files and the build flags.
