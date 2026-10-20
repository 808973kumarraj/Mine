---
name: "explainer-html"
description: "Make an animated explainer as one self-contained interactive HTML page (play, pause, scrub, chapters, optional voice) instead of a rendered MP4 video."
---

# explainer-html: explainer videos that ship as a web page

Same method as the explainer-video skill (research, beats, scenes keyed to narration), but the
deliverable is ONE `.html` file the viewer plays in a browser. No MP4 render, no ffmpeg mux.
The page IS the video: a GSAP timeline driven by a player bar.

Core idea, unchanged: every animation is keyed to a **beat** (one line of narration), never to
raw seconds. Beats are timed at reading pace, or to measured voice clips when there is a voice.

## Workflow. Stop at the two checkpoints.
1. **Research.** Write `brief.md`: one row per claim that will be said or shown, each with a
   source link. Anything made up for illustration goes under an "Illustrative" heading. Pick an
   angle that existing explainers on the topic miss. Facts only from the brief.
2. **Beats.** Write the script as beats: `{id, ch, text, v, hold}` (one or two short sentences
   per beat, `v` = what is on screen). Group into chapters. Write the last line first. Show the
   user the script as a table (time, chapter, narration, on screen).
   **Checkpoint 1: the user approves the script before any scenes are built.**
3. **Scenes.** Build the page (see "Page contract"). One scene per idea; several beats can share
   a scene. Every tween is placed at `T(id)`, `E(id)` or `F(id, fraction)` of a beat.
4. **Critique loop.** Open the page headless (puppeteer-core) and seek the timeline to 0.35s after
   each beat starts and 0.45s before it ends; screenshot each. Also capture `pageerror` events:
   a JS error makes every frame look static. Score each frame 1-10 on legibility, composition,
   hierarchy, collisions, clarity with sound off, consistency. Fix the worst three, repeat until
   everything is 8+.
5. **Voice (optional).** Kokoro TTS per beat (`kokoro_onnx`, or `npx hyperframes tts`), read the
   clip durations, retime the beats, mix the clips into one track at the beat start times,
   loudnorm to about -16 LUFS, encode to mp3/m4a, and embed it as a base64 `data:` URI. Re-run the
   critique loop on the voiced timing. The user may record their own take instead.
6. **Deliver.** Publish the page as an Artifact (or send the `.html` file if they asked for the
   file). Also give `captions.srt`, `chapters.txt` and the `brief.md` sources.
   **Checkpoint 2: the user plays the page before anything is called done.**

## Timing (build time, in a small Node script that writes the HTML)
```js
// words/sec at reading pace; with voice, dur = measured clip length
let t = 0.4; for (const b of beats) { const w = b.text.split(/\s+/).length;
  b.dur = voice ? voice[b.id] : Math.max(1.6, w / 2.6); b.start = t; b.end = t + b.dur + (b.hold || 0); t = b.end + 0.4; }
const T = id => get(id).start, E = id => get(id).end, F = (id, f) => T(id) + get(id).dur * f;
```
Generate scene HTML, CSS and timeline calls as strings, then write one `index.html`.

## Page contract
- **One file.** Markup, CSS, JS, fonts (woff2 as base64 if custom), audio (base64) all inline.
  GSAP: load from cdnjs (`https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js`) for
  Artifacts; if the page must work offline or the CDN is blocked in the sandbox, inline
  `gsap.min.js` from `npm i gsap`. Test with the same copy you ship.
- **Stage:** a 16:9 box (`aspect-ratio:16/9; width:min(100vw, 177.78vh)`) with
  `container-type:size`; size everything in `cqw`/`cqh` so it scales to any window and phone.
  For Shorts-style pages use 9:16.
- **Timeline:** `const tl = gsap.timeline({paused:true})`. Scenes are absolutely positioned
  layers; each fades in at its first beat and out at its last (`autoAlpha`).
- **Player bar** under the stage: play/pause button (and Space key), a scrubber `input[type=range]`
  bound to `tl.progress()`, current time / total, chapter markers on the scrubber that jump to a
  chapter, a subtitles toggle (CC), and a speed control (0.75x / 1x / 1.25x / 1.5x).
- **Audio sync:** the `<audio>` element is the clock when there is a voice. On every
  `requestAnimationFrame`, `tl.time(audio.currentTime)`. Play/pause/seek/rate act on the audio.
  Without voice, use `tl.play()` and `tl.timeScale(rate)`.
- **Subtitles:** chunks of about 7 words per beat, shown in a caption pill at the stage bottom,
  driven from the same time.
- **Chrome:** chapter chip top-left, progress rail top-right. Keep content between about 9cqh
  and 86cqh.
- **Start state:** show a poster frame (title + big play button) before first play; browsers block
  autoplay with sound.
- Respect `prefers-reduced-motion` (offer the same timeline, shorter tweens) and work in light
  and dark page backgrounds around the stage; the stage keeps its own palette.
- Under 16 MB total (audio is the big part: mono 64 kbps mp3 is plenty for narration).

## Look and motion
Cream background, ink text, ink-bordered cards with a flat ink shadow, mono uppercase chips, one
serif-italic accent word per frame in a peach pill. Palette: bg #f3ecd9, surface #faf6ec, ink
#161513, muted #6a6258, accent #f0a87a, accent-strong #e07a45, butter #f0c860, sage #a9c79b,
lilac #bdb2ea. Motion helpers: `land` (pop, back.out), `rise` (fade up, staggered), `fade`,
`exit`, `draw` (SVG dashoffset), `type` (typewriter), `count` (count-up). Every beat moves.

## Rules that are not optional
- Facts only from `brief.md`. Illustrative numbers are labelled on screen.
- Never a blank frame at a beat start; counters stay hidden until they count.
- Readable on a phone: mono labels 1cqw or larger, body 1.3cqw or larger.
- Short sentences, plain words, no em dashes on screen or in narration.
- Seek-safe: no `Math.random()` at runtime, no `Date.now()`, no infinite repeats; avoid `tl.call`
  for state (use tweens or `tl.set`) so scrubbing backwards restores the earlier state.
- Gotchas: never put a GSAP transform on an element that also has a CSS transform (wrap it);
  tween x/y/scale, not left/top/width; later tweens on an element use `to`, not a second
  `fromTo`; elements that appear later start hidden (`tl.set(...,{autoAlpha:0},0)` or CSS);
  an opaque element later in the DOM covers moving chips, so give movers a z-index.
- Never upload or post anything yourself. The user publishes.