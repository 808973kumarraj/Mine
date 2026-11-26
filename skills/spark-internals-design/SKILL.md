---
name: spark-internals-design
description: Design system for the Spark Internals explainer series (EP. 07 "The Shuffle"). Use when making new Spark Internals episodes, or any thumbnail, slide, docs page or visual that must look like part of the series: graph-paper 9:16 frames, ink-outlined machines and partitions, a dark studio deck with word-by-word narration, uppercase mono labels, and Newsreader serif statements. Covers tokens, fonts, components, the drawing kit and the narration voice.
---

# Spark Internals design system

Spark Internals is an animated explainer series about Apache Spark internals. Each episode is a 470 × 835 (about 9:16) sheet of graph paper on a dark stage, with machines, partitions and disks drawn as ink-outlined objects and a dark deck at the bottom that speaks the narration.

Use this skill for new episodes first, and for anything that should look like part of the series.

## Before you write anything

1. Read `README.md` in this folder. It holds the full rules for voice, frame, color, type, spacing, drawing, motion, accessibility and components.
2. For exact values, use `tokens.css` (or `tokens.json`, the source). For a component's props, read `api/components/<Name>.md`. For its behavior, read `components/<Name>/README.md`.
3. Start from `components/Episode/preview.html`, a working two-scene example with a beat.

## Load it on a page

- Link `tokens.css`, then `components/bundle.css`.
- Load React 18.3.1 from `components/lib/react.production.min.js`, then `react-dom.production.min.js`.
- Load `components/bundle.js`. Everything is on `window.SparkInternals`.
- Fonts are in `fonts/`: JetBrains Mono (chrome, labels, code) and Newsreader (statements only).
- Draw with `const K = SparkInternals.kit(ctx)`. Examples: `K.title(5, 'COST PIPELINE', t)` for a scene title, `K.box(52, 262, 78, 56, 40, { dividers: 1, vents: 5, leds: [K.C.green], label: 'EXEC 1' })` for an executor.

## Core rules

- **Voice:** short spoken sentences and fragments, present tense, plain verbs. No exclamation marks, no emoji. Lead with a number or the thing on screen. Keep real units: "48.2 GB", "4.7 µs". Put code in backticks; it becomes a code pill, using the real API names.
- **Scenes:** one idea per scene, two or three sentences, 6.5–9 s of speech. Beats are one sentence, 2.5 s. End on a three- or four-word imperative ("Move less data.").
- **Labels:** uppercase mono with " · " between facts. Measurements stay lowercase. Counters are zero-padded ("05 / 09"). Symbols: × multiplies, ≈ estimates, → becomes, — joins a scene number to its name, › is the terminal prompt.
- **Color means one thing:** blue for data, code and network; green for healthy, done and kept; red for cost, failure and stalls; amber for now (cursor, current step, waiting, broadcast). Partition keys are always k1 blue, k2 green, k3 amber, k4 umber. Red is never a key. Pair every state color with a word.
- **Amber is never text on cream or paper.** Amber text lives on the deck only.
- **Frame:** 470 × 835, `radius-frame` corners, `shadow-frame`. Scenes are drawn in a 470-wide coordinate space; the scene title sits at x 20, y 68; content stays between x 16 and x 454 and snaps to the 14px grid.
- **Motion:** ease outCubic for arrivals, inCubic for departures, inOutCubic for moves, outBack for landings. Under reduced motion, show each figure in its final state and start paused.
- **Accessibility:** give each scene canvas an `aria-label` describing what it shows, keep narration as text, and never carry state by color alone.

## Do not

- Put more than one big number on screen at a time.
- Use Newsreader for labels or running text.
- Add icons from an outside set. Meaning comes from drawn objects and the few text glyphs above.
- Invent a logo. The series name is set in type in the header: "> SPARK INTERNALS / EP. 07".
- Use the episode's "⬇ .HTML" download button in artifact pages.

## Folder map

- `README.md`: the complete design system guide
- `tokens.css`, `tokens.json`, `manifest.json`: tokens and bundle manifest
- `components/`: 36 components, each with a README and `preview.html`, plus `bundle.js`, `bundle.css`, `index.d.ts`, and `lib/` (React)
- `api/`: props reference for components and tokens
- `fonts/`: JetBrains Mono and Newsreader (SIL Open Font License 1.1)
- `assets/Marks/`: the episode thumbnail mark, `episode-thumbnail.svg`
