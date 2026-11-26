Spark Internals is the look of an animated explainer series about Apache Spark's internals, taken from EP. 07, "The Shuffle". Every episode is a 9:16 sheet of graph paper on a dark stage. Machines, partitions and disks are drawn as small objects in ink outlines. A dark studio deck at the bottom speaks the narration one word at a time, and a single serif sentence lands between scenes. Use this system for new episodes first, and for anything that should look like part of the series: thumbnails, slides, docs pages.

## Voice

The narration is short, concrete and spoken. Write the way the episode does:

> A billion rows. A hundred executors. Then one innocent line — `groupBy("user_id")` — and the whole job starts to crawl.

- Use short sentences and fragments, present tense and plain verbs: "grabs", "jams", "flows", "crawl". No exclamation marks, no emoji.
- Lead with a number or with the thing on screen. Numbers keep their real units and separators: "1,000,000,000", "4.7 µs", "48.2 GB", "12 MB".
- Put code in backticks in the script; it becomes a code pill. Use the real API names: `filter`, `select`, `broadcast`, `groupByKey`.
- Keep one idea per scene: two or three sentences, 6.5–9 seconds of speech. A beat is one sentence, usually a turn: "But every key has to meet its siblings." "So what does one move actually cost?" "The fix is not a faster pipe."
- End on a three- or four-word imperative. It becomes the end card's title: "Move less data."
- Labels on screen are uppercase mono with " · " between facts: "STAGE 2/3 · EXCHANGE hashpartitioning(user_id)", "BROADCAST · 4 COPIES · 48 MB". Code inside a label keeps its own case. Measurements beside values stay lowercase: "shuffle write", "spill to disk", "tasks 76 / 200". Key names stay lowercase: k1 to k4.
- Symbols: "×" multiplies, "≈" estimates, "→" means "becomes", "·" separates, "—" joins a scene number to its name ("05 — COST PIPELINE"), "›" is the terminal prompt. Counters are zero-padded: "05 / 09".

## The frame

- The frame is 470 × 835 (`frame-width`, `frame-height`), about 9:16, and min(95vh, 835px) tall. Put it on `stage`, with `radius-frame` corners and `shadow-frame`.
- Its ground is `cream` graph paper: `grid-minor` lines every `pitch-minor` (14px), `grid-major` lines every `pitch-major` (70px), and a `vignette` that fades in from 55% out to the edges. The `si-paper` class puts the same paper on any page.
- Every size is given at k = 1 and multiplies by k = frame width ÷ 470. Components read `--k`; `Frame` and `Episode` set it for you.
- Top to bottom: the header (`header-height`, 38px) over a `stroke-rule` `blue` rule; the scene, drawn on a canvas in a 470-wide coordinate space and scaled to stay clear of the deck; the studio deck, inset `space-10` from the sides and bottom, `deck-height` (37%) tall and `deck-height-beat` (85%) during a beat.
- On the canvas, the scene title sits at x 20, y 68, and content stays between x 16 and x 454. Snap layout to the 14px grid. Executors stand in one row, 102px apart, starting at x 52.

## Color

The paper is warm and quiet. Color is kept for meaning, and each hue means one thing.

- `ink` draws everything: text, numbers and outlines. `muted` is for eyebrows, captions and counters.
- `blue` is data and code: anything moving over the network, flow lines, the bit ribbon, code pills, filter and select. `green` is healthy, done and kept. `red` is cost and failure: stalls, stragglers, overhead, the barrier, anti-patterns. `amber` is now: the cursor, the word being spoken, the current step, activity, waiting, and broadcast.
- Partition keys reuse hues as categories, always in this order: `key-1` blue, `key-2` green, `key-3` amber, `key-4` umber. Red is never a key. While keys are on screen, say status in words (a pill), not by hue alone.
- Surfaces: `cream` for the ground, `paper` for cards, `node` for machines, `white` for records, table cells and progress tracks. Diagram materials: `tile`, `tile-shade`, `slab` (a partition with no key yet), `platform`, `fact` and `block`. Tints for rows and blocks: `tint-kept`, `tint-dropped`, `tint-queued`.
- The deck has its own set: `deck-bg`, `deck-text`, `deck-muted`, `deck-unspoken`, `deck-line`, `deck-tick`, `deck-code` on `deck-code-bg` with `deck-code-line`, and `deck-on` for a toggled button.
- Text on a filled shape takes its `on-` token: `on-ink`, `on-blue`, `on-green` and `on-red` are cream; `on-amber` is ink.
- Amber is a mark color on light grounds. Never set amber text on `cream` or `paper`; amber text lives on the deck.
- There is one theme, Paper. The frame looks the same in light and dark viewers because it is a sheet of paper on a dark stage; a page that hosts it puts `stage` behind it.

## Type

Two families, both shipped here as font files: JetBrains Mono for everything the system says, Newsreader for the few sentences it means. Both are under the SIL Open Font License 1.1, with their copyright notices inside the files.

- Set the chrome in `chrome-header` (uppercase), `chrome-meta` and `chrome-button`, and the narration in `chrome-script` (17px on 1.42). Code inside the script is `chrome-code`.
- Draw scene text in the Canvas styles: the one big number in `count-xl` (`count-lg` on a card, `count-md` on a counter), before-and-after figures in `delta`, scene titles in `scene-title`, code in `canvas-code`, values in `canvas-value`, captions in `canvas-caption`, eyebrows and pill text in `canvas-label`, names under objects in `canvas-micro`.
- Use Newsreader only for statements: `beat` (42px) for a beat, and `closing` (50px) with `closing-sub` (21px italic) on the end card. Never use it for labels or running text.
- Use tabular figures wherever numbers change or line up.
- The fonts are Latin subsets. "→", "←", "≈" and superscripts fall back to the system mono and serif, as they do in the episode.

## Space, strokes and corners

- Spacing tokens are the episode's own steps, named by their pixel value: `space-2`, `space-5`, `space-6`, `space-7`, `space-8`, `space-10`, `space-12`, `space-14`, `space-16`, `space-20`. Pad cards with `space-14` and gap pills with `space-6`.
- Every drawn thing has an ink outline with round joins: `stroke-fine` (0.8px) for tiles, slabs and cells; `stroke-box` (1.1px) for machines, the disk and records; `stroke-card` (1.2px) for cards and the barrier; `stroke-rule` (1.3px) for the header rule; `stroke-deck` (3.5px) for the cream border around the deck.
- Each radius belongs to one kind of object: `radius-check` 2, `radius-bar` 3, `radius-code` 5, `radius-button` 6, `radius-terminal` 7, `radius-card` 8, `radius-frame` 14, `radius-deck` 18, and `radius-pill` (always half the height).
- Shadows fall straight down in warm ink: `shadow-card`, `shadow-pill`, `shadow-record`, `shadow-terminal`, `shadow-deck`, `shadow-frame`. Machines cast a soft parallelogram down and to the right; the kit draws it.

## Drawing

Scenes are drawn from a handful of primitives in the drawing kit, `SparkInternals.kit(ctx)`, which reads its colors from the tokens. Build new objects from these rules, not from illustrations.

- Machines are oblique boxes. The front face rises 8.5% of its width; the depth recedes up and to the left, 0.56 × depth across and 0.48 × depth up. The top face is 25% lighter than the front and the side 22% darker. Fill with `node`.
- The cluster is isometric at 30°: tile tops in `tile`, left faces 16% darker, right faces 30% darker, each tile with three vents and an LED.
- Partitions are slabs, 26 × 5 × 14, stacked 5px apart: `slab` grey until tagged, then their key color with the key name on top.
- Hatching marks time and space: green over pale green for work done, red for a straggler, `hatch-grey` for waiting and for platforms, `hatch-white` over green bars.
- Lines: flow lines are blue, dashed 5/4 and marching at about 26px a second; the scene-title rule is muted, dashed 2/3; the barrier is red, dashed 3/3; something that is not there yet is a blue wireframe, dashed 4/3.
- LEDs are green for healthy, amber for busy and red for straggling. They pulse gently while a scene plays.

## Motion

- Easing: `ease.outCubic` for things arriving, `ease.inCubic` for things leaving, `ease.inOutCubic` for moves, `ease.outBack` (overshoot 1.70158) for things that land.
- Scenes fade in over 0.35 s and, unless a beat follows, out over their last 0.3 s. Machines drop 170px and land with an overshoot. Items stagger by 0.07–0.24 s. Slabs travel on an arc that lifts 70px at mid-flight.
- Titles print with the stipple reveal over 1.1 s. The terminal types its line in 1.3–1.6 s, and its cursor flips 2.2 times a second.
- The deck grows for a beat over 0.75 s on cubic-bezier(0.65, 0, 0.35, 1); the script resizes over 0.6 s; a word changes color in 0.18 s; buttons change in 0.15 s. The header cursor blinks once a second, in steps.
- EP. 07 runs 77 seconds: nine scenes of 6.5–9 s and four beats of 2.5 s.
- Under reduced motion, show each figure in its final state, stop the cursors and start episodes paused.

## Iconography

There is no icon set. Meaning comes from drawn objects (machines, slabs, the disk, the hourglass, LEDs) and a few text glyphs (›, ·, ×, ≈, →). There is no logo either: the series name is set in type in the header, "> SPARK INTERNALS / EP. 07", followed by the blinking amber cursor. The episode's thumbnail mark, an isometric tile with a blue face, a green LED and an amber cursor, is in assets/Marks.

## Accessibility

- Text pairs reach 4.5:1 except where the episode's own pairs fall short. Those are kept and flagged in their token notes: `muted` on `cream` (4.4:1), `deck-unspoken` (2.3:1, dim until the word is spoken), `deck-line` borders (1.5:1) and `deck-tick` (2.5:1).
- Two of the episode's pairings are corrected here: amber pills and amber slab labels take ink instead of cream (5.4:1 instead of 2.8:1), and the current step of a StepList is set in ink, not amber.
- Keyboard focus is a 2px ring after a 2px gap: `focus-ring` (blue) on cream and paper, `focus-ring-deck` (amber) on the deck and the stage. The episode had no focus style.
- Never carry state by color alone. Every state color sits beside a word: STALLED, waiting, real work, ANTI-PATTERN, DO THIS.
- Scenes are canvas. Give each scene canvas an `aria-label` that says what it shows, and keep the narration as text.

## Components

- Frame: `Episode`, `Frame`, `Header`, `Deck`, `DeckButton`, `Scrubber`.
- Labels: `SceneTitle`, `StippleText`, `Pill`.
- Data: `Card`, `Stat`, `MetricRows`, `ProgressBar`, `StepList`, `TaskBars`, `DataGrid`.
- Code: `Terminal`, `CodeCompare`.
- Data in motion: `Record`, `BitRibbon`, `Pipe`.
- Machines: `Machine`, `Slab`, `RackGrid`, `Disk`, `Hourglass`, `LED`.
- Endings: `ClosingCard`.

To make a new episode, give `Episode` a list of segments whose `draw(K, t)` functions use the kit. `components/Episode/preview.html` is a working two-scene example with a beat.

## Using this system

- Load tokens.css, then components/bundle.css, React 18 (components/lib/react.production.min.js, then components/lib/react-dom.production.min.js), and components/bundle.js. Everything is on `window.SparkInternals`.
- Draw with `const K = SparkInternals.kit(ctx)`, for example `K.box(52, 262, 78, 56, 40, { dividers: 1, vents: 5, leds: [K.C.green], label: 'EXEC 1' })` for an executor, or `K.title(5, 'COST PIPELINE', t)` for a scene title.
- Outside a frame, components keep their frame sizes, so labels are 7.5–9.5px. On a page, set `--k` on a wrapper to scale them up; 1.5 reads well at desktop widths.
