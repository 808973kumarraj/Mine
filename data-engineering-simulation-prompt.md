# Prompt: an interactive simulation of a data-engineering topic

The same recipe as `birdsong.html`, adapted for data engineers and with the interactivity specified
in full: what every element does on hover, tap, drag, hold and key press, time controls, record
tracing, faults, cause-and-effect feedback, guided challenges, undo and shareable state.

Fill in the **topic sheet**, leave the rest as it is, and paste the whole block into a chat.
A filled-in sheet for Spark shuffling and starter sheets for other topics follow the prompt.

```text
Build a fully interactive simulation that lets a data engineer understand [TOPIC] by operating
it. It is an instrument, not a slideshow: a drawing of the system with a cut-away, parts you can
drag, a live read-out you can scrub, and a control panel with presets, sliders and keys. Nothing
on the screen is decoration: everything that looks touchable responds, and every response shows
its cause and its effect.

TOPIC SHEET (this is the part I filled in)
- Topic: [one line, e.g. "what a shuffle costs in Spark"]
- Tool and version: [e.g. Apache Spark 3.5; write "generic" if it is a concept, not a tool]
- Who it is for: [e.g. data engineers who have run jobs but never tuned one]
- The one idea they should leave with: [a single sentence; everything on screen serves it]
- The system to draw: [the machine, e.g. "four executors side by side, one of them opened up"]
- What the cut-away shows: [the hidden mechanism]
- Parts to drag on the drawing (5 to 8). For each: name - what dragging changes - the real
  setting it stands for - the effect.
- Scenarios to compare (5 to 7): real situations, one line each on what makes it distinct.
- Live read-out: [what an engineer would watch in the tool's own UI, moving in time]
- Verdict numbers (2 to 4, with units): [run time, data moved, peak memory, cost ...]
- Control panel contents: [presets, modes, sliders, wave targets, keys; or leave them to you]
- Challenges (5): [goals stated as numbers; or leave them to you]

1. WHAT IS ON SCREEN
a. Run button and scenario cards (icon, name, one-line description; selected card has a
   coloured double border).
b. The drawing: the system as physical machinery with a cut-away. Data is visible as small
   record tiles coloured by key, moving through the machine. Each draggable part has a round
   handle, a dotted leader line and a label with a live sub-line: a reading with units
   ("Shuffle write - 1.2 GB"), a state ("Spilling to disk") or a hint ("Drag to add
   partitions"). Real setting names and values appear in small monospace type.
c. The live read-out: a wide strip scrolling right to left in time, with axis ticks and units,
   one colour per worker or stage, trouble in the accent colour, and a legend of tappable chips.
d. The verdict strip: headline numbers, each with its change since the previous run.
e. The control panel (section 6) and a slim transport bar (section 5).
f. A help button that opens the tour, the challenges and the shortcut sheet.

2. THE MODEL BEHIND IT
- One state object in real units (rows, bytes per row, partitions, workers, cores, memory per
  task, disk and network MB/s, skew). The drawing, labels, read-out, verdict and explanations
  are all computed from it every frame. Handles, sliders, presets, keys, undo and shared links
  all write to that one object, so every control and every picture always agree.
- Simple cost rules with the right direction and plausible sizes, written out in a comment
  block at the top. Example: data moved = rows that survive the filter x bytes per row; a
  task's time = its partition's bytes / throughput; a task spills when its partition is larger
  than its memory; a stage ends when its slowest task ends.
- Deterministic: a seeded random generator, so the same settings give the same run.
- Accurate about the tool: real setting names and defaults for the stated version. If unsure of
  a default or behaviour, say so in a comment and in your final notes instead of guessing.
- Honest about scale: draw a few hundred tiles and print the ratio ("1 tile = 5 million rows").
  Say once, in small print, that numbers are illustrative and not benchmarks.
- Keep the last 60 seconds of state so time can be scrubbed.

3. INTERACTION RULES THAT APPLY EVERYWHERE
- Respond within one frame. The model recomputes immediately; transitions last at most a
  quarter of a second and never block further input.
- Every control has visible states: rest, hover (mouse), focus ring (keyboard), pressed or
  dragging, selected, and disabled. A disabled control says why on hover or long-press.
- Linked highlighting: pointing at anything highlights the same thing everywhere it appears,
  at the same moment: the part in the drawing, its label, its slider in the panel, its trace in
  the read-out and its number in the verdict.
- Everything is reversible: double-click or double-tap resets one control to the scenario's
  value; sliders carry a small tick at that value; a Reset All button; undo and redo.
- Every control shows its value with units, and the real setting name where there is one.
- Hover for 0.4 s, or long-press on touch, shows a one-sentence tip: what this is and what
  raising it does.
- Cursors match the action: grab and grabbing on handles, a horizontal-resize cursor on
  sliders, a pointer on buttons.

4. INTERACTIONS ON THE DRAWING
- Drag a handle: it follows the pointer in two dimensions up to a short reach; the part
  stretches, squeezes or turns with it; the part gets a thick outline in the accent colour, the
  handle becomes a filled dot with a ring, the leader line and label take the same colour; the
  label's reading updates continuously. Letting go leaves it there.
- Tap a part without dragging: an inspector card opens beside it with its current numbers, the
  setting behind it, a small chart of its last 10 seconds, and one sentence saying what it is
  doing right now. Tapping elsewhere or pressing Escape closes it.
- Tap a record tile: trace it. The tile gets a ring, a solid trail shows where it has been, a
  dotted line shows where it will go, a step list gives each hop with time and bytes, and all
  other tiles dim. Tap again or press Escape to release it.
- Tap a worker: isolate it. Only its tiles and its row in the read-out stay bright.
- Hovering a label highlights its part, and the other way round.
- Zoom and pan: scroll or pinch to zoom the drawing from 1x to 3x around the pointer, drag the
  background to pan, double-click the background to reset the view.
- A part with no data reaching it looks pale and idle; it colours in when data arrives.
- A bottleneck badge always points at the one part currently limiting the run (network, disk,
  memory, a skewed task) and moves when the bottleneck changes.

5. INTERACTIONS WITH TIME
- Transport bar: play or pause, step one tick forward, speed 0.25x 0.5x 1x 2x 4x, and a Live
  button.
- Scrub: drag along the read-out to move a playhead back through the last 60 seconds. The
  drawing, labels and verdict show that moment. Live returns to now.
- Hover or tap any mark in the read-out to see its exact values in a small tag.
- Tap a bar or segment to select that task: its worker and partition light up in the drawing
  and the inspector opens on it.
- Pin: freezes the current run as a ghost outline in the read-out and as a second column in
  the verdict, for comparison. One pin at a time; a Clear Pin button.
- Tap a legend chip to show or hide that series.

6. THE CONTROL PANEL, COMPONENT BY COMPONENT
a. Preset grid: six numbered buttons in two columns ("1 Tiny Dev Job" ... "6 Broadcast Win").
   Choosing one glides every slider and mode to its values over 0.3 s, so the person sees what
   changed. The active preset is tinted; it un-tints as soon as any value is moved by hand.
b. Mode row: a segmented control with three exclusive strategies; a latch button for one
   on/off feature; a stepper "-  WORKERS 4  +" that repeats while held.
c. Slider grid: eight or nine sliders in two columns, each in its own thin-bordered cell: name
   on the left, live value with unit on the right in monospace, thin track, small rectangular
   thumb, a tick at the scenario's value. Drag the thumb, tap the track to jump, use arrow keys
   to step (Shift for ten steps), double-click to reset, tap the value to type an exact number.
d. Load wave: sliders for period and depth, and a 2 x 2 block of target buttons choosing what
   the wave pushes up and down (Volume, Skew, Row size, Failures). One target at a time;
   pressing the active one switches the wave off.
e. Event keyboard: a piano-style row. Eleven white keys (A S D F G H J K L ; ') each send a
   burst of records for one data key or partition, named in small type on the key: tap for one
   burst, hold for a steady stream. Seven black keys (W E T Y U O P) inject faults: kill a
   worker, slow a disk, drop the network, send a late burst, send duplicates, change the
   schema, force retries: tap for a two-second fault, hold to keep it going. A pressed key
   lights in the colour of the worker or stage that handles it. Several keys at once work, by
   touch, mouse and computer keyboard.
f. Hint line with the shortcuts. In the panel the run button reads "Auto-Play Demo Sequence":
   a scripted series of key presses and slider moves that demonstrates the one idea, with each
   control visibly moving as if a person were operating it.

7. SHOWING CAUSE AND EFFECT (after every change, however it was made)
- A short pulse travels along the affected path in the drawing.
- Verdict numbers count to their new values, with an arrow, the change in percent, and a tint
  for better or worse. The previous run stays as a faint ghost in the read-out.
- One "why" sentence appears under the verdict, generated from the model's numbers, for
  example: "Partition 7 holds 41% of the rows, so one task runs 6x longer than the rest."
- An Explain switch adds small numbered captions at each stage of the drawing, each one
  sentence about what is happening there right now.

8. FAULTS AND RECOVERY
Every fault shows its cause, its effect and the recovery: the affected part flashes and goes
dark, its data is visibly re-routed or re-sent, the read-out shows the retry, and the verdict
counts failures and time lost.

9. GUIDED LAYER
- First-open tour: four or five coach marks that point at real controls and each ask for one
  action ("Drag the filter gate to the left"). A step completes only when the person does it.
  Skippable, and replayable from the help button.
- Challenges: five goals stated as numbers ("Finish under 40 s without adding workers", "Make
  it spill, then fix it two different ways"). Each has a live progress bar checked by the
  model, a Hint button that pulses the relevant control, and a short explanation when solved.

10. STATE
- Undo and redo for every change, at least 50 steps, by buttons and Ctrl+Z / Ctrl+Shift+Z.
- Reset All returns to the scenario's values.
- The whole setup is written into the page address after the # sign, so a copied link reopens
  the exact state. Provide a Copy Link button.

11. KEYBOARD MAP
Space pause - . step - [ and ] slower and faster - 1 to 6 presets - Z / X fewer or more
workers - C / V less or more input - letters for the event keys - Tab moves between controls
in reading order - arrows adjust a focused slider - Escape closes the inspector or a trace -
? opens the shortcut sheet.

12. PHONE AND TOUCH
One column in this order: run button, scenario cards in a compact grid, the drawing at full
width, the read-out, the verdict, the control panel. The transport bar sticks to the bottom of
the screen. The inspector opens as a sheet from the bottom. Touch targets are at least 40 px.
Dragging a handle, slider or key never scrolls the page; pinch zooms only the drawing.

13. LOOK
A calm technical-illustration style: warm paper background, a serif typeface for names, a
monospace typeface for values, setting names and small buttons, thin outlines, flat colour with
soft shading, a muted palette, one accent colour for active or in-trouble things and a teal and
red pair to tell two workers or stages apart. Panels and buttons have thin double borders.
Draw everything in code. No stock icons, no emoji, no logos.

14. CONSTRAINTS
- One self-contained .html file: plain HTML, CSS and JavaScript. No libraries, no build step,
  no external requests, no embedded images, fonts, audio or base64 data. It must work when
  opened as a local file on a phone. I want to be able to read and edit every line.
- A fixed design size scaled to the window on wide screens; the single column on narrow ones.
- 60 frames per second on a phone and on a large screen. Redraw only what changes. If the still
  artwork uses textures or filters, paint it once into a canvas and animate above it.
- Respect the reduced-motion setting: keep the feedback, drop the travelling pulses.
- Comment the code by section, explaining what each part models.

15. ORDER OF WORK
1. Before writing code, show me: the state variables with units, the cost rules, every
   scenario and preset, what each handle, slider, mode, key and fault does to the state, the
   five challenges with their pass conditions, and the real settings and defaults you will
   display, marking any you are unsure of.
2. Model, read-out, verdict and transport bar. Check them with numbers.
3. Control panel, wired to the model.
4. Drawing, handles, inspector, record tracing, zoom.
5. Cause and effect, the bottleneck badge, faults.
6. Tour and challenges. 7. Undo, reset, shared link. 8. Phone layout.
If it does not fit in one reply, deliver it stage by stage and keep every stage working. Leave
no stubs or TODO comments: if something is not built, say so.

16. INTERACTION TEST MATRIX (run it before you call it done)
For every handle, slider, mode, stepper, wave target, key, legend chip and transport button,
check: it responds to mouse, to touch and to the keyboard; its hover, focus, pressed and
selected states are visible; it changes the labels, the read-out and the verdict in the
direction a practitioner would expect (list those directions); double-click resets it; undo
restores it. Then check: every scenario and preset shows its distinctive pattern; scrubbing
shows past moments correctly; tracing follows one record end to end; each fault recovers; each
challenge can be solved and detects success; a copied link reopens the same state; the same
settings give the same run twice; there are no console errors and no network requests; it works
at 1366 x 768 and at 390 x 844 without the page scrolling under a dragged control.
Tell me plainly what you verified, what you could not verify, which rules are simplified, and
which tool facts I should double-check against the documentation.
```

## Filled-in topic sheet: Spark shuffling

```text
- Topic: what a shuffle costs in a Spark job, and how to make it cheaper
- Tool and version: Apache Spark 3.5
- Who it is for: data engineers who have run Spark jobs but never tuned one
- The one idea: a shuffle moves every surviving row across the network once, so the cheapest
  shuffle is the one with less data in it, or the one that never happens
- The system to draw: four executors side by side, one opened up, with partition trays below
- What the cut-away shows: rows hashed by key into partition buckets, serialized, written to
  local disk, fetched over the network by the next stage, deserialized
- Parts to drag on the drawing:
    1. Input valve - rows in (100 million to 10 billion) -> more of everything
    2. Filter gate - share of rows kept before the shuffle - filtering early -> less data moved
    3. Column trimmer - bytes per row - selecting fewer columns -> less data moved
    4. Partition dial - number of shuffle partitions - spark.sql.shuffle.partitions
       (default 200) -> too few spills, too many makes a swarm of tiny tasks
    5. Skew weight - share of rows on the hottest key -> one oversized task
    6. Memory per task - memory each task may use -> spill when a partition is larger
    7. Broadcast switch - the small table's size against spark.sql.autoBroadcastJoinThreshold
       (default 10 MB) -> below it the big table is not shuffled at all
    8. Adaptive lever - spark.sql.adaptive.enabled -> merges small partitions and splits
       skewed ones in joins
- Scenarios: plain groupBy; filter first; skewed key; too few partitions; too many partitions;
  broadcast join; adaptive execution on
- Live read-out: task timeline per executor (one bar per task, coloured by executor, spill
  hatched, the skewed task in the accent colour) with shuffle write and read bytes above it
- Verdict numbers: run time, data shuffled, peak memory per task, tasks that spilled
- Control panel contents:
    presets: Tiny Dev Job, Nightly Aggregate, Skew Disaster, Memory Squeeze, Partition Swarm,
      Broadcast Win
    modes: BROADCAST | SORT-MERGE | SHUFFLE-HASH; latch: ADAPTIVE; stepper: EXECUTORS (1 to 8)
    sliders: Input rows, Row size (bytes), Rows kept (%), Partitions, Memory per task (MB),
      Key skew (%), Network (MB/s), Disk (MB/s)
    load wave targets: Volume, Skew, Row size, Failures
    white keys: send a burst for one country key (IN US BR DE JP FR GB CA AU MX ZA)
    black keys: kill an executor, slow a disk, network blip, late burst, duplicate batch,
      wider rows, fetch failure with retry
- Challenges:
    1. Finish the plain groupBy in under 40 s without adding executors
    2. Make a task spill, then fix it two different ways
    3. Get the skewed scenario down to zero oversized tasks
    4. Halve the data shuffled without changing the input rows
    5. Make the join run with no shuffle of the large table
```

## Starter sheets for other data-engineering topics

| Topic | Sliders and parts | Modes and keys | Scenarios to compare | Live read-out |
|---|---|---|---|---|
| Kafka partitions and consumer groups | producer rate, partition count, time per message, key skew, batch size | modes: round-robin, key-hash, sticky; stepper: consumers; keys send one message key, black keys kill or slow a consumer | steady state, a consumer dies and the group rebalances, hot key, more consumers than partitions | lag per partition over time |
| Parquet and predicate pushdown | row-group size, filter selectivity, columns selected, sort quality of the filter column | modes: unsorted, sorted, partitioned; latch: dictionary encoding; keys run one query each | select everything, two columns only, filter on a sorted column, filter on an unsorted column | row groups read versus skipped, bytes scanned |
| Streaming windows and late data | event rate, lateness spread, watermark delay, window length | modes: tumbling, sliding, session; keys send on-time events, black keys send late bursts or stall a source | on-time stream, clients that go offline, watermark too tight, watermark too loose | events by event time versus arrival, accepted versus dropped, state size |
| Small files and compaction | write frequency, rows per write, target file size, partition-column cardinality | modes: append, micro-batch, compact; latch: auto-compaction; keys trigger a write or a query | hourly appends, streaming micro-batches, over-partitioned table, after compaction | file count and size histogram, query planning time |
| Orchestrator scheduling | worker slots, pool size, task duration, retry count | modes: sequential, parallel, backfill; stepper: days to backfill; black keys fail an upstream task | normal day, 30-day backfill, flaky upstream, pool starvation | Gantt chart of task runs, queue depth |
| Change data capture into a warehouse | change rate, batch interval, share of updates and deletes | modes: append-only, merge, snapshot; keys send inserts, updates or deletes; black keys send out-of-order changes | inserts only, heavy updates, late and out-of-order changes, schema change | freshness lag, rows rewritten per merge |
