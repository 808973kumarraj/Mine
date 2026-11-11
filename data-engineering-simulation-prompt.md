# Prompt: an interactive simulation of a data-engineering topic

The same recipe as `birdsong.html`, adapted for data engineers: a system drawn as a machine with a
cut-away, real settings you can drag, a live scrolling read-out, and a control panel with presets,
mode buttons, sliders and a playable keyboard.

Fill in the **topic sheet**, leave the rest as it is, and paste the whole block into a chat.
A filled-in sheet for Spark shuffling and starter sheets for other topics follow the prompt.

```text
Build an interactive simulation that lets a data engineer understand [TOPIC] by operating it.
Think of it as an instrument, not a slideshow: a drawing of the system with a cut-away, parts
you can drag, a live scrolling read-out, and a control panel with presets, sliders and keys.

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
- Control panel contents: [fill in the lists under CONTROL PANEL below, or leave them to you]

THE SCREEN
1. Run button and scenario list. A button that reads "Pause Job" while running and
   "Auto-Run Job" when paused, then one card per scenario (small icon, name, one-line
   description). The selected card has a coloured double border.
2. The drawing. The system as physical machinery with a cut-away window. Data is visible:
   records are small tiles coloured by key, moving through the machine. Each draggable part has
   a round handle, a dotted leader line and a label. Under each label a short line changes live:
   a reading with units ("Shuffle write - 1.2 GB"), a state ("Spilling to disk"), or a hint when
   untouched ("Drag to add partitions"). Where a part stands for a real setting, show the
   setting name and value in small monospace type.
   Grabbing a part must feel physical: a thick outline in the accent colour, the handle becomes a
   filled dot with a ring, the leader line and label take the same colour, the handle follows
   the pointer up to a short reach, and the part visibly changes. A part with no data reaching
   it looks pale and idle.
3. The live read-out. A wide strip that scrolls right to left in time, with labelled axis
   ticks, drawn from what the model is producing right now. One colour per worker or stage.
   Trouble (spill, retry, one oversized task, growing lag) is drawn in the accent colour.
4. The verdict strip. The headline numbers for the current run, with the previous run's
   numbers kept beside them so the effect of a change is obvious.
5. The control panel, described next.

CONTROL PANEL (build every component; this is where the person plays)
A bordered panel, top to bottom:
a. Preset grid. Six numbered buttons in two columns ("1 Tiny Dev Job", "2 Nightly Aggregate",
   "3 Skew Disaster", "4 Memory Squeeze", "5 Partition Swarm", "6 Broadcast Win" or the
   equivalents for this topic). Choosing one moves every slider and mode to that preset's
   values. The active preset is tinted. Number keys 1 to 6 select them.
b. Mode row, three controls side by side:
   - a segmented control with three exclusive strategies (for example
     BROADCAST | SORT-MERGE | SHUFFLE-HASH); the active one is filled dark;
   - a latch button for one on/off feature (for example ADAPTIVE);
   - a stepper for one whole-number quantity: "-  WORKERS 4  +".
c. Slider grid. Eight or nine sliders in two columns. Each slider sits in its own thin-bordered
   cell: the name on the left in the heading typeface, the live value with its unit on the right
   in monospace ("1.2 GB", "200", "38%", "120 MB/s"), a thin track underneath and a small
   rectangular thumb. The value text updates while dragging and the model responds at once.
   Use quantities an engineer actually thinks in, for example: input rows, bytes per row,
   rows kept by the filter (%), partitions, memory per task (MB), key skew (%), network (MB/s),
   disk (MB/s).
d. Load wave. One more slider for the wave's period and one for its depth, plus a 2 x 2 block of
   small target buttons that choose what the wave pushes up and down (for example Volume, Skew,
   Row size, Failures). This lets the person watch the system breathe under changing load.
   Only one target is active at a time; pressing the active one switches the wave off.
e. Event keyboard. A piano-style row. The eleven white keys carry a computer-key letter
   (A S D F G H J K L ; ') and, in small type beneath, what they send: a burst of records for one
   data key or partition. The seven black keys (W E T Y U O P) inject faults: kill a worker,
   slow a disk, drop the network for a moment, send a late burst, send duplicates, change the
   schema, force retries. A pressed key lights in the colour of the worker or stage that ends
   up handling it, so two keys held together show two colours. Keys work by touch (several
   fingers at once), by mouse and from the computer keyboard.
f. Hint line under the keys, listing the shortcuts:
   "Z / X Workers  -  C / V Input  -  Space Pause".
The run button changes its text to "Auto-Play Demo Sequence" here: it plays a scripted series of
key presses and slider moves that demonstrates the one idea, and can be paused.

TWO WAYS TO USE IT
- Demo: the job runs by itself in a loop from the moment the page opens. Dragging a part or
  moving a slider changes the outcome of the next pass.
- By hand: nothing moves until the person lets data in (a source valve on the drawing, or a
  key on the event keyboard). Handles stay where they are left; double-click resets one.

HOW IT SHOULD WORK INSIDE
- Start from a model, not from animation. Keep one state object in real units (rows, bytes per
  row, partitions, workers, cores, memory per task, disk and network MB/s, skew) and compute the
  drawing, the labels, the read-out and the verdict from it every frame. Sliders, handles,
  presets and keys all write to that one object, so every control and every picture agree.
- Use simple cost rules whose direction is right and whose size is plausible, and write them
  out in a comment block at the top of the script. For example: data moved = rows that survive
  the filter x bytes per row; a task's time = its partition's bytes / throughput; a task spills
  when its partition is larger than its memory; a stage ends when its slowest task ends.
- Make it deterministic with a seeded random generator: the same settings give the same run.
- Be accurate about the tool. Use real setting names and default values for the version in the
  topic sheet. If you are unsure of a default or behaviour, say so in a comment and in your
  final notes instead of guessing.
- Be honest about scale: draw a few hundred tiles and print the ratio ("1 tile = 5 million
  rows"). Say once, in small print, that the numbers are illustrative and not benchmarks.
- Each scenario and preset must look different in the read-out, not only in the drawing.

LOOK
A calm technical-illustration style: warm paper background, a serif typeface for names, a
monospace typeface for values, setting names and small buttons, thin outlines, flat colour with
soft shading, a muted palette, one accent colour for active or in-trouble things, and a second
colour pair (for example teal and red) to tell two workers or stages apart. Panels and buttons
have thin double borders. Draw everything in code. No stock icons, no emoji, no logos.

CONSTRAINTS
- One self-contained .html file: plain HTML, CSS and JavaScript. No libraries, no build step,
  no external requests, no embedded images, fonts, audio or base64 data. It must work when
  opened as a local file on a phone. I want to be able to read and edit every line.
- Lay the screen out at a fixed design size and scale it to the window. On a narrow or portrait
  screen use one column: run button, scenario cards in a compact grid, the drawing at full
  width, the read-out, the verdict, then the control panel at full width.
- Touch targets of at least 40 px. Dragging a handle, a slider or a key must not scroll the page.
- Hold 60 frames per second on a phone and on a large screen. Redraw only what changes. If the
  still artwork uses textures or filters, paint it once into a canvas and animate above it.
- Comment the code by section, explaining what each part models.

ORDER OF WORK
1. Before writing code, show me: the state variables with units, the cost rules, each
   scenario's and preset's values, what every slider, mode, key and fault does to the state,
   and the real settings and defaults you plan to display, marking any you are unsure of.
2. Build the model, the read-out and the verdict first and check them with numbers.
3. Add the control panel and wire every control to the model.
4. Add the drawing and its handles, then the scenarios, then the demo sequence.

CHECK BEFORE YOU CALL IT DONE
Open the page in a browser and confirm:
- every scenario and every preset runs and shows its distinctive pattern in the read-out;
- every slider, mode button, stepper, wave target, key and handle changes the labels, the
  read-out and the verdict in the direction a practitioner would expect (list those directions),
  with a mouse, with a finger and from the keyboard;
- by-hand mode is still until data is let in; the same settings give the same run twice;
- there are no console errors and nothing is requested from the network;
- it works at 1366 x 768 and at 390 x 844 without the page scrolling under a dragged control.
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
