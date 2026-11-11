# Prompt: an interactive simulation of a data-engineering topic

The same recipe as `birdsong.html`, adapted for data engineers: a system drawn as a machine with a
cut-away, real settings you can grab, real scenarios to compare, and a live read-out like the one
you would watch in the tool's own UI.

Fill in the **topic sheet**, leave the rest as it is, and paste the whole block into a chat.
A filled-in sheet for Spark shuffling and starter sheets for other topics follow the prompt.

```text
Build an interactive simulation that lets a data engineer understand [TOPIC] by operating it.

TOPIC SHEET (this is the part I filled in)
- Topic: [one line, e.g. "what a shuffle costs in Spark"]
- Tool and version: [e.g. Apache Spark 3.5; write "generic" if it is a concept, not a tool]
- Who it is for: [e.g. data engineers who have run jobs but never tuned one]
- The one idea they should leave with: [a single sentence; everything on screen serves it]
- The system to draw: [the machine, e.g. "four executors side by side, one of them opened up"]
- What the cut-away shows: [the hidden mechanism, e.g. "rows hashed into partitions, written to
  local disk, then pulled across the network"]
- Things the person can grab (5 to 8). Use the real inputs and settings an engineer controls.
  For each: its name, what dragging it changes, the real setting it stands for, and the effect.
    1. [name] - drag to [action] - stands for [setting or input] -> [effect]
    2. ...
- Scenarios to compare (5 to 7): real situations that stress the mechanism differently,
  each with one line on what makes it distinct. [list them]
- Live read-out: [what an engineer would watch in the tool's own UI, moving in time:
  task timeline per worker, bytes moved, spill, lag, queue depth, files written ...]
- Verdict strip: [2 to 4 headline numbers with units: run time, data moved, peak memory, cost]
- Sandbox: [a name], a made-up cluster or pipeline with its own control panel
  (sliders with units, mode buttons, 6 presets, keyboard shortcuts).
- Sound: [none / soft ticks when records land, off by default]

WHAT TO BUILD
One screen with these areas:
1. Centre: the system drawn as physical machinery, with a cut-away window showing what happens
   inside. Data is visible: records are small tiles, coloured by key (or partition, or topic),
   and they move through the machine. Each grabbable part has a small round handle. Dotted leader
   lines run from the handles to labels beside the drawing. Each label has the part's name and,
   under it, a short line that changes live: a reading with units ("Shuffle write - 1.2 GB"),
   a state in plain words ("Spilling to disk"), or a hint when untouched ("Drag to add partitions").
   Where a handle stands for a real setting, show that setting and its current value in small
   monospace type under the label.
   Grabbing a part must feel physical: thick outline in the accent colour, the handle becomes a
   filled dot with a ring, the leader line and label take the same colour, the handle follows the
   pointer freely up to a short reach, and the part visibly changes with it. A part that needs
   data to work should look idle and pale until data reaches it.
2. Left: a run/pause button and the list of scenarios. Picking one reloads the system in that
   scenario's own shape and switches to its behaviour.
3. Bottom: the live read-out, scrolling in time, drawn from what the model is producing, with
   labelled axes and units. One colour per worker or per partition. Mark trouble (spill, retry,
   one oversized task, growing lag) in the accent colour so it is the first thing the eye finds.
4. Verdict strip: the headline numbers for the current run. After any change, keep the previous
   run's numbers beside the new ones so the effect of the change is obvious.
5. Right, only in the sandbox: the control panel.

Two ways to use every scenario:
- Demo: the job runs by itself in a loop, starting as soon as the page opens. The parts move as
  they really would. Dragging a part while it runs changes the outcome of the next pass.
- By hand: nothing moves until the person lets data in (for example by opening the source valve).
  From there each stage waits for them, and the handles stay where they are left.
  Double-clicking a handle resets it.

HOW IT SHOULD WORK INSIDE
- Start from a model, not from animation. Keep one small state object in real units (rows, bytes
  per row, partitions, workers, cores, memory per task, disk and network MB/s, skew) and compute
  the picture, the labels, the read-out and the verdict from it every frame.
- Use simple cost rules whose direction is right and whose size is plausible. For example:
  data moved = rows that survive the filter x bytes per row; a task's time = its partition's
  bytes / throughput; a task spills when its partition is larger than its memory; a stage ends
  when its slowest task ends. Write the rules out in a comment block at the top of the script.
- Make it deterministic: use a seeded random generator so the same settings always give the
  same run. Engineers will compare runs.
- Be accurate about the tool. Use the real names and default values of settings for the version
  in the topic sheet. If you are not sure of a default or a behaviour, say so in a code comment
  and in your final notes instead of guessing.
- Be honest about scale. You cannot draw a billion rows: draw a few hundred tiles and print the
  ratio ("1 tile = 5 million rows"). State once, in small print, that the numbers are
  illustrative and not benchmarks.
- Each scenario must look different in the read-out, not only in the drawing. Skew should show
  as one long bar; too few partitions as spill; too many as a swarm of tiny tasks.

LOOK
A calm technical-illustration style: warm paper background, a serif typeface for names, a
monospace typeface for setting names and numbers, thin outlines, flat colour with soft shading,
a muted palette with one accent colour for whatever is active or in trouble. Draw everything as
SVG or canvas in code. No stock icons, no emoji, no logos.

CONSTRAINTS
- One self-contained .html file: plain HTML, CSS and JavaScript. No libraries, no build step,
  no external requests, no embedded images, fonts, audio or base64 data. It must work when opened
  as a local file on a phone. I want to be able to read and edit every line.
- Lay the screen out at a fixed design size and scale it to the window. On a narrow or portrait
  screen switch to a single column: scenarios on top, the drawing filling the width, the
  read-out, the verdict, then the labels as a list, then the sandbox panel.
- Handles need touch targets of at least 40 px and must not scroll the page while dragged.
- Hold 60 frames per second on a phone and on a large screen. Redraw only what changes. If the
  still artwork uses textures or filters, paint it once into a canvas and animate above it.
- Comment the code by section, explaining what each part models.

ORDER OF WORK
1. Before writing code, show me: the state variables with units, the cost rules, each
   scenario's settings, and the real settings and defaults you plan to display, marking any
   you are unsure of.
2. Build the model, the read-out and the verdict first and check them with numbers.
3. Add the drawing and handles, then the scenarios, then the sandbox.

CHECK BEFORE YOU CALL IT DONE
Open the page in a browser and confirm, scenario by scenario:
- the demo runs and the read-out shows that scenario's distinctive pattern;
- each handle moves the verdict numbers in the direction a practitioner would expect (list
  those directions), with a mouse and with a finger, without the page scrolling;
- by-hand mode is still until data is let in;
- the same settings give the same run twice;
- there are no console errors and nothing is requested from the network;
- it works at 1366 x 768 and at 390 x 844.
Tell me plainly what you verified, what you could not verify, which rules are simplified,
and which tool facts I should double-check against the documentation.
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
- Things the person can grab:
    1. Input valve - drag to change rows in (100 million to 10 billion) -> more of everything
    2. Filter gate - drag to change the share of rows kept before the shuffle - stands for
       filtering early -> less data moved
    3. Column trimmer - drag to change bytes per row - stands for selecting fewer columns
       -> less data moved
    4. Partition dial - drag to change the number of shuffle partitions - stands for
       spark.sql.shuffle.partitions (default 200) -> too few spills, too many makes tiny tasks
    5. Skew weight - drag to change the share of rows on the hottest key -> one oversized task
    6. Memory per task - drag to change memory available to each task -> spill below the line
    7. Broadcast switch - drag the small table's size across the limit - stands for
       spark.sql.autoBroadcastJoinThreshold (default 10 MB) -> below it the big table is not shuffled
    8. Adaptive lever - on or off - stands for spark.sql.adaptive.enabled -> merges small
       partitions and splits skewed ones in joins
- Scenarios: plain groupBy; filter first; skewed key; too few partitions; too many partitions;
  broadcast join; adaptive execution on
- Live read-out: task timeline per executor (one bar per task, coloured by partition, spill
  hatched, the skewed task in the accent colour) with shuffle write and read bytes above it
- Verdict strip: run time, data shuffled, peak memory per task, tasks that spilled
- Sandbox: "SynthCluster" with sliders for rows, row size, partitions, executors, cores, memory,
  network MB/s, disk MB/s and skew; join mode buttons BROADCAST | SORT-MERGE | SHUFFLE-HASH;
  presets Tiny dev job, Nightly aggregate, Skew disaster, Memory squeeze, Partition swarm,
  Broadcast win
- Sound: none
```

## Starter sheets for other data-engineering topics

| Topic | Things to grab | Scenarios to compare | Live read-out |
|---|---|---|---|
| Kafka partitions and consumer groups | producer rate, partition count, consumer count, time per message, key skew, batch size | steady state, a consumer dies and the group rebalances, hot key, more consumers than partitions, slow consumer | lag per partition over time |
| Parquet and predicate pushdown | row-group size, sort order of the filter column, filter selectivity, columns selected, compression | unsorted file, sorted file, select everything, two columns only, filter on a high-cardinality column | row groups read versus skipped, bytes scanned |
| Join strategies | left table size, right table size, key skew, memory per task, broadcast limit | broadcast, sort-merge, shuffle-hash, skewed join, many-to-many join that explodes | data moved and task timeline |
| Streaming windows and late data | event rate, how late events arrive, watermark delay, window length | on-time stream, clients that go offline, watermark too tight, watermark too loose | events by event time versus arrival time, accepted versus dropped, state size |
| Small files and compaction | write frequency, rows per write, target file size, compaction schedule, partition-column cardinality | hourly appends, streaming micro-batches, over-partitioned table, after compaction | file count and size histogram, query planning time |
| Orchestrator scheduling | worker slots, pool size, task duration, retries, upstream failure, backfill range | normal day, 30-day backfill, flaky upstream, pool starvation | Gantt chart of task runs, queue depth |
| Change data capture into a warehouse | change rate, batch interval, share of updates and deletes, out-of-order changes | inserts only, heavy updates, late and out-of-order changes, schema change | freshness lag, rows rewritten per merge |
