# Spark Shuffle (Episode 3): recreation spec

Everything measured from the source video (`spark.mp4`, md5 `a81484aa…`) with Claude Eyes, ffmpeg and pixel sampling.
The standalone build that uses this kit is `../spark-shuffle-explainer.html`.

## Format
- 1080×1920 portrait (9:16), 30 fps, 80.5 s, H.264 + AAC
- 18 shots (13.4 cuts/min), first word at 0.0 s, speech for 100% of the runtime
- Audio: −14.0 LUFS integrated, true peak −4.2 dBFS, 2.6 LU range, no beat detected (voice-led)
- Voice: 170.8 words/min

## Layout (fractions of the 1080×1920 frame)
| Region | Position |
|---|---|
| Header text `> BINARY HABIT / SPARK PERF / 03` + amber block cursor | top ≈ 3%, left/right margin ≈ 8% |
| Header rule (blue, partial width) | just under the header, ≈ 4.5% |
| Animation area | 6% → 57% |
| Presenter panel (rounded, soft shadow) | x 86→1000 px (8.6%→91.4%), y 1160→1792 px (60.4%→93.4%) |
| Full-screen presenter | whole frame, the header stays drawn by the video |

## Full-screen presenter windows
`6.6–7.9 s`, `29.1–31.6 s`, `54.5–57.3 s`, `75.4–77.5 s`. Everywhere else the presenter shows in the bottom panel.

## Palette (sampled)
| Token | Hex | Use |
|---|---|---|
| paper | `#eae7da` | background, faint 24 px grid a shade darker (`#dddbce`) |
| card | `#f7f6f0` | executor and partition boxes, with a 1.5 px `#c9c7bb` edge and a 5 px solid drop edge |
| ink | `#1d1d1b` | text and the terminal background |
| blue | `#284878` | IN chips, the header rule, partitions and data blocks |
| red / clay | `#884838` → `#b2412e` | "slowing down", cost, strikes |
| amber | `#c88848` | cursor, BR chips |
| green | `#2f7a5b` | progress bars, "all running", ✓ pills |

## Typography
- A monospace face everywhere (close match: JetBrains Mono / Space Mono), bold for titles and numbers
- Big counters (`100`, `1,000,000,000`) ≈ 5% of the frame height; tiny letter-spaced uppercase labels under them
- Pills: a rounded outline in the accent color, 9–11 px at 540 px wide

## Shot list and motion
| Time | On screen | Motion |
|---|---|---|
| 0:00–0:01 | `> BIN` header typing | typewriter + block cursor |
| 0:01–0:04 | `100 SPARK EXECUTORS`, isometric 10×10 grid of boxes, "● ALL RUNNING" | counter 0→100; boxes drop in as a diagonal wave |
| 0:04–0:06 | `df.groupBy("country")` terminal | pops in above the grid |
| 0:05–0:07 | job card `SPARK JOB · STAGE 2 OF 3 / groupBy · count`, clock 00:06→02:05, 44%→63%, "tasks waiting on data…", "SLOWING DOWN" | bar races, then crawls; dots turn red |
| 0:07–0:08 | full-screen presenter | hard cut |
| 0:08–0:14 | `1,000,000,000 ORDERS`, executors 1–4 + "+ 96 more executors", "✓ PARALLEL · NO PROBLEM" | counter spin; boxes pop in one by one; blocks stack in; bars fill |
| 0:14–0:19 | `>>> df.groupBy("country").count()` | typewriter |
| 0:19–0:25 | "same key → same partition", partitions IN/US/BR/DE under the executors | chips arc along dashed paths, one country at a time, and the target partition tints blue |
| 0:25–0:29 | "data moves between executors" → SHUFFLE | same arcs, all keys |
| 0:29–0:31 | full-screen presenter | |
| 0:31–0:46 | SHUFFLE COST, STEP n/6: ORDER RECORD (order_id 48213907, country IN, €1,240) → serialize (binary) → write to disk (+ disk I/O) → send over network (+ network I/O) → deserialize → aggregate | the record card travels from executor 4 to executor 2; the disk and network icons pulse |
| 0:47–0:54 | "shuffle overhead", STAGE 1 → "stage 2 waits for all of this", STAGE 2 TASK 1–4 "waiting for data…" | rows slide in; animated ellipsis |
| 0:54–0:57 | full-screen presenter | |
| 0:57–1:03 | pipe with a narrow neck, "bottleneck"; "− less data" ✓ flows vs "+ more executors" ✗ still stuck | blocks jam at the neck, a few blocks stream through |
| 1:03–1:07 | "BEFORE THE SHUFFLE: filter rows + columns", isometric table, a SHUFFLE tile | rows, then columns fade away |
| 1:07–1:10 | "avoid unnecessary shuffles", the "extra shuffle" card struck through, "✓ less data moved · faster job" | strike draws on; card tips flat |
| 1:10–1:14 | "LESS DATA MOVEMENT: a powerful trick…", scattered executor boxes | a small table drops between the executors |
| 1:14–1:17 | full-screen presenter ("NEXT EPISODE: BROADCAST JOIN") | |
| 1:17–1:20 | end card: `DATA / 03`, **Spark Performance**, Episode 3 · Shuffling, IN/US/BR/DE pills (IN filled), **BinaryHabit.** | fade up |

## Files in this kit
- `frames/tNNN.N.jpg`: reference stills (540 px wide) at every key moment, for matching layouts by eye
- `voice.m4a`: the full voice track
- `presenter-panel.mp4`: the bottom presenter panel cropped out (no audio)
- `words.json`: word-by-word transcript with timestamps
- `captions.json`: `[start_seconds, text]` chunks of about 5 words, with the transcription slips fixed (groupBy, data movement, sitting on)
- `transcript.txt`: the transcript line by line
- `dna.json` / `dna.md`: Eyes' measured style data (pacing, captions, audio, color)
