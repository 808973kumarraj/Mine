# How Browser Caching Works

An animated explainer for junior developers. It follows one file, `style.css`, through three
requests: a first visit (cache miss, `200`), a cache hit (no request), and an expired copy that is
revalidated with `If-None-Match` and comes back `304 Not Modified`. It runs 2:45 with captions,
and you can play it like a video or step through it one beat at a time.

Open `index.html` in a browser. It is one page; only GSAP loads from cdnjs.

## Controls

- **Space** or the play button: play and pause. Clicking the picture also toggles.
- **Left and right arrow keys** or **Prev step / Next step**: play one step and stop.
- **Request tabs and step dots**: jump to a request, or to one step inside it.
- **Pause after each step**: normal playback stops at the end of every step.
- **CC** (or **C**): captions on or off. The speed menu offers 0.75x to 1.5x.

## Files

| File | What it is |
| --- | --- |
| `brief.md` | Every claim shown or said, with its source. Example values are listed separately. |
| `beats.js` | The script: one beat per line of narration, grouped into chapters, with timing. |
| `src/stage.js`, `src/stage.css` | The scenes, drawn on a 1600x900 canvas that scales to fit. |
| `src/timeline.js` | The GSAP timeline. Every tween is placed relative to a beat. |
| `src/player.js`, `src/page.css` | Player bar, step bar, captions, and the page around the stage. |
| `build.js` | Writes `index.html`, `captions.srt` and `chapters.txt`. |
| `critique.js` | Headless check: screenshots each beat and reports page errors. |
| `assets/fonts/` | Bricolage Grotesque, IBM Plex Mono and Instrument Serif (SIL OFL 1.1, licenses included). |

## Rebuild and check

```sh
node build.js
# needs a local copy of https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js
NODE_PATH=$(npm root -g) node critique.js --gsap ./gsap.min.js --out ./shots
NODE_PATH=$(npm root -g) node critique.js --gsap ./gsap.min.js --out ./shots --reverse
```

To change what is said, edit `beats.js`. Timing follows the word count, so scenes keyed to beats
move with it. Then run `node build.js` again.
