# Prompt: new explainer animation in the style of the attached HTML

Attach one of the existing pages (for example `delta/index.html` or `shuffle/index.html`) to the chat,
fill in the three fields at the top, then paste everything below the line.

---

**TOPIC:** [what the video explains, e.g. "How Kafka partitions work"]
**SERIES LINE (header):** [e.g. `> BINARY HABIT / KAFKA / 01`]
**LENGTH:** [e.g. about 70 seconds]

---

I have attached an HTML file. It is a vertical (1080 × 1920) animated explainer video built as a single
web page: inline SVG drawn by JavaScript and driven by one GSAP timeline. I want a **new HTML file in exactly
the same style and architecture** that explains the TOPIC above. Treat the attached file as the design system
and the code template: reuse its look, its helpers and its structure; only the scenes and their content are new.

## 1. Study the attached file first

Before writing anything, read the attached file and identify:

- the stage setup (SVG `viewBox="0 0 1080 1920"`, background gradient, faint grid pattern, `defs` with
  patterns and blur filters);
- the color palette constant (`C = {...}`) and the fonts loaded from Google Fonts;
- the low-level helpers (creating elements, setting attributes, opacity, easing/progress, keyframes,
  color mixing, number formatting, perspective helpers such as `faceMatrix`, `homography`, `boxGeom`);
- the visual building blocks (machine boxes, trays/folders, tiles, file cards, cards and panels, pills/tags,
  the terminal with typing and a blinking cursor, the curved "rail" connector, the step title block, the
  big display word with a blue underline, the checklist panel, the end card);
- the `scene(id, build, draw)` pattern and the single `render(t)` function;
- the header (typed breadcrumb, blinking orange cursor, blue progress rule) and the loop back to the
  opening frame;
- the boot code (font loading, `?t=`, `?external=1`, `data-ready`).

Copy all of this unchanged into the new file. Do not rewrite the helpers in a different style, do not
switch libraries, and do not "improve" the design system.

## 2. Keep the technical contract

The new file must keep the exact same runtime contract as the attached one:

1. **One self-contained HTML file.** Inline SVG only. External resources: GSAP 3.12.5 from cdnjs
   (`https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js`) and the same Google Fonts link. Nothing
   else: no images, no `<img>`, `<video>`, canvas, raster data URIs, other libraries or other CDNs.
2. **One clock.** A single paused GSAP timeline tweens a `clock.t` from 0 to `DUR`; its `onUpdate` calls
   `render(clock.t)`. No `requestAnimationFrame` loops, `setTimeout` choreography, CSS animations or
   `Date.now()` driving anything visible.
3. **Pure function of time.** `render(t)` calls every scene's `draw(t)`. Each draw function sets every element
   it owns from `t` alone (position, opacity, text, color), so seeking to any time, in any order, always gives
   the same image. Randomness only through the file's seeded `rnd()`, used at build time, never at draw time.
4. **Expose the same API:**
   `window.anim = { tl, duration: DUR, seek(t), play(from), pause() }`.
   `index.html?t=12.5` freezes at 12.5 s, `?external=1` does not autoplay, with no parameter it plays and loops.
   Set `document.documentElement.dataset.ready = '1'` after fonts are loaded and frame 0 is drawn.
   Keep the guard that ignores `render()` calls before the build is finished.
5. **Seamless loop.** The frame at `t = DUR` must equal the frame at `t = 0`. Keep the attached file's
   approach: the opening frame is the empty grid with the header typing in; in the last ~0.7 s the end card
   fades out and the header text rubs out and the progress rule returns to zero.
6. **Set** `DUR` to the new length and the header text constant (`HDR`) to the SERIES LINE. Update the
   `<title>` and the comment block at the top of `<body>` to describe the new video.

## 3. Keep the visual language

- **Layout zones (stage coordinates):** header at y ≈ 196–236; step label at y ≈ 288 and step title at
  y ≈ 350, left aligned at x ≈ 128; optional side panel top-right (x ≈ 703–965, y ≈ 260–484); main diagram
  between y ≈ 450 and 1500; tags/pills and captions between y ≈ 1200 and 1660; keep a 60 px side margin and
  leave the bottom ~250 px quiet. Nothing may be cut off at the frame edges.
- **Fonts:** Space Mono bold for titles and labels on boxes, IBM Plex Mono for small caps labels, code, tags
  and captions, Silkscreen only for the header and the small "DATA / NN" line, Archivo condensed extra-bold
  only for the one big display word, IBM Plex Sans only for the end-card subtitle.
- **Colors carry meaning:** green = works / good / done, red = problem / cost / failure, blue = the key idea
  of the episode (key, log, current version), orange = cursor and small accents, ink and paper tones for
  everything else. Flat fills; only the soft shadows and gradients already defined in the file.
- **Building blocks:** draw objects with the existing helpers (boxes in perspective with a label, an LED and
  ruled lines; trays; tiles; file cards with a colored tab; cards; tags; rails; the terminal). Every object is
  drawn around its own origin and placed with transforms. Small isometric/perspective scenes are good;
  realistic illustration is not.
- **Motion vocabulary (timings):** scene fade in ~0.3 s, fade out ~0.35 s with a short overlap; objects pop
  in with `back.out` over ~0.25 s or drop in from 60–100 px above with `power2.in`; things that travel follow
  an arc (`fly(from, to, p, arcHeight)`); checklist ticks fill in ~0.2 s; titles fade from light gray to ink
  in ~0.35 s; typing runs at ~20–22 characters per second; counters roll with `power2.out`. Use absolute
  times from the scene plan, never relative chains, so one beat can be retimed without moving the others.
- **One idea per scene,** 5–15 s each, with a step label ("STEP 2 / 4", "THE PROBLEM", ...) and a short
  title of at most ~20 characters so it never runs into the side panel.

## 4. Content rules

- Explain the TOPIC for a developer audience in 7–10 scenes: hook (what people think it is) → the problem →
  the core idea named in the big display word → how it works step by step → one or two important details or
  edge cases → the payoff ("what you get") → end card. End card: "DATA / NN", the topic as title, a one-line
  subtitle, 3–4 small chips with keywords, and the "BinaryHabit." wordmark, as in the attached file.
- **Be technically correct.** Use real API names, real file names, real defaults. If you are not sure about a
  detail, leave it out instead of guessing. Numbers that are only illustrations (row counts, file counts)
  must look like examples, not claims about a real system.
- Keep on-screen text short: titles ≤ ~20 characters, tags ≤ ~35 characters, captions ≤ ~70 characters,
  code lines ≤ ~35 characters (they must fit in the terminal).

## 5. How to answer

1. First output a **scene plan** as a table: time range, step label, title, what is drawn, what moves and when
   (absolute seconds), tags/captions. The scene times must add up to the LENGTH and include the end card and
   the loop return.
2. Then output the **complete HTML file** in one code block: everything from `<!doctype html>` to `</html>`,
   with no placeholders, no "...", no "same as before" and no omitted helpers. Each scene starts with a comment
   banner giving its time range and a one-line description, like the attached file.
3. If the file is too long for one reply, stop at the end of a complete scene, write `CONTINUE FROM SCENE X`,
   and continue in the next reply when I say "continue", without repeating earlier parts.

## 6. Check before you answer

Go through this list and fix anything that fails:

- [ ] Only GSAP from cdnjs and the Google Fonts link are loaded; no images, video or canvas.
- [ ] `window.anim`, `?t=`, `?external=1` and `data-ready` work exactly as in the attached file.
- [ ] Every draw function hides its scene outside its time range and is a pure function of `t`.
- [ ] No `Math.random()` at draw time, no timers, no CSS animations on the stage.
- [ ] Frame at `DUR` equals frame at 0 (end card fades, header rubs out, progress rule back to zero).
- [ ] No two scenes are fully visible at the same moment except during the short crossfades; no frame
      is blank for more than ~0.3 s except the opening and the loop return.
- [ ] Nothing overlaps unintentionally: titles vs. side panel, tags vs. diagrams, text vs. frame edges.
- [ ] All text uses the fonts and sizes of the attached file; all colors come from the palette.
- [ ] The technical content is correct and the scene plan matches the code timings.
