# Prompt: Interactive Animated Short (three.js, built in the background)

Based on wluha's (@cryptowluha) field guide "How People Make Animated Films With Opus 5.5". It uses Route 3 (one three.js scene, fixed shot by shot) and the guide's seven habits. The output is a **live interactive screen in the browser**, not a Claude artifact and not a rendered video.

Paste everything below the line into Claude Code.

---

You are building an interactive animated short as a local web app. Work **autonomously in the background**: don't stop to ask me questions and don't publish an artifact. Write the files to the repo, run them, check them and fix them yourself. Report back only when it's done.

## Deliverable
- `scene/index.html`: one self-contained page. Three.js comes from a pinned CDN version. No build step.
- `scene/README.md`: how to run it (`npx serve scene` or `python3 -m http.server`) and the controls.
- Commit and push when finished.

## The story (swap in your own)
A small orange vinyl toy (#D97757) waters plants on a sunny windowsill. The can is empty at the big cactus, *determined*. Then it gurgles and splashes the toy in the face, *surprised*. The second try works and the cactus blooms, *happy*. The toy leans in to smell the flower, *joy*. The flower sneezes it into the next pot, *confused → laughing*. Total about 13 seconds at 60 fps.

Every beat line ends with one feeling word. Read only the feeling words in order. If they still tell the story, the script is OK.

## Interactivity (this is a screen, not a video)
- Play / pause / scrub timeline, with chapter markers for each beat. Keys: space, ←/→ to step one frame, 1–5 to jump to a beat.
- Orbit the camera freely when paused. A "Director cam" toggle returns to the authored camera path.
- Click the toy and it reacts with a blink, a look, and a small squash. Click a plant and it wiggles.
- Hover any object to see its name. A small HUD shows the current beat and its feeling word.
- Speed control (0.25×, 0.5×, 1×). Sound on/off. The sound is synthesized with WebAudio, not sample files.
- Responsive. Must work at phone width with touch controls.

## Craft rules (from the guide)
1. The character wants something small, it goes wrong in a way it didn't see coming, and it ends by looking at the camera.
2. The eyes react 6–10 frames **before** the body moves.
3. Nothing is ever fully still. Breathing and blinks keep running, even in held shots and while paused in interactive mode.
4. Something new happens every 2–3 seconds.
5. Sound lands **one frame after** the thing that makes it.
6. Keep water, smoke and hair small and shiny. Use soft sprites, not piles of cubes. Don't build complex props from primitives when a simple stylized form reads better.
7. Lock the look first: one palette, one light setup (warm window key, cool fill), soft shadows.

## Process (do all of it yourself, in the background)
1. **Stills before motion:** build the static scene. Use Playwright (pre-installed) to screenshot it, compare it with the brief, and fix the gaps. Do 3 rounds before you animate anything.
2. **Animate**, then capture **8 still frames**, one per key moment. Review them yourself and write a note that starts with *what to keep*, then *what to fix*. Rebuild. Do a second round of 8 frames and a second note.
3. **Measure, don't eyeball:**
   - Check every frame for intersections between the toy, can, pots, sill and camera. Log any frame where objects pass through each other and fix it.
   - Sample the toy's average rendered color and keep it close to #D97757.
   - Verify that every sound event fires one frame after its visual cause.
   - Confirm a steady 60 fps on the timeline and that there are no console errors.
   - Test the interactions (click, scrub, keys, touch) with Playwright.
4. Save both review notes and the measurement results in `scene/NOTES.md`.

## Done means
The page opens and plays the full short. Every control works on desktop and at phone width. The checks above pass. NOTES.md records both review rounds. Everything is committed and pushed. Then give me a short summary: what you built, what the measurements found, and anything still rough.
