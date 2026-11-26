# Episode

The whole player: the 470 × 835 frame, a canvas for scenes, the header, and the studio deck with word-by-word narration, a scrubber and step, play, expand and voice controls.

**You provide** `segments`: an ordered list of scenes and beats. A scene is `{ name, dur, text, draw(K, t, dur, name, n) }`; a beat is `{ beat: true, dur: 2.5, text }`. Optional: `series`, `episode`, `autoplay` (default on, off under reduced motion), `storageKey` (remembers the playhead in this browser), `width`.

- `draw` paints the scene at local time `t` in a 470-wide coordinate space with `K`, the drawing kit (see the README's Drawing section). Start each scene with `K.title(n, NAME, t)`.
- Scenes run 6.5–9 s, beats 2.5 s. A beat freezes the previous scene's last frame and expands the deck to 85% with the sentence in `beat` type.
- Wrap code in backticks in `text`; it becomes a code pill.
- Narration timing is automatic: words light up in proportion to their length.
- Keys: Space plays and pauses, the arrow keys step, while the episode has focus.

Do: keep one idea per scene and one sentence per beat. Don't: put more than one big number on screen at a time.

The episode's own "⬇ .HTML" button is left out: artifact pages cannot start downloads.
