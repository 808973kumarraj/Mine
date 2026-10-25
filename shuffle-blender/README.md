# Spark shuffle, isometric, made in Blender

The same 80-second shuffle explainer, rebuilt in **Blender 4.2 LTS** (free, open source, GPL):
real 3D boxes seen through an orthographic isometric camera, 3D text lying on the floor and
printed on the box fronts, keyframed motion, Freestyle outlines and flat illustration shading.

| File | What it is |
|---|---|
| `shuffle.py` | builds the whole scene and renders it (`blender -b --factory-startup -P shuffle.py -- OUT_DIR`) |
| `fonts/` | JetBrains Mono, SIL Open Font License (`fonts/OFL.txt`) |

Render options: `--frames A-B` renders part of the range (run several Blender processes in
parallel on different ranges); `--still 12,23.5` renders single frames at those seconds.
The soundtrack is music only, composed for this version: `audio/score.py` writes the score as MIDI, SoundCraft plays and renders it (`audio/score.scraft`), FilmCraft encodes it to `audio/music.m4a` at −16 LUFS. It has a chime on every scene change.

Not rendered yet: a full render takes about 2 hours on a 4-core CPU without a GPU (about 3.4 s per
frame with four parallel processes). The finished animation is `../shuffle-iso-standalone.html`.

## In the browser, straight from Blender

`../shuffle-blender-standalone.html` plays the Blender scene live, without any video:
`blender -b --factory-startup -P shuffle.py -- x --export web/shuffle.glb` writes the scene as glTF
(`web/shuffle.glb`) plus its keyframes (`web/shuffle.anim.json`), and `web/src.html` plays them with
three.js r147 (MIT) using the same isometric camera, flat shading and outlines. Pack it into one file with
`python3 web/build.py LIBDIR fonts.css ../shuffle-blender-standalone.html` (see `web/build.py`).
