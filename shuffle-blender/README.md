# Spark shuffle, isometric, made in Blender

The same 80-second shuffle explainer, rebuilt in **Blender 4.2 LTS** (free, open source, GPL):
real 3D boxes seen through an orthographic isometric camera, 3D text lying on the floor and
printed on the box fronts, keyframed motion, Freestyle outlines and flat illustration shading.

| File | What it is |
|---|---|
| `shuffle.py` | builds the whole scene and renders it (`blender -b --factory-startup -P shuffle.py -- OUT_DIR`) |
| `shuffle-blender.mp4` | the rendered video, 1080×1100, 24 fps, with the voice + music mix |
| `shuffle-blender.webm` | the same, smaller (AV1/Opus) |
| `fonts/` | JetBrains Mono, SIL Open Font License (`fonts/OFL.txt`) |

Render options: `--frames A-B` renders part of the range (run several Blender processes in
parallel on different ranges); `--still 12,23.5` renders single frames at those seconds.
The soundtrack is the FilmCraft mix from `../shuffle-iso/audio/mix.m4a`.
