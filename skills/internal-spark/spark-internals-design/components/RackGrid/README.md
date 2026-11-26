# RackGrid

A cluster seen from above at 30°: a square of isometric rack tiles, each with vents and an LED. Healthy racks bob gently; when the job slows, some turn amber and a few stragglers go red on a pink tile.

**You provide** `size` (tiles per side, 10 for "100 executors"), `scale`, `slow`, `stragglers` (share of red tiles, default 0.13) and `animate`.

- Use it to show scale at the start of an episode, then cut to four `Machine`s for the detail.
- Tiles drop in back to front with a dashed blue ghost of their top that fades as they land.
