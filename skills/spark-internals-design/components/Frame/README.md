# Frame

The paper the whole series is drawn on: a 470 × 835 graph-paper frame (14px and 70px grids, a warm vignette, 14px corners) that sets `--k` to its width ÷ 470.

**You provide** `children` (usually a docked `Header`, a canvas and a docked `Deck`) and optionally `width`.

- Put it on the `stage` color. Nothing else sits on the stage.
- Every chrome size inside scales by `--k`, so a 300px frame is the 470px frame at 0.64.
- For a page that only needs the graph paper, use the `si-paper` class instead.
