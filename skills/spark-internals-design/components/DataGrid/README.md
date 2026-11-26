# DataGrid

A small table drawn on the oblique (cells 50 × 17, skewed to match the machines) that shows what a filter and a select keep: kept rows tinted green, dropped rows tinted red, dropped columns greyed, then the table that is left.

**You provide** `columns`, `rows` (strings, or `{ v, tone }` cells), `keepRows`, `keepColumns`, and `filtered` to show only what is kept. `oblique={false}` draws it flat.

- Four to six rows, five columns at most: it is a picture of the data, not the data.
- Pair it with the blue `.filter(…)` and `.select(…)` pills and a `delta` stat ("1.0B → 0.38B").
