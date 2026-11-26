# Machine

Every computer, stage and platform in the series: an oblique box whose front face rises 8.5% across its width and whose depth recedes up and to the left, outlined in 1.1px ink, with dividers, vents, status LEDs and a label on the front.

**You provide** `label`, `leds` (token names, right to left), and the box's `w`, `h`, `depth`; optional `dividers`, `vents`, `color` (default `node`), `pattern` (a hatch), `wire` (a dashed blue outline for something not yet there) and `animate` (LEDs pulse).

- Executors are 78 × 56 × 40 with one divider, five vents and one LED, spaced 102px apart.
- LEDs: `green` healthy, `amber` busy or waiting, `red` straggling.
- Machines enter by dropping 170px with an overshoot (`ease.outBack`).
