# TaskBars

Where the time goes around a stage barrier: grey hatched bars for waiting, green hatched bars for real work, and a dashed red line where nothing can start until the slowest task ends.

**You provide** `tasks` (`{ label, wait, work }`, each 0–1 of its side of the barrier), `barrier` (0–1 across the track) and the barrier's `label`.

- The legend ("waiting", "real work") is always shown; the bars also differ in pattern, not only color.
- Pair it with an `Hourglass` while the barrier holds.
