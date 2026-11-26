# Scrubber

The episode timeline: a range input in amber with a tick at every segment start, grey for scenes and a taller amber tick for beats.

**You provide** `max` (total seconds), `value`, `ticks` (numbers, or `{ at, beat }`), and `onChange(seconds)`.

- EP. 07 runs 77 seconds: nine scenes and four beats.
- Ticks are decoration; the input carries the label "Seek".
