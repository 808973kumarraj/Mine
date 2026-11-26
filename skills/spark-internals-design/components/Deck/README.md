# Deck

The studio deck: a dark panel docked at the bottom of the frame that carries the narration word by word, with the segment label in amber and the clock on its top line.

**You provide** `text` (backticks make code pills), `current` (index of the word being spoken; earlier words are spoken, later ones dim), `label`, `clock`, and the controls as `children`. `beat` switches to a beat; `docked` pins it inside a `Frame` at 37% height (85% on a beat).

- Words move from `deck-unspoken` to `amber` (now) to `deck-text` (spoken). `deck-unspoken` is dim on purpose: never use it for text that stays.
- A beat sets one sentence in `beat` type (Newsreader 42px), centered.
- The deck keeps a 3.5px cream border and 18px corners, also on a dark page.
