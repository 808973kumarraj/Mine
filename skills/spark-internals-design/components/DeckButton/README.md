# DeckButton

The deck's only button: transparent, uppercase 9px mono, a thin `deck-line` border that turns amber on hover and green (with a `deck-on` label) when toggled on.

**You provide** a short label as `children`, `onClick`, and `on` for toggles (it also sets `aria-pressed`).

- Labels are verbs or states: "← Step", "Pause", "Expand: Auto", "TTS: Off".
- Buttons share one row and stretch equally; long labels ellipsize.
- The border reads 1.5:1 on the deck, the episode's own value; the label is what shows it is a button.
