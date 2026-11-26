# StepList

A pipeline as a checklist: done steps get a green box and a white check, the current step an amber box filling up, the rest a muted outline, each with its cost on the right in red.

**You provide** `steps` (`{ label, cost }`), `current` (index) and `progress` (0–1 through the current step).

- Labels are lowercase verbs or nouns of the pipeline: "serialize", "network fetch".
- Costs are deltas in the stat's unit: "+0.9".
- The current label is ink 800, not amber: amber text on paper reads 3.0:1, which the episode accepted and this system doesn't.
