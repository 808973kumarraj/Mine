# Prompt: an interactive simulation of any topic

This is the prompt behind `birdsong.html`, rewritten so it works for any subject.
Fill in the **topic sheet** at the top, leave the rest as it is, and paste the whole block into a chat.
The more specific the topic sheet, the better the result. A filled-in example follows the prompt.

```text
Build an interactive simulation that lets someone understand [TOPIC] by playing with it.

TOPIC SHEET (this is the part I filled in)
- Topic: [what it is, in one line]
- Who it is for: [e.g. curious adults with no background / high-school students / new engineers]
- The one idea they should leave with: [a single sentence; everything on screen should serve it]
- The subject to draw: [the thing itself, e.g. "a songbird on a branch, seen from the side"]
- The hidden mechanism to expose: [what a cut-away or x-ray view should reveal]
- Parts the person can grab (5 to 8): for each one give its name, what dragging it changes,
  and what that does to the result.
    1. [name] - drag to [action] -> [effect]
    2. ...
- Cases to compare (5 to 7): real examples that use the same mechanism differently,
  each with a one-line description of what makes it distinct. [list them]
- Live read-out: [the graph or picture that shows the result as it happens,
  e.g. a scrolling spectrogram, a pressure-volume loop, a waveform, a map]
- Sound: [yes, synthesised from the model / no]
- Playground mode: [an invented, exaggerated case with its own control panel where the person
  can drive every parameter directly, e.g. a keyboard, sliders and presets / none]

WHAT TO BUILD
One screen with four areas:
1. Centre: an illustration of the subject with a cut-away window showing the mechanism.
   Each grabbable part has a small round handle. Dotted leader lines run from the handles to
   labels beside the drawing. Each label has the part's name and, under it, a short line that
   changes live: a reading with units when the part is working ("Singing - 2.4 kHz"), a state
   in plain words ("Nearly closed - Pure whistle"), or a hint when it is untouched
   ("Drag to open or close").
   Grabbing a part must feel physical: the part gets a thick outline in the accent colour, its
   handle becomes a filled dot with a ring, the leader line and label take the same colour, the
   handle follows the pointer freely up to a short reach, and the part visibly stretches or
   squeezes with it. A part that needs an input to work (air, power, pressure) should look
   slack and pale until that input arrives.
2. Left: a play/pause button and a list of the cases. Picking a case redraws the subject in that
   case's own colours and shape and switches to its behaviour.
3. Bottom: the live read-out, drawn from what the model is actually producing, with labelled axes.
   If two or more sources contribute, give each its own colour.
4. Right, only in playground mode: the control panel (presets, sliders with units, mode buttons).

Two ways to use every case:
- Demo: the case runs by itself in a loop, starting as soon as the page opens (silently if the
  browser has not allowed sound yet, with a one-line note saying how to switch it on). The parts
  move as they really would, so the person can see cause and effect. Dragging a part while it
  runs bends the behaviour immediately.
- By hand: nothing happens until the person supplies the driving input (for example, squeezing
  the air sacs). From there every part is theirs to move and the handles stay where they leave them.
  Double-clicking a handle resets it.

HOW IT SHOULD WORK INSIDE
- Start from a model, not from animation. Keep one small state object (the physical quantities)
  and compute the picture, the labels, the read-out and the sound from it every frame. A demo is
  then just a function of time that writes into that state, and a dragged handle is an adjustment
  applied on top. This is what keeps the picture, the numbers and the sound in agreement.
- Get the directions of every relationship right and keep magnitudes plausible, with real units
  in the labels. Where you simplify, simplify the maths, not the cause and effect. If something is
  a rough approximation, say so in a code comment.
- Make each case recognisably different in behaviour, not only in colour.
- If there is sound, generate it with the Web Audio API from the same state, schedule it slightly
  ahead so fast patterns stay tight, and start audio only after the first click, tap or key press.

LOOK
A calm scientific-illustration style: warm paper background, a serif typeface, thin outlines,
flat colour with soft shading, muted palette with one accent colour for whatever is active.
Draw everything as SVG or canvas in code. No stock icons, no emoji, no gradients for decoration.

CONSTRAINTS
- One self-contained .html file: plain HTML, CSS and JavaScript. No libraries, no build step,
  no external requests, no embedded images, fonts, audio or base64 data. I want to be able to
  read and edit every line.
- Lay the screen out at a fixed design size and scale it to the window. On a narrow or portrait
  screen switch to a single column: case list on top, the drawing filling the width, the read-out,
  then the labels as a list. Handles need touch targets of at least 40 px and must not scroll
  the page while dragged.
- Hold 60 frames per second on a large high-resolution screen as well as a phone. Redraw only
  what changes. If the still artwork uses textures or filters, paint it once into a canvas and
  animate on a separate layer above it; filtered artwork that is redrawn every frame is slow.
- Pause sound when the tab is hidden.
- Comment the code by section, explaining what each part models.

ORDER OF WORK
1. Before writing code, list the state variables, the equations or rules linking them, and the
   demo sequence for each case, and show me that list.
2. Build the model and the read-out first and check them with numbers.
3. Add the drawing and handles, then the cases, then the playground panel.

CHECK BEFORE YOU CALL IT DONE
Open the page in a browser and confirm, case by case:
- the demo runs and the read-out shows that case's distinctive pattern;
- dragging each handle changes the label, the read-out and the sound in the expected direction,
  with a mouse and with a finger (the page must not scroll while a handle is held);
- by-hand mode is silent and still until the driving input is supplied;
- there are no console errors, and nothing is requested from the network;
- it works at 1366 x 768 and at 390 x 844.
Tell me plainly what you verified, what you could not verify, and which parts of the model
are simplified.
```

## The topic sheet that produced `birdsong.html`

```text
- Topic: how a songbird sings with two independent voices (the syrinx)
- Who it is for: curious adults with no biology background
- The one idea: a bird has two sound sources, one on each side, and can sing two notes at once
  or hand a note from one side to the other
- The subject to draw: a songbird on a branch, side view
- The hidden mechanism: a cut-away of the chest showing windpipe, syrinx and air sacs
- Parts the person can grab:
    1. Belly air sacs - squeeze up -> blows air; nothing sounds without it
    2. Left voice - pull -> bends the low note
    3. Right voice - pull -> bends the high note
    4. Center splitter - slide -> favours one side and mutes the other
    5. Chest air cushion - pull -> adds buzz and grit
    6. Windpipe - stretch or widen -> moves the tube's resonance
    7. Throat chamber - drag -> warm and pure versus tight and bright
    8. Beak - drag -> open and bright versus clamped and muffled
- Cases: Wood Thrush (two-voice flute chord), Northern Cardinal (one sweep handed between sides),
  Waterslager Canary (left-side rolls), Zebra Finch (buzzy harmonic stacks),
  Common Nightingale (soft-to-loud whistles then a trill), Superb Lyrebird (mimicry and clicks)
- Live read-out: scrolling spectrogram, 0 to 8 kHz, left voice teal, right voice red
- Sound: yes, synthesised from the model
- Playground mode: "SynthBird", a mechanical bird played from a keyboard, with six presets,
  sliders for breath, glide, detune, grit, pluck, tube length, throat wah and an LFO
```

## Other topics that fit the same shape

| Topic | Parts to grab | Cases to compare | Live read-out |
|---|---|---|---|
| How the heart pumps | valves, ventricle wall, pacemaker rate, vessel stiffness | resting, sprinting, athlete, leaky valve, high blood pressure | pressure-volume loop and ECG trace |
| How a jet engine makes thrust | fan, compressor stages, fuel flow, nozzle | turbojet, turbofan, afterburner, turboprop | pressure and temperature along the engine |
| How the ear hears | eardrum, middle-ear bones, basilar membrane, hair cells | whisper, speech, music, loud concert, age-related loss | which part of the cochlea responds, by frequency |
| How a guitar string sounds | pluck position, tension, length, body size, pickup | nylon, steel, bass, harmonic, slide | waveform and harmonic spectrum |
| How a volcano erupts | magma supply, gas content, viscosity, vent width | Hawaiian, Strombolian, Plinian, lava dome | chamber pressure and plume height over time |
| How a camera exposes a photo | aperture, shutter, sensor gain, focus | portrait, night, sport, landscape | histogram and a live preview with depth of field |
