# Spark shuffle, 2.5D isometric

`../shuffle-iso-standalone.html` is the finished explainer: one self-contained file that opens
offline. Everything in it was made with the six ArtCraft creator apps (Linux builds of the
versions `qualixar/craft-studio-setup` pins):

| App | Made | Files |
|---|---|---|
| VectorCraft 0.14.0 | 18 isometric sprites (servers, trays, cubes, disk, switch, belt, table rows) and the icons, cleaned to plain SVG | `art/make_art.py` → `art/raw/` → `art/clean/`, `icons/` |
| PhotoCraft 0.12.0 | paper texture (noise + blur), then the lossy WebP | `assets/paper-raw.png`, `assets/paper-bg.webp` |
| LightCraft 0.11.0 | warm grade, vignette and grain on the texture | (between the two PhotoCraft steps) |
| EffectCraft 0.14.0 | animated end card, exported as Lottie | `effectcraft/endcard.jsx` → `endcard.json` |
| SoundCraft 0.11.0 | 100 BPM music bed (pad, bass, arpeggio) from a MIDI score | `audio/make_midi.py` → `bed.mid` → `bed.scraft` |
| FilmCraft 0.10.0 | voice + music mix, music ducked 14 dB under the voice, exported at −16 LUFS | `audio/mix.fcproj` → `audio/mix.m4a` |

The page itself (`src.html`) places the sprites on an isometric floor and animates them with
GSAP, clocked by the FilmCraft mix. Rebuild the single file with:

    python3 build.py gsap.min.js lottie.min.js fonts.css ../shuffle-iso-standalone.html

(gsap 3.12.5 and lottie-web 5.12.2 from cdnjs; fonts.css is the Google Fonts stylesheet for
JetBrains Mono 400/700/800.)
