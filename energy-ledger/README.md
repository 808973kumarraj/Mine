# The Energy Ledger

An interactive 3D pop-up notebook about energy stores, as one standalone HTML file
(`../energy-ledger.html`, about 1.1 MB, works offline).

1 coin = 1 joule. Each page has a model to play with and a ledger of coin stacks that
always adds up to the same total:

| Page | Model | Stores |
|---|---|---|
| The swing | drag the ball, let go, turn air resistance on | gravitational, kinetic, thermal |
| Slide and stop | a sledge slides down a 1 m slope onto plastic, carpet or sandpaper | gravitational, kinetic, thermal |
| Light and heat | a torch with a filament bulb or an LED | chemical, light, thermal |
| Spread thin | the same 20 J spreading through a room over 10 minutes | hot bulb, warm wall, whole room |
| The books balance | a balance scale: before = after | |

## Build

    python3 build.py            # writes ../energy-ledger.html

`src/index.html`, `src/style.css` and `src/app.js` are the notebook. `build.py` inlines the
fonts and the libraries so the page needs no server and no network.

URL options for testing: `?p=3` opens at page 3, `?shot=1` stops the idle camera sway.

## Credits

- three.js r147 and its LineSegments2 add-ons (MIT), in `vendor/`
- GSAP 3.12.5 (GreenSock standard license), in `vendor/`
- Newsreader and Caveat fonts (SIL Open Font License), in `fonts/`
