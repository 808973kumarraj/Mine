# 3D Home

A 3D view of a house, generated from its floor plan, in **one HTML file** you can open on a phone or
a computer: `dist/Home_3D_Minimalist_Concrete.html`.

- Look at the house from every side, lift the roof, or cut the walls to see inside
- Walk through every room (joystick + drag on a phone, W A S D + mouse on a computer)
- Play a guided tour of the whole house
- Day / night switch
- Soft, realistic interior light that is calculated in the browser when the page opens
- Optional "Photo" mode that slowly renders a physically accurate picture while you hold still

Everything is drawn with code. There are no model, image or texture files, nothing is downloaded when
the page runs, and the HTML is plain readable code (no minified or base64 parts).

## Change it and rebuild

```
npm install
npm run build
```

- `src/main.js` — the app
- `src/template.html` — page layout and controls
- `build.mjs` — puts the libraries and the app into the single HTML file
- `CLAUDE.md` — project notes and the owner's rules (read this first)

## Libraries

Bundled, unminified, under the MIT licence: [three.js](https://threejs.org),
[three-gpu-pathtracer](https://github.com/gkjohnson/three-gpu-pathtracer),
[three-mesh-bvh](https://github.com/gkjohnson/three-mesh-bvh).
