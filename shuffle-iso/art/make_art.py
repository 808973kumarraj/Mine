"""Draw the isometric sprites as SVG (true 30-degree isometric). VectorCraft cleans each one up.

usage: python3 make_art.py OUT_DIR
Every sprite's anchor (its floor centre) sits at the middle of the bottom edge of the
diamond, at (W/2, H - D/2), as recorded in sprites.json for the page to place them.
"""
import json, math, pathlib, sys

C, S = math.cos(math.radians(30)), math.sin(math.radians(30))
INK = '#2b2721'
out = pathlib.Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
anchors = {}


def iso(x, y, z):                     # world (x right-back, y left-back, z up) -> screen
    return ((x - y) * C, (x + y) * S - z)


def poly(pts, fill, stroke=INK, sw=2.2, extra=''):
    d = ' '.join(f'{px:.2f},{py:.2f}' for px, py in pts)
    return f'<polygon points="{d}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}" stroke-linejoin="round"{extra}/>'


def line(a, b, stroke, sw=2, extra=''):
    return f'<line x1="{a[0]:.2f}" y1="{a[1]:.2f}" x2="{b[0]:.2f}" y2="{b[1]:.2f}" stroke="{stroke}" stroke-width="{sw}" stroke-linecap="round"{extra}/>'


def cuboid(w, d, h, top, left, right, sw=2.2):
    """w along x, d along y; returns the three visible faces centred on the floor origin."""
    P = lambda x, y, z: iso(x - w / 2, y - d / 2, z)
    return [
        poly([P(0, d, 0), P(w, d, 0), P(w, d, h), P(0, d, h)], left, sw=sw),       # front-left face (y = d)
        poly([P(w, 0, 0), P(w, d, 0), P(w, d, h), P(w, 0, h)], right, sw=sw),      # front-right face (x = w)
        poly([P(0, 0, h), P(w, 0, h), P(w, d, h), P(0, d, h)], top, sw=sw),        # top
    ], P


def save(name, parts, w, d, h, pad=14):
    xs, ys = [], []
    for x in (-w / 2, w / 2):
        for y in (-d / 2, d / 2):
            for z in (0, h):
                sx, sy = iso(x, y, z); xs.append(sx); ys.append(sy)
    minx, maxx, miny, maxy = min(xs) - pad, max(xs) + pad, min(ys) - pad, max(ys) + pad
    W, H = maxx - minx, maxy - miny
    body = ''.join(parts)
    svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W:.0f}" height="{H:.0f}" viewBox="{minx:.2f} {miny:.2f} {W:.2f} {H:.2f}">'
           f'{body}</svg>')
    (out / f'{name}.svg').write_text(svg)
    anchors[name] = {'w': round(W), 'h': round(H), 'ax': round(-minx), 'ay': round(-miny)}


# executor: a server block with vents, a status light and a drive bay
def server(name, top, left, right, led):
    w, d, h = 120, 90, 96
    parts, P = cuboid(w, d, h, top, left, right)
    for i in range(4):                                           # vent slots on the front-left face
        z = 26 + i * 12
        parts.append(line(P(14, d, z), P(70, d, z), '#9a917c', 2.4))
    parts.append(poly([P(80, d, 18), P(108, d, 18), P(108, d, 30), P(80, d, 30)], '#3a352c', sw=1.4))   # drive bay
    cx, cy = P(100, d, 74)
    parts.append(f'<ellipse cx="{cx:.2f}" cy="{cy:.2f}" rx="5" ry="4" fill="{led}" stroke="{INK}" stroke-width="1.4"/>')
    for i in range(3):                                           # rack lines on the right face
        z = 22 + i * 24
        parts.append(line(P(w, 12, z), P(w, d - 12, z), '#b3a991', 2))
    save(name, parts, w, d, h)


server('server', '#fbf8f1', '#efe9dc', '#d8d0bd', '#4fa37b')
server('server-hot', '#fbf8f1', '#efe9dc', '#d8d0bd', '#d0442f')
server('server-stage', '#f3f6fb', '#e3e9f3', '#c6d0e0', '#4f7bd0')

# mini executor for the 10x10 cluster
parts, P = cuboid(34, 34, 22, '#fbf8f1', '#ebe4d5', '#d3cab5', sw=1.6)
save('node', parts, 34, 34, 22, pad=6)
parts, P = cuboid(34, 34, 22, '#f6d9d2', '#e9b8ad', '#cf9284', sw=1.6)
save('node-hot', parts, 34, 34, 22, pad=6)

# partition tray: low, open, with an inner well
w, d, h = 130, 96, 22
parts, P = cuboid(w, d, h, '#d9d1bd', '#e7e0cf', '#cbc2ab')
parts.append(poly([P(10, 10, h), P(w - 10, 10, h), P(w - 10, d - 10, h), P(10, d - 10, h)], '#c4bba4', sw=1.6))
save('tray', parts, w, d, h)

# data cubes, one per key colour
for name, top, left, right in [('cube-in', '#3b62bd', '#2a4c9c', '#1d3878'), ('cube-us', '#fbf9f4', '#ece6d8', '#d4cbb6'),
                               ('cube-br', '#f1d9a8', '#e2c084', '#c8a05e'), ('cube-de', '#cfe3d6', '#a9cbb5', '#83ad93'),
                               ('cube-red', '#d9634e', '#b8432f', '#8d2d1e')]:
    parts, P = cuboid(26, 26, 22, top, left, right, sw=1.6)
    save(name, parts, 26, 26, 22, pad=5)

# cost block (taller red cube) for the overhead belt
parts, P = cuboid(44, 44, 48, '#d9634e', '#b8432f', '#8d2d1e', sw=2)
save('cost', parts, 44, 44, 48, pad=6)

# disk: stacked platters
r, plates = 54, 6
parts = []
for i in range(plates):
    z = i * 7
    cx, cy = iso(0, 0, z)
    parts.append(f'<ellipse cx="{cx:.2f}" cy="{cy + 0:.2f}" rx="{r * C * 1.41:.2f}" ry="{r * S * 1.41:.2f}" fill="{"#efebe1" if i == plates - 1 else "#d6cfbf"}" stroke="{INK}" stroke-width="2"/>')
cx, cy = iso(0, 0, (plates - 1) * 7)
parts.append(f'<ellipse cx="{cx:.2f}" cy="{cy:.2f}" rx="12" ry="7" fill="#1e1c18"/>')
save('disk', parts, r * 2.1, r * 2.1, plates * 7)

# network switch: flat box with port row and blinking lights
w, d, h = 110, 60, 26
parts, P = cuboid(w, d, h, '#2f2c27', '#3d3932', '#25221e')
for i in range(8):
    parts.append(poly([P(10 + i * 12, d, 8), P(18 + i * 12, d, 8), P(18 + i * 12, d, 16), P(10 + i * 12, d, 16)], '#d8c98f' if i % 3 else '#6fd19c', stroke='#111', sw=1))
save('switch', parts, w, d, h)

# conveyor belt
w, d, h = 300, 70, 14
parts, P = cuboid(w, d, h, '#cfc6b0', '#ddd5c2', '#bdb39b')
for i in range(1, 15):
    parts.append(line(P(i * 20, 4, h), P(i * 20, d - 4, h), '#a89e86', 2))
save('belt', parts, w, d, h)

# table slab (one row of a dataframe)
w, d, h = 64, 30, 6
parts, P = cuboid(w, d, h, '#fbf9f4', '#e9e3d5', '#d2c9b4', sw=1.4)
parts.append(line(P(10, 15, h), P(50, 15, h), '#c2b9a3', 3))
save('row', parts, w, d, h, pad=5)
parts, P = cuboid(w, d, h, '#dfe6f6', '#c7d2ec', '#a9b8de', sw=1.4)
save('row-head', parts, w, d, h, pad=5)

# lookup table: small stack of blue sheets
w, d, h = 70, 50, 18
parts, P = cuboid(w, d, h, '#e6ecf8', '#cdd7ef', '#aebde2', sw=1.6)
for z in (6, 12):
    parts.append(line(P(0, d, z), P(w, d, z), '#8fa2d0', 1.6))
save('lookup', parts, w, d, h)

(out / 'sprites.json').write_text(json.dumps(anchors, indent=1))
print(len(anchors), 'sprites ->', out)
