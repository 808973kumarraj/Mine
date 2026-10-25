"""Write one HTML file per scene of "How Delta Lake works", for Geneva's HTML renderer.

usage: python3 make_scenes.py           (reads voice/scenes.json + voice/narration.subtitles.json, writes scenes/*.html)

Geneva draws a subset of HTML/CSS without a browser: no JavaScript, SVG or skew. So isometric shapes are
divs cut with clip-path polygons, text on isometric faces uses rotate·scale·rotate (any 2D linear map
factors that way), and motion is CSS @keyframes on transform and opacity, delayed to each narration line.
"""
import json, math, pathlib
import numpy as np

HERE = pathlib.Path(__file__).parent
W, H = 1920, 1080
C, S = math.cos(math.radians(30)), math.sin(math.radians(30))
PAL = dict(paper='#ece6d6', ink='#2b2721', muted='#8a8270', rule='#cfc6b0', grid='#d9cdb3', blue='#2f55b0', red='#c0462f',
           green='#2f6b55', amber='#c8782f', card='#f8f5ee', dark='#2a2722')
FILE = ('#e4ebf8', '#c9d5ee', '#a9badf')                      # parquet file block: top, left, right
LOG = ('#f6e7c8', '#ecd3a3', '#d6b67c')                      # commit json
BOX = ('#fbf8f1', '#efe9dc', '#d8d0bd')                      # servers, platforms
REDF = ('#f2c4b8', '#e39a88', '#c9735f')
GHOST = ('#ece6d6', '#e4dccb', '#d9cfba')

scenes = json.load(open(HERE / 'voice/scenes.json'))['scenes']
cues = json.load(open(HERE / 'voice/narration.subtitles.json'))


def line_times(sc):
    """Start of each narration line, relative to the scene."""
    return [c['start'] - sc['start'] for c in cues if sc['start'] <= c['start'] < sc['start'] + sc['duration']]


# ---------------- isometric drawing ----------------
class Scene:
    def __init__(self, name, ox=960, oy=640):
        self.name, self.ox, self.oy, self.els, self.kf, self.n = name, ox, oy, [], {}, 0

    def p(self, x, y, z=0):                                   # world -> screen
        return (self.ox + (x - y) * C, self.oy + (x + y) * S - z)

    def uid(self, base):
        self.n += 1; return f'{base}{self.n}'

    def anim(self, css_anim):
        return f'animation:{css_anim};' if css_anim else ''

    def box(self, x, y, w, d, h, col=BOX, label=None, anim=None, z=0, label_size=18, top_label=None, top_size=16, dashed=False):
        """An isometric block with its floor centre at (x, y), sides w (x) and d (y), height h, optional front label."""
        pts = lambda *q: [self.p(x + a, y + b, z + c) for a, b, c in q]
        hw, hd = w / 2, d / 2
        top = pts((-hw, -hd, h), (hw, -hd, h), (hw, hd, h), (-hw, hd, h))
        left = pts((-hw, hd, 0), (hw, hd, 0), (hw, hd, h), (-hw, hd, h))
        right = pts((hw, -hd, 0), (hw, hd, 0), (hw, hd, h), (hw, -hd, h))
        allp = top + left + right
        x0, y0 = min(a for a, _ in allp) - 6, min(b for _, b in allp) - 6
        x1, y1 = max(a for a, _ in allp) + 6, max(b for _, b in allp) + 6
        bw, bh = x1 - x0, y1 - y0
        poly = lambda ps: 'polygon(' + ','.join(f'{a - x0:.1f}px {b - y0:.1f}px' for a, b in ps) + ')'

        def inset(ps, k):                                     # shrink a face toward its centre (the ink edge shows around it)
            cx, cy = sum(a for a, _ in ps) / len(ps), sum(b for _, b in ps) / len(ps)
            return [(a + (cx - a) * k / max(1, math.dist((a, b), (cx, cy))), b + (cy - b) * k / max(1, math.dist((a, b), (cx, cy)))) for a, b in ps]
        faces = ''
        ink = PAL['ink'] if not dashed else '#9a8f78'
        for ps, colr in ((left, col[1]), (right, col[2]), (top, col[0])):
            faces += f'<div style="position:absolute;inset:0;background:{ink};clip-path:{poly(ps)}"></div>'
            faces += f'<div style="position:absolute;inset:0;background:{colr};clip-path:{poly(inset(ps, 3.2))}"></div>'
        shadow = (f'<div style="position:absolute;left:{self.p(x, y)[0] - x0 - w * 0.75:.0f}px;top:{self.p(x, y)[1] - y0 - d * 0.18:.0f}px;'
                  f'width:{w * 1.5:.0f}px;height:{d * 0.62:.0f}px;border-radius:50%;background:radial-gradient(closest-side,rgba(60,48,30,.26),rgba(60,48,30,0))"></div>') if z == 0 else ''
        text = ''
        if label:
            text += self.plane_text(label, x - hw + 10, y + hd, z + h - 12 - label_size * 0.2, 'left', label_size, x0, y0, PAL['ink'])
        if top_label:
            text += self.plane_text(top_label, x - hw + 8, y - hd / 2 + 4, z + h, 'top', top_size, x0, y0, PAL['ink'])
        op = 'opacity:.55;' if dashed else ''
        self.els.append((self.depth(x, y, z), f'<div class="o" style="left:{x0:.0f}px;top:{y0:.0f}px;width:{bw:.0f}px;height:{bh:.0f}px;{op}{self.anim(anim)}">{shadow}{faces}{text}</div>'))

    def depth(self, x, y, z): return x + y + z * 0.01

    def plane_text(self, s, x, y, z, plane, size, x0, y0, color, weight=800):
        """Text lying in an isometric plane, as rotate·scale·rotate (Geneva draws no skew)."""
        M = {'top': np.array([[C, -C], [S, S]]), 'left': np.array([[C, 0], [S, 1]]), 'right': np.array([[C, 0], [-S, 1]])}[plane]
        if plane == 'top': M = np.array([[C, -C], [S, S]]) @ np.diag([1, 1])
        U, sv, Vt = np.linalg.svd(M)
        if np.linalg.det(U) < 0: U[:, 1] *= -1; sv = sv * np.array([1, -1])
        if np.linalg.det(Vt) < 0: Vt[1, :] *= -1; sv = sv * np.array([1, -1])
        a1, a2 = math.degrees(math.atan2(U[1, 0], U[0, 0])), math.degrees(math.atan2(Vt[1, 0], Vt[0, 0]))
        sx, sy = self.p(x, y, z)
        return (f'<div style="position:absolute;left:{sx - x0:.1f}px;top:{sy - y0:.1f}px;transform-origin:0 0;'
                f'transform:rotate({a1:.3f}deg) scale({sv[0]:.4f},{sv[1]:.4f}) rotate({a2:.3f}deg);white-space:nowrap;'
                f'font:{weight} {size}px/1 JetBrains Mono;color:{color};letter-spacing:.04em">{s}</div>')

    def floor_text(self, s, x, y, size=20, color=PAL['ink'], anim=None, weight=800):
        sx, sy = self.p(x, y)
        t = self.plane_text(s, x, y, 0, 'top', size, sx, sy, color, weight)
        self.els.append((-1e6 + self.depth(x, y, 0), f'<div class="o" style="left:{sx:.0f}px;top:{sy:.0f}px;width:1px;height:1px;{self.anim(anim)}">{t}</div>'))

    def flat(self, html, css, anim=None, z=9e6):              # 2D overlay (titles, cards)
        self.els.append((z, f'<div class="o" style="{css};{self.anim(anim)}">{html}</div>'))

    def line(self, a, b, color=PAL['blue'], width=4, dashed=False, anim=None):
        """A straight screen-space line between two world points (a rotated thin div)."""
        (x1, y1), (x2, y2) = self.p(*a), self.p(*b)
        L, ang = math.hypot(x2 - x1, y2 - y1), math.degrees(math.atan2(y2 - y1, x2 - x1))
        bg = f'repeating-linear-gradient(90deg,{color} 0 14px,transparent 14px 24px)' if dashed else color
        self.els.append((8e6, f'<div class="o" style="left:{x1:.0f}px;top:{y1 - width / 2:.0f}px;width:{L:.0f}px;height:{width}px;'
                              f'background:{bg};border-radius:{width}px;transform-origin:0 50%;transform:rotate({ang:.2f}deg);{self.anim(anim)}"></div>'))

    def keyframes(self, name, body): self.kf[name] = body

    def html(self, title=None, kicker=None, title_t=0.0):
        grid = ''
        for i in range(-30, 31):                               # isometric floor grid around the origin
            for (a, b) in (((i * 60, -1800), (i * 60, 1800)), ((-1800, i * 60), (1800, i * 60))):
                grid += self._grid_line(a, b)
        head = (f'<div class="crumb">&gt; BINARY HABIT / DATA ENG / DELTA LAKE<b></b></div>')
        if kicker: head += f'<div class="kicker" style="animation:up .5s {title_t:.2f}s both">{kicker}</div>'
        if title: head += f'<div class="title" style="animation:up .6s {title_t + .1:.2f}s both">{title}</div>'
        body = ''.join(h for _, h in sorted(self.els, key=lambda e: e[0]))
        kfs = ''.join(f'@keyframes {k}{{{v}}}' for k, v in self.kf.items())
        return f'''<!doctype html><html><head><meta charset="utf-8"><style>
@font-face{{font-family:JetBrains Mono;src:url(../fonts/JetBrainsMono-Regular.ttf);font-weight:400}}
@font-face{{font-family:JetBrains Mono;src:url(../fonts/JetBrainsMono-Bold.ttf);font-weight:700}}
@font-face{{font-family:JetBrains Mono;src:url(../fonts/JetBrainsMono-ExtraBold.ttf);font-weight:800}}
body{{margin:0;width:{W}px;height:{H}px;overflow:hidden;background:{PAL['paper']};font-family:JetBrains Mono;color:{PAL['ink']}}}
.o{{position:absolute}}
.crumb{{position:absolute;left:90px;right:90px;top:62px;font:700 26px JetBrains Mono;letter-spacing:.28em;color:#4a453b;padding-bottom:18px;border-bottom:3px solid {PAL['rule']}}}
.crumb b{{display:inline-block;width:14px;height:26px;background:{PAL['amber']};margin-left:10px;position:relative;top:4px}}
.kicker{{position:absolute;left:94px;top:140px;font:700 24px JetBrains Mono;letter-spacing:.2em;color:{PAL['muted']}}}
.title{{position:absolute;left:92px;top:176px;font:800 64px JetBrains Mono;color:{PAL['ink']}}}
.card{{background:{PAL['card']};border:3px solid {PAL['ink']};border-radius:12px;box-shadow:10px 12px 0 rgba(43,39,33,.13);padding:22px 28px}}
.chip{{display:inline-block;border:3px solid currentColor;border-radius:999px;padding:8px 22px;font:800 26px JetBrains Mono;background:{PAL['card']}}}
.mono{{font:700 26px/1.55 JetBrains Mono}}
@keyframes up{{from{{opacity:0;transform:translateY(30px)}}to{{opacity:1;transform:translateY(0)}}}}
@keyframes drop{{0%{{opacity:0;transform:translateY(-160px)}}35%{{opacity:1}}70%{{transform:translateY(8px)}}85%{{transform:translateY(-4px)}}100%{{opacity:1;transform:translateY(0)}}}}
@keyframes pop{{0%{{opacity:0;transform:scale(.6)}}70%{{opacity:1;transform:scale(1.06)}}100%{{opacity:1;transform:scale(1)}}}}
@keyframes fadein{{from{{opacity:0}}to{{opacity:1}}}}
@keyframes fadeout{{from{{opacity:1}}to{{opacity:.18}}}}
@keyframes gone{{from{{opacity:1}}to{{opacity:0}}}}
@keyframes solid{{from{{opacity:.45}}to{{opacity:1}}}}
@keyframes grow{{from{{transform:scaleX(0)}}to{{transform:scaleX(1)}}}}
@keyframes pulse{{0%{{transform:scale(1)}}50%{{transform:scale(1.08)}}100%{{transform:scale(1)}}}}
{kfs}
</style></head><body>{grid}{head}{body}</body></html>'''

    def _grid_line(self, a, b):
        (x1, y1), (x2, y2) = self.p(*a), self.p(*b)
        L, ang = math.hypot(x2 - x1, y2 - y1), math.degrees(math.atan2(y2 - y1, x2 - x1))
        return (f'<div style="position:absolute;left:{x1:.0f}px;top:{y1:.0f}px;width:{L:.0f}px;height:1.6px;background:{PAL["grid"]};'
                f'transform-origin:0 50%;transform:rotate({ang:.2f}deg)"></div>')


A = lambda name, dur, delay, ease='ease-out': f'{name} {dur}s {ease} {delay:.2f}s both'
out = HERE / 'scenes'; out.mkdir(exist_ok=True)
FILES6 = [(-210, -90), (-90, -90), (30, -90), (-210, 30), (-90, 30), (30, 30)]


def files(sc, t, positions=FILES6, names=None, cols=None, step=.12, anim='drop'):
    for i, (x, y) in enumerate(positions):
        sc.box(x, y, 90, 90, 60, (cols or {}).get(i, FILE), label=(names[i] if names else f'part-{i + 1}'), label_size=15,
               anim=A(anim, .7, t + i * step) if t is not None else None)


def log_stack(sc, x, y, n, t=None, step=.25, first=0, highlight=None, hcol=None):
    for i in range(n):
        col = hcol if highlight is not None and i == highlight else LOG
        sc.box(x, y, 120, 80, 16, col, z=i * 18, top_label=f'{first + i:03d}.json', top_size=14,
               anim=A('drop', .6, t + i * step) if t is not None else None)


for sc_meta in scenes:
    nm, dur = sc_meta['name'], sc_meta['duration']
    L = line_times(sc_meta) + [dur] * 4
    sc = Scene(nm)

    if nm == 'title':
        sc.oy = 700
        for i, (x, y) in enumerate([(-120, -40), (0, -40), (-120, 80), (0, 80)]):
            sc.box(x, y, 100, 100, 70, FILE, label='.parquet', label_size=15, anim=A('drop', .7, .2 + i * .12))
        log_stack(sc, 230, -20, 4, t=.8, step=.15)
        sc.flat('How Delta Lake works', 'left:0;right:0;top:250px;text-align:center;font:800 92px JetBrains Mono', A('up', .7, .1))
        sc.flat('PARQUET FILES  +  A TRANSACTION LOG', f'left:0;right:0;top:370px;text-align:center;font:700 26px JetBrains Mono;letter-spacing:.24em;color:{PAL["muted"]}', A('fadein', .6, .6))
        html = sc.html()

    elif nm == 'problem':
        sc.box(-90, -30, 520, 380, 24, BOX, label='CLOUD STORAGE', label_size=20, anim=A('fadein', .5, L[0]))
        files(sc, L[0] + .3, [(x, y) for x, y in FILES6])
        for i, (x, y) in enumerate([(150, -90), (150, 30)]):
            sc.box(x, y, 90, 90, 60 if i == 0 else 34, REDF if i else FILE, label=('part-7' if i == 0 else 'part-8 ✗'), label_size=15, anim=A('drop', .7, L[1] + .6 + i * .3), z=24)
        sc.box(-90, -30, 90, 90, 60, BOX, label='', anim=None) if False else None
        sc.box(400, -260, 150, 120, 120, BOX, label='WRITE JOB', label_size=17, anim=A('drop', .7, L[1]))
        sc.flat('<span class="chip" style="color:#c0462f">✗ JOB FAILED HALFWAY</span>', 'left:1240px;top:330px', A('pop', .5, L[1] + 1.4))
        sc.box(-470, 230, 150, 120, 120, BOX, label='READER', label_size=17, anim=A('drop', .7, L[1] + 3.2))
        sc.flat('<span class="chip" style="color:#c0462f">⚠ sees broken data</span>', 'left:300px;top:760px', A('pop', .5, L[1] + 4.2))
        html = sc.html('Plain data lake', 'THE PROBLEM')

    elif nm == 'log':
        sc.box(-130, -30, 400, 300, 24, BOX, label='orders/', label_size=20)
        files(sc, None, [(x - 40, y) for x, y in FILES6])
        sc.box(300, 120, 260, 220, 24, LOG, label='_delta_log/', label_size=20, anim=A('pop', .6, L[0] + .5))
        log_stack(sc, 300, 120, 3, t=L[1] + .8, step=.35)
        sc.flat('<div class="card mono" style="font-size:24px">000.json<br>001.json<br>002.json</div>', 'left:1360px;top:300px', A('up', .6, L[1] + 2.4))
        sc.flat('<span class="chip" style="color:#2f55b0">THE LOG = SOURCE OF TRUTH</span>', 'left:1180px;top:740px', A('pop', .5, L[2] + .3))
        html = sc.html('Delta Lake = files + a log', 'THE IDEA')

    elif nm == 'write':
        sc.box(-130, -30, 400, 300, 24, BOX, label='orders/', label_size=20)
        files(sc, None, [(x - 40, y) for x, y in FILES6[:4]])
        for i, (x, y) in enumerate([(-130 + 80, 30 + 0), (-10 + 40, 30)]):
            pass
        newpos = [(30 - 40 + 120, -90), (30 - 40 + 120, 30)]
        for i, (x, y) in enumerate(newpos):
            sc.box(x, y, 90, 90, 60, FILE, label=f'part-{5 + i}', label_size=15, anim=f"{A('drop', .7, L[0] + .5 + i * .25)},{A('solid', .4, L[1] + 1.6)}", z=24, dashed=False)
        sc.box(300, 120, 260, 220, 24, LOG, label='_delta_log/', label_size=20)
        log_stack(sc, 300, 120, 3)
        sc.box(300, 120, 120, 80, 16, ('#d9ead9', '#b9d6bd', '#93bb9b'), z=3 * 18, top_label='003.json', top_size=14, anim=A('drop', .7, L[1] + .8))
        sc.flat('<span class="chip" style="color:#2f6b55">✓ COMMIT 003 — ATOMIC</span>', 'left:1240px;top:730px', A('pop', .5, L[1] + 1.6))
        sc.flat(f'<div class="card mono" style="font-size:22px">{{"add": "part-5"}}<br>{{"add": "part-6"}}</div>', 'left:1320px;top:300px', A('up', .5, L[1] + 1.2))
        for i in range(2):
            sc.box(-330 + i * 100, 250, 90, 90, 60, GHOST, label=f'tmp-{i + 1}', label_size=15, anim=f"{A('drop', .6, L[2] + .3 + i * .2)},{A('fadeout', .5, L[2] + 2.2)}", dashed=True)
        sc.flat('<span class="chip" style="color:#c0462f">✗ no commit → ignored</span>', 'left:180px;top:810px', A('pop', .5, L[2] + 1.8))
        html = sc.html('Writing: files first, then one commit', 'WRITE')

    elif nm == 'read':
        sc.box(-130, -30, 400, 300, 24, BOX, label='orders/', label_size=20)
        stale = {1, 3}
        for i, (x, y) in enumerate([(x - 40, y) for x, y in FILES6]):
            a = A('fadeout', .6, L[1] + .3) if i in stale else A('pulse', .6, L[1] + .3 + i * .1)
            sc.box(x, y, 90, 90, 60, FILE, label=f'part-{i + 1}', label_size=15, anim=a, z=24)
        sc.box(300, 120, 260, 220, 24, LOG, label='_delta_log/', label_size=20)
        for i in range(4):
            sc.box(300, 120, 120, 80, 16, LOG, z=i * 18, top_label=f'{i:03d}.json', top_size=14, anim=A('pulse', .4, L[0] + 1.2 + i * .4))
        sc.box(560, -260, 150, 120, 120, BOX, label='SPARK', label_size=17, anim=A('drop', .7, L[0]))
        sc.line((560, -200, 60), (330, 100, 80), PAL['blue'], 5, True, A('fadein', .4, L[0] + .8))
        sc.flat('<div class="card mono" style="font-size:22px">current files:<br>part-1, part-3,<br>part-5, part-6</div>', 'left:1350px;top:290px', A('up', .5, L[0] + 2.8))
        html = sc.html('Reading: the log decides', 'READ')

    elif nm == 'update':
        sc.box(-130, -30, 400, 300, 24, BOX, label='orders/', label_size=20)
        for i, (x, y) in enumerate([(x - 40, y) for x, y in FILES6]):
            a = A('fadeout', .7, L[1] + 2.0) if i == 1 else None
            sc.box(x, y, 90, 90, 60, FILE, label=f'part-{i + 1}', label_size=15, anim=a, z=24)
        sc.box(80 + 120, 150, 90, 90, 60, ('#d9ead9', '#b9d6bd', '#93bb9b'), label='part-7', label_size=15, z=24, anim=A('drop', .7, L[1] + .6))
        sc.flat('<span class="chip" style="color:#c0462f">✗ never edited in place</span>', 'left:150px;top:300px', A('pop', .5, L[0] + .4))
        sc.flat('<div class="card mono" style="font-size:22px">004.json<br><span style="color:#c0462f">{"remove": "part-2"}</span><br><span style="color:#2f6b55">{"add": "part-7"}</span></div>', 'left:1300px;top:300px', A('up', .5, L[1] + 2.6))
        html = sc.html('Updates and deletes', 'COPY ON WRITE')

    elif nm == 'concurrent':
        sc.box(0, 160, 260, 220, 24, LOG, label='_delta_log/', label_size=20)
        log_stack(sc, 0, 160, 3)
        sc.box(-430, 60, 150, 120, 120, BOX, label='WRITER A', label_size=17, anim=A('drop', .7, L[0]))
        sc.box(380, -330, 150, 120, 120, BOX, label='WRITER B', label_size=17, anim=A('drop', .7, L[0] + .3))
        sc.keyframes('flyA', 'from{transform:translate(-430px,-120px);opacity:0}20%{opacity:1}to{transform:translate(0,0);opacity:1}')
        sc.keyframes('flyB', 'from{transform:translate(380px,-300px);opacity:0}20%{opacity:1}60%{transform:translate(40px,-30px)}75%{transform:translate(120px,-120px)}to{transform:translate(120px,-120px);opacity:1}')
        sc.box(0, 160, 120, 80, 16, ('#d9ead9', '#b9d6bd', '#93bb9b'), z=3 * 18, top_label='003.json', top_size=14, anim=A('flyA', 1.2, L[1] + .4, 'ease-in-out'))
        sc.box(0, 160, 120, 80, 16, REDF, z=3 * 18 + 40, top_label='003.json', top_size=14, anim=f"{A('flyB', 1.4, L[1] + .5, 'ease-in-out')},{A('gone', .3, L[2] + .2)}")
        sc.box(0, 160, 120, 80, 16, ('#d9ead9', '#b9d6bd', '#93bb9b'), z=4 * 18, top_label='004.json', top_size=14, anim=A('drop', .7, L[2] + 2.0))
        sc.flat('<span class="chip" style="color:#2f6b55">✓ A wins 003</span>', 'left:300px;top:760px', A('pop', .5, L[1] + 1.8))
        sc.flat('<span class="chip" style="color:#c0462f">✗ B conflicts</span>', 'left:1300px;top:420px', A('pop', .5, L[1] + 2.1))
        sc.flat('<span class="chip" style="color:#2f55b0">↻ B retries → 004</span>', 'left:1240px;top:760px', A('pop', .5, L[2] + 1.6))
        html = sc.html('Two writers at once', 'OPTIMISTIC CONCURRENCY')

    elif nm == 'timetravel':
        sc.box(-130, -30, 400, 300, 24, BOX, label='orders/', label_size=20)
        later = {4, 5}
        for i, (x, y) in enumerate([(x - 40, y) for x, y in FILES6]):
            sc.box(x, y, 90, 90, 60, FILE, label=f'part-{i + 1}', label_size=15, z=24, anim=A('fadeout', .7, L[1] + 1.4) if i in later else None)
        sc.box(300, 120, 260, 220, 24, LOG, label='_delta_log/', label_size=20)
        for i in range(5):
            sc.box(300, 120, 120, 80, 16, ('#d9ead9', '#b9d6bd', '#93bb9b') if i == 1 else LOG, z=i * 18, top_label=f'{i:03d}.json', top_size=14,
                   anim=A('fadeout', .6, L[1] + 1.2) if i > 1 else None)
        sc.flat('<div class="card mono" style="font-size:24px;background:#2a2722;color:#e8e2d0;border-color:#2a2722">SELECT * FROM orders<br><span style="color:#b9c6ff">VERSION AS OF 1</span></div>', 'left:1240px;top:300px', A('up', .6, L[1] + .3))
        sc.flat('<span class="chip" style="color:#2f55b0">⟲ the table at version 1</span>', 'left:1240px;top:740px', A('pop', .5, L[1] + 2.0))
        html = sc.html('Time travel', 'HISTORY')

    elif nm == 'checkpoint':
        sc.box(0, 120, 260, 220, 24, LOG, label='_delta_log/', label_size=20)
        for i in range(10):
            sc.box(-40 + (i // 5) * 0, 120, 120, 80, 14, LOG, z=i * 15, top_label=f'{i:03d}.json' if i in (0, 9) else '', top_size=13, anim=A('drop', .4, .2 + i * .12))
        sc.box(140, 70, 140, 110, 70, ('#dfe6f6', '#c7d2ec', '#a9b8de'), z=24, label='010.checkpoint', label_size=13, anim=A('drop', .8, L[0] + 2.2))
        sc.box(-420, 60, 150, 120, 120, BOX, label='READER', label_size=17, anim=A('drop', .7, L[1]))
        sc.line((-420, 60, 70), (140, 70, 100), PAL['blue'], 5, True, A('fadein', .4, L[1] + .8))
        sc.flat('<span class="chip" style="color:#2f55b0">start from the checkpoint</span>', 'left:1200px;top:740px', A('pop', .5, L[1] + 1.2))
        html = sc.html('Checkpoints keep reads fast', 'EVERY 10 COMMITS')

    elif nm == 'outro':
        sc.oy = 590
        for i, (x, y) in enumerate([(-120, -40), (0, -40), (-120, 80), (0, 80)]):
            sc.box(x, y, 100, 100, 70, FILE, label='.parquet', label_size=15)
        log_stack(sc, 230, -20, 4)
        sc.flat('Files + Log = Delta Lake', 'left:0;right:0;top:260px;text-align:center;font:800 84px JetBrains Mono', A('up', .7, .2))
        sc.flat('BinaryHabit<span style="color:#c8782f">.</span>', 'left:0;right:0;top:800px;text-align:center;font:800 52px JetBrains Mono', A('fadein', .6, 1.2))
        html = sc.html()

    (out / f'{nm}.html').write_text(html)
    print(nm, round(dur, 2), 's', len(html) // 1024, 'KB')
