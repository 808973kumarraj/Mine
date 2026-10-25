"""Blender 4.2 script: the Spark shuffle explainer as a 2.5D isometric animation.

    blender -b --factory-startup -P shuffle.py -- OUT_DIR [--frames A-B] [--still T1,T2,...]

Builds the whole 80.4 s scene (real 3D boxes, an orthographic isometric camera, 3D text on the
floor and on box faces, keyframed motion), then renders PNG frames to OUT_DIR. Flat illustration
shading (fixed light/mid/dark per face) plus Freestyle outlines, rendered with Cycles at 1 sample.
--still renders single frames at the given seconds instead (for checking).
"""
import bpy, math, os, sys

argv = sys.argv[sys.argv.index('--') + 1:] if '--' in sys.argv else []
OUT = os.path.abspath(argv[0]) if argv else '/tmp/shuffle-frames'
HERE = os.path.dirname(os.path.abspath(__file__))
FPS, DUR = 24, 80.4
W, H = 1080, 1100
S = 30.0                       # orthographic width in Blender units
U = S / 1080                   # one design pixel (1080-wide layout) in camera units

bpy.ops.wm.read_factory_settings(use_empty=True)
sc = bpy.context.scene
sc.render.fps = FPS
sc.frame_start, sc.frame_end = 1, round(DUR * FPS)
F = lambda t: round(t * FPS) + 1

# ---------------- palette (sRGB hex -> linear) ----------------
def rgb(h, a=1.0):
    h = h.lstrip('#'); c = [int(h[i:i + 2], 16) / 255 for i in (0, 2, 4)]
    lin = [x / 12.92 if x <= 0.04045 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
    return (*lin, a)
PAPER, INK, MUTED = rgb('#ece6d6'), rgb('#2b2721'), rgb('#8a8270')
BOX, STAGE, TRAY, BELT = rgb('#faf6ee'), rgb('#eef2fa'), rgb('#d6cdb8'), rgb('#cfc6b0')
BLUE, RED, GREEN, AMBER, DARK = rgb('#2f55b0'), rgb('#c0462f'), rgb('#2f6b55'), rgb('#c8782f'), rgb('#2a2722')
KEYC = {'in': rgb('#3560c0'), 'us': rgb('#fbf8f2'), 'br': rgb('#ecc888'), 'de': rgb('#b4d6c0')}
NODE, HOT = rgb('#faf6ee'), rgb('#efb3a6')

# ---------------- render settings ----------------
sc.render.engine = 'CYCLES'
sc.cycles.samples = 1
sc.cycles.use_denoising = False
sc.cycles.max_bounces = 0
sc.cycles.device = 'CPU'
sc.render.film_transparent = False
sc.render.resolution_x, sc.render.resolution_y = W, H
sc.render.image_settings.file_format = 'PNG'
sc.view_settings.view_transform = 'Standard'
world = bpy.data.worlds.new('paper'); sc.world = world
world.use_nodes = True
world.node_tree.nodes['Background'].inputs[0].default_value = PAPER
world.node_tree.nodes['Background'].inputs[1].default_value = 1.0

# one flat material for every solid: colour from the object's colour, shade from the face direction
def flat_material():
    m = bpy.data.materials.new('Flat'); m.use_nodes = True; nt = m.node_tree; n = nt.nodes; L = nt.links
    n.clear()
    out = n.new('ShaderNodeOutputMaterial'); em = n.new('ShaderNodeEmission'); tr = n.new('ShaderNodeBsdfTransparent'); mix = n.new('ShaderNodeMixShader')
    oi = n.new('ShaderNodeObjectInfo'); geo = n.new('ShaderNodeNewGeometry'); sep = n.new('ShaderNodeSeparateXYZ')
    L.new(geo.outputs['Normal'], sep.inputs[0])
    def mathop(op, a, b=None, v=None):
        k = n.new('ShaderNodeMath'); k.operation = op
        L.new(a, k.inputs[0]) if not isinstance(a, float) else None
        if isinstance(a, float): k.inputs[0].default_value = a
        if b is not None: L.new(b, k.inputs[1])
        if v is not None: k.inputs[1].default_value = v
        return k.outputs[0]
    top = mathop('MAXIMUM', sep.outputs['Z'], v=0.0)
    left = mathop('MAXIMUM', mathop('MULTIPLY', sep.outputs['Y'], v=-1.0), v=0.0)
    right = mathop('MAXIMUM', sep.outputs['X'], v=0.0)
    shade = mathop('ADD', mathop('ADD', top, mathop('MULTIPLY', left, v=0.86)), mathop('MULTIPLY', right, v=0.70))
    mul = n.new('ShaderNodeMix'); mul.data_type = 'RGBA'; mul.blend_type = 'MULTIPLY'; mul.inputs['Factor'].default_value = 1.0
    L.new(oi.outputs['Color'], mul.inputs[6])
    cmb = n.new('ShaderNodeCombineColor'); L.new(shade, cmb.inputs[0]); L.new(shade, cmb.inputs[1]); L.new(shade, cmb.inputs[2])
    L.new(cmb.outputs[0], mul.inputs[7])
    L.new(mul.outputs[2], em.inputs['Color'])
    L.new(oi.outputs['Alpha'], mix.inputs['Fac'])
    L.new(tr.outputs[0], mix.inputs[1]); L.new(em.outputs[0], mix.inputs[2]); L.new(mix.outputs[0], out.inputs['Surface'])
    m.blend_method = 'BLEND'
    return m
FLAT = flat_material()

# text material: emission, colour from object colour, ignores face direction
def text_material():
    m = bpy.data.materials.new('Text'); m.use_nodes = True; nt = m.node_tree; n = nt.nodes; L = nt.links
    n.clear()
    out = n.new('ShaderNodeOutputMaterial'); em = n.new('ShaderNodeEmission'); tr = n.new('ShaderNodeBsdfTransparent'); mix = n.new('ShaderNodeMixShader'); oi = n.new('ShaderNodeObjectInfo')
    L.new(oi.outputs['Color'], em.inputs['Color']); L.new(oi.outputs['Alpha'], mix.inputs['Fac'])
    L.new(tr.outputs[0], mix.inputs[1]); L.new(em.outputs[0], mix.inputs[2]); L.new(mix.outputs[0], out.inputs['Surface'])
    return m
TXT = text_material()

FONTS = {w: bpy.data.fonts.load(os.path.join(HERE, 'fonts', f'JetBrainsMono-{w}.ttf')) for w in ('Regular', 'Bold', 'ExtraBold')}

# collections: outlined solids get Freestyle lines, everything else does not
def coll(name):
    c = bpy.data.collections.new(name); sc.collection.children.link(c); return c
C_LINE, C_FLAT = coll('outlined'), coll('plain')

sc.render.use_freestyle = True
sc.render.line_thickness_mode = 'ABSOLUTE'
sc.render.line_thickness = 1.25
vl = sc.view_layers[0]
fs = vl.freestyle_settings
fs.crease_angle = math.radians(120)
ls = fs.linesets[0] if fs.linesets else fs.linesets.new('lines')
ls.select_by_collection = True; ls.collection = C_LINE
ls.select_by_visibility = True
ls.select_silhouette = ls.select_border = ls.select_crease = True
if ls.linestyle is None: ls.linestyle = bpy.data.linestyles.new('ink')
ls.linestyle.color = INK[:3]
ls.linestyle.thickness = 1.25

# ---------------- keyframe helpers ----------------
def ease(obj, path, interp='BEZIER', easing='AUTO'):
    ad = obj.animation_data
    if not ad or not ad.action: return
    for fc in ad.action.fcurves:
        if fc.data_path == path:
            for k in fc.keyframe_points: k.interpolation = interp; k.easing = easing

def key(obj, path, t, value, index=-1, interp=None, easing='EASE_OUT'):
    if index >= 0:
        getattr(obj, path)[index] = value
    else:
        setattr(obj, path, value)
    obj.keyframe_insert(path, index=index, frame=F(t))
    if interp:
        for fc in obj.animation_data.action.fcurves:
            if fc.data_path == path and (index < 0 or fc.array_index == index):
                for k in fc.keyframe_points:
                    if abs(k.co[0] - F(t)) < .5: k.interpolation = interp; k.easing = easing

def visible(obj, t0, t1):
    """Show obj (and its children) from t0 to t1 in the render, narrowed by their own appear/vanish times."""
    for o in [obj] + list(obj.children_recursive):
        a, b = max(t0, APPEAR.get(o.name, t0)), min(t1, VANISH.get(o.name, t1))
        o.hide_render = True; o.keyframe_insert('hide_render', frame=0)
        o.hide_render = False; o.keyframe_insert('hide_render', frame=F(a))
        o.hide_render = True; o.keyframe_insert('hide_render', frame=F(b))
        for fc in o.animation_data.action.fcurves:
            if fc.data_path == 'hide_render':
                for k in fc.keyframe_points: k.interpolation = 'CONSTANT'
        o.hide_render = not (a <= 0)

APPEAR, VANISH = {}, {}      # object name -> time it first shows / last shows (outlines would linger otherwise)
def alpha(obj, t, a0, a1, d=0.3):
    for o in [obj] + list(obj.children_recursive):
        key(o, 'color', t, a0, index=3); key(o, 'color', t + d, a1, index=3)
        if a0 == 0: APPEAR[o.name] = max(APPEAR.get(o.name, 0), t)
        if a1 == 0: VANISH[o.name] = t + d

def tint(obj, t, col, d=0.25):
    c0 = list(obj.color)
    for i in range(3): key(obj, 'color', t, c0[i], index=i); key(obj, 'color', t + d, col[i], index=i)

def drop(obj, t, h=3.5, d=0.8):
    z = obj.location.z
    key(obj, 'location', t, z + h, index=2); key(obj, 'location', t + d, z, index=2, interp='BOUNCE', easing='EASE_OUT')
    obj.location.z = z
    alpha(obj, t, 0.0, 1.0, 0.15)

def hop(obj, t, to, peak=6, d=0.9):
    x0, y0, z0 = obj.location
    key(obj, 'location', t, x0, 0); key(obj, 'location', t, y0, 1); key(obj, 'location', t, z0, 2)
    key(obj, 'location', t + d, to[0], 0, interp='BEZIER', easing='AUTO'); key(obj, 'location', t + d, to[1], 1, interp='BEZIER', easing='AUTO')
    key(obj, 'location', t + d / 2, max(z0, to[2]) + peak, 2); key(obj, 'location', t + d, to[2], 2)
    obj.location = to

# ---------------- object builders ----------------
def empty(name, loc=(0, 0, 0), parent=None):
    e = bpy.data.objects.new(name, None); C_FLAT.objects.link(e); e.location = loc
    if parent: e.parent = parent
    return e

def solid(name, mesh_fn, loc, scale, color, parent, outlined=True):
    mesh_fn()
    o = bpy.context.active_object; o.name = name
    for c in o.users_collection: c.objects.unlink(o)
    (C_LINE if outlined else C_FLAT).objects.link(o)
    o.location = loc; o.scale = scale; o.color = color
    o.data.materials.append(FLAT); o.parent = parent
    return o

cube_mesh = lambda: bpy.ops.mesh.primitive_cube_add(size=1)
def text(body, loc, rot=(0, 0, 0), size=0.5, color=INK, weight='Bold', parent=None, align='LEFT', name='txt'):
    cu = bpy.data.curves.new(name, 'FONT'); cu.body = body; cu.font = FONTS[weight]; cu.size = size; cu.align_x = align
    cu.space_character = 1.06
    o = bpy.data.objects.new(name, cu); C_FLAT.objects.link(o)
    o.location = loc; o.rotation_euler = [math.radians(a) for a in rot]; o.color = color; o.parent = parent
    cu.materials.append(TXT)
    return o

def box(name, x, y, w, d, h, color=BOX, label=None, parent=None, led=True, vents=True, label_size=0.34):
    """A server-style box: root empty on the floor at (x, y), the solid, a printed label and details on its front face."""
    r = empty(name, (x, y, 0), parent)
    solid(name + '_body', cube_mesh, (0, 0, h / 2), (w, d, h), color, r)
    fy = -d / 2 - 0.012                                   # front face (faces the camera's left)
    if label: text(label, (-w / 2 + 0.22, fy, h - 0.55), (90, 0, 0), label_size, INK, 'ExtraBold', r)
    if vents:
        for i in range(4): solid(f'{name}_v{i}', cube_mesh, (-w / 2 + 1.0, fy, h * 0.28 + i * 0.36), (1.4, 0.01, 0.07), rgb('#a99f88'), r, outlined=False)
        solid(name + '_bay', cube_mesh, (w / 2 - 0.85, fy, 0.55), (0.8, 0.012, 0.32), DARK, r, outlined=False)
    if led: solid(name + '_led', lambda: bpy.ops.mesh.primitive_cylinder_add(vertices=16, radius=0.12, depth=0.02, rotation=(math.pi / 2, 0, 0)), (w / 2 - 0.45, fy, h - 0.42), (1, 1, 1), rgb('#4fa37b'), r, outlined=False)
    return r

def cube(name, x, y, z, key_name, parent, size=0.82):
    r = empty(name, (x, y, z), parent)
    solid(name + '_c', cube_mesh, (0, 0, size / 2), (size, size, size), KEYC[key_name], r)
    return r

def tray(name, x, y, label, parent):
    r = empty(name, (x, y, 0), parent)
    solid(name + '_rim', cube_mesh, (0, 0, 0.35), (4.3, 3.2, 0.7), TRAY, r)
    solid(name + '_well', cube_mesh, (0, 0, 0.705), (3.7, 2.6, 0.01), rgb('#c2b9a2'), r)
    text(label, (-2.15, -1.6 - 0.4, 0.01), (0, 0, 0), 0.42, INK, 'ExtraBold', r)
    return r

def disk(name, x, y, parent):
    r = empty(name, (x, y, 0), parent)
    for i in range(6):
        solid(f'{name}_p{i}', lambda: bpy.ops.mesh.primitive_cylinder_add(vertices=48, radius=1.8, depth=0.2), (0, 0, 0.1 + i * 0.24), (1, 1, 1),
              rgb('#efebe1') if i == 5 else rgb('#d6cfbf'), r)
    solid(name + '_hole', lambda: bpy.ops.mesh.primitive_cylinder_add(vertices=24, radius=0.38, depth=0.02), (0, 0, 1.42), (1, 1, 1), DARK, r, outlined=False)
    text('DISK', (-0.7, -2.6, 0.01), (0, 0, 0), 0.42, INK, 'ExtraBold', r)
    return r

def switch(name, x, y, parent):
    r = empty(name, (x, y, 0), parent)
    solid(name + '_b', cube_mesh, (0, 0, 0.45), (3.7, 2.0, 0.9), rgb('#3a362f'), r)
    for i in range(8): solid(f'{name}_port{i}', cube_mesh, (-1.5 + i * 0.42, -1.012, 0.42), (0.26, 0.01, 0.26), rgb('#7fd8a6') if i % 3 == 0 else rgb('#d8c98f'), r, outlined=False)
    text('NETWORK', (-1.85, -1.0 - 0.45, 0.01), (0, 0, 0), 0.42, INK, 'ExtraBold', r)
    return r

# ---------------- camera, HUD and grid ----------------
rig = empty('rig')
cam = bpy.data.objects.new('cam', bpy.data.cameras.new('cam')); C_FLAT.objects.link(cam); sc.camera = cam
cam.data.type = 'ORTHO'; cam.data.ortho_scale = S; cam.data.sensor_fit = 'HORIZONTAL'
cam.data.clip_start, cam.data.clip_end = 0.1, 400
cam.parent = rig
cam.location = (60, -60, 60 * math.sqrt(2) * 0.8165 / 0.5774 * 0.5774)
cam.rotation_euler = (math.radians(54.7356), 0, math.radians(45))
cam.location = (60, -60, 60 * math.tan(math.radians(35.2644)) * math.sqrt(2))

def frame_on(t, x, y):
    """From t on, the camera looks at floor point (x, y)."""
    for i, v in enumerate((x, y)): key(rig, 'location', t, v, index=i, interp='CONSTANT')

def hud(body, px, py, size_px, color=INK, weight='ExtraBold', align='LEFT', name='hud'):
    """Flat on-screen text, placed in the 1080x1100 design layout (px from the top-left)."""
    o = text(body, ((px - 540) * U, (550 - py) * U - size_px * U * 0.8, -20), (0, 0, 0), size_px * U, color, weight, cam, align, name)
    return o

def hud_card(px, py, wpx, hpx, color=rgb('#f8f5ee'), name='card', depth=-20.05, border=True):
    o = solid(name, lambda: bpy.ops.mesh.primitive_plane_add(size=1), ((px + wpx / 2 - 540) * U, (550 - py - hpx / 2) * U, depth), (wpx * U, hpx * U, 1), color, cam, outlined=False)
    if border and wpx > 20 and hpx > 20:
        e = solid(name + '_edge', lambda: bpy.ops.mesh.primitive_plane_add(size=1), (0, 0, -0.01), (1 + 5 / wpx, 1 + 5 / hpx, 1), INK, o, outlined=False)
    return o

grid_root = empty('grid')
gm = bpy.data.meshes.new('gridmesh'); verts, faces = [], []
N, hw = 40, 0.03                          # grid lines: thin flat strips one unit apart
for i in range(-N, N + 1):
    for (x0, y0, x1, y1) in ((-N, i - hw, N, i + hw), (i - hw, -N, i + hw, N)):
        k = len(verts); verts += [(x0, y0, 0), (x1, y0, 0), (x1, y1, 0), (x0, y1, 0)]; faces.append((k, k + 1, k + 2, k + 3))
gm.from_pydata(verts, [], faces)
g = bpy.data.objects.new('gridlines', gm); C_FLAT.objects.link(g); g.parent = grid_root
g.color = rgb('#d8ccb1'); g.data.materials.append(TXT)

# crumb bar and progress line, always on screen
hud('> BINARY HABIT / SPARK PERF / 03', 58, 64, 22, rgb('#4a453b'), 'Bold')
rule = hud_card(58, 100, 964, 2, rgb('#cfc6b0'), 'rule', -20.1, border=False)
prog = hud_card(58, 99, 964, 4, BLUE, 'prog', -20.09, border=False)
prog.location.x = (58 - 540) * U
for v in prog.data.vertices: v.co.x += 0.5           # grow from the left edge
key(prog, 'scale', 0, 0.0001, index=0, interp='LINEAR'); key(prog, 'scale', DUR, 964 * U, index=0, interp='LINEAR')

# live text (counters, typing), refreshed every frame
LIVE = []
def live(obj, fn): LIVE.append((obj, fn))
def on_frame(scene, *_):
    t = (scene.frame_current - 1) / FPS
    for o, fn in LIVE: o.data.body = fn(t)
bpy.app.handlers.frame_change_pre.append(on_frame)
def ramp(t, t0, d, a, b):
    k = min(max((t - t0) / d, 0), 1); return a + (b - a) * (1 - (1 - k) ** 3)
comma = lambda n: f'{int(round(n)):,}'

row4 = lambda i: -9 + i * 6          # four executors along x, 6 units apart
KEYS = ['in', 'us', 'br', 'de']
keyof = lambda i, k: KEYS[(i + k) % 4]

# ======================= SCENES =======================
WIN = {}                             # scene root name -> (t0, t1)
def scene(t0, t1, name):
    r = empty(name); WIN[r.name] = (t0, t1); return r
SC = []

# 0-8  100 executors; groupBy heats them up; the job slows down
r = scene(0, 8, 's0'); SC.append(r); frame_on(0, 0, 0)
nodes = []
for i in range(10):
    for j in range(10):
        n = empty(f'n{i}{j}', ((i - 4.5) * 1.5, (j - 4.5) * 1.5, 0), r)
        solid(f'n{i}{j}_b', cube_mesh, (0, 0, 0.4), (1.1, 1.1, 0.8), NODE, n)
        drop(n, 0.2 + math.hypot(i - 4.5, j - 4.5) * 0.12, 2.5, 0.6)
        nodes.append(n)
for k, n in enumerate(nodes):
    b = n.children[0]; tint(b, 4.6 + ((k * 37) % 100) * 0.012, HOT)
n1 = hud('24', 540, 130, 132, INK, 'ExtraBold', 'CENTER'); n1.parent = cam
live(n1, lambda t: str(int(round(ramp(t, 0, 2.2, 24, 100)))))
c1 = hud('SPARK EXECUTORS', 540, 270, 18, MUTED, 'Bold', 'CENTER')
for o in (n1, c1): alpha(o, 4.2, 1, 0, 0.4)
job = hud_card(222, 130, 636, 190, name='job'); alpha(job, 4.6, 0, 1)
jt = [hud('SPARK JOB · STAGE 2 OF 3', 252, 150, 15, MUTED, 'Bold'), hud('groupBy · count', 252, 196, 38), hud('00:04', 828, 196, 40, GREEN, 'ExtraBold', 'RIGHT'),
      hud('running…', 252, 286, 15, GREEN, 'Bold'), hud('0%', 828, 286, 15, GREEN, 'Bold', 'RIGHT')]
live(jt[2], lambda t: f'00:{int(round(ramp(t, 4.8, 3, 4, 27))):02d}')
live(jt[4], lambda t: f'{int(round(ramp(t, 4.8, 3, 0, 60)))}%')
bar_bg = hud_card(252, 250, 576, 30, rgb('#f1ece0'), 'barbg', -20.04)
bar = hud_card(252, 250, 576 * 0.6, 30, GREEN, 'bar', -20.03, border=False)
for v in bar.data.vertices: v.co.x += 0.5
bar.location.x = (252 - 540) * U
key(bar, 'scale', 4.8, 0.0001, index=0); key(bar, 'scale', 7.8, 576 * 0.6 * U, index=0)
for o in jt + [bar_bg, bar]: alpha(o, 4.6, 0, 1)
slow = hud('● SLOWING DOWN', 540, 1000, 20, RED, 'ExtraBold', 'CENTER'); alpha(slow, 6.3, 0, 1)
SCN0_HUD = [n1, c1, job, bar_bg, bar, slow] + jt

# 8-14  one billion orders rain into four executors
r = scene(8, 14, 's1'); SC.append(r); frame_on(8, 0, 1.5)
n2 = hud('985,661,228', 540, 140, 88, INK, 'ExtraBold', 'CENTER'); live(n2, lambda t: comma(ramp(t, 8, 1.6, 985661228, 1e9)))
c2 = hud('ORDERS', 540, 240, 18, MUTED, 'Bold', 'CENTER')
for i in range(4):
    b = box(f'e1_{i}', row4(i), 0, 4, 3, 3.2, BOX, f'EXECUTOR {i + 1}', r); drop(b, 9.2 + i * 0.12)
    for k in range(6):
        c = cube(f'c1_{i}{k}', row4(i) - 1 + (k % 3) * 1.0, -0.6 + (k // 3) * 1.2, 3.2, 'us', r); drop(c, 10.2 + i * 0.2 + k * 0.08, 4, 0.6)
more = text('+ 96 more executors', (-11, 4.2, 0.01), (0, 0, 0), 0.62, MUTED, 'Bold', r); alpha(more, 11.4, 0, 1, .5)
SCN1_HUD = [n2, c2]

# 14-20 the line of code; cubes take their key's colour
r = scene(14, 20, 's2'); SC.append(r); frame_on(14, 0, 1.5)
term = hud_card(58, 130, 964, 90, DARK, 'term')
tx = hud('>>> ', 86, 158, 30, rgb('#e8e2d0'), 'Regular')
CODE = 'df.groupBy("country").count()'
live(tx, lambda t: '>>> ' + CODE[:int(max(0, min(len(CODE), (t - 14.5) / 3.4 * len(CODE))))] + ('▌' if int(t * 2) % 2 else ' '))
for i in range(4):
    box(f'e2_{i}', row4(i), 0, 4, 3, 3.2, BOX, f'EXECUTOR {i + 1}', r)
    for k in range(6):
        c = cube(f'c2_{i}{k}', row4(i) - 1 + (k % 3) * 1.0, -0.6 + (k // 3) * 1.2, 3.2, 'us', r)
        tint(c.children[0], 18 + i * 0.1 + k * 0.05, KEYC[keyof(i, k)])
SCN2_HUD = [term, tx]

# 20-25 same key -> same partition; 25-32 the cubes move
def cluster(name, t_trays):
    r = empty(name)
    for i in range(4):
        box(f'{name}_e{i}', row4(i), 5, 4, 3, 3.2, BOX, f'EXECUTOR {i + 1}', r)
    trays = []
    for i, k in enumerate(KEYS):
        tr = tray(f'{name}_t{i}', row4(i), -5, f'PARTITION · {k.upper()}', r); trays.append(tr)
        if t_trays is not None: drop(tr, t_trays + i * 0.12, 3)
    return r, trays

r, _ = cluster('s3', 20.4); WIN[r.name] = (20, 25); SC.append(r); frame_on(20, 1, 1)
h3 = hud('same key → same partition', 540, 130, 44, INK, 'ExtraBold', 'CENTER')
for i in range(4):
    for k in range(6):
        cube(f'c3_{i}{k}', row4(i) - 1 + (k % 3) * 1.0, 4.4 + (k // 3) * 1.2, 3.2, keyof(i, k), r)
    k = next(k for k in range(6) if keyof(i, k) == 'in')
    p0 = (row4(i) - 1 + (k % 3) * 1.0, 4.4 + (k // 3) * 1.2, 4.1); p2 = (row4(0), -5, 0.8)
    cu = bpy.data.curves.new(f'arc{i}', 'CURVE'); cu.dimensions = '3D'; cu.bevel_depth = 0.05
    sp = cu.splines.new('BEZIER'); sp.bezier_points.add(1)
    a, bpt = sp.bezier_points
    a.co = p0; bpt.co = p2
    a.handle_right = (p0[0], p0[1], p0[2] + 5); a.handle_left = p0
    bpt.handle_left = (p2[0], p2[1], p2[2] + 6); bpt.handle_right = p2
    arc = bpy.data.objects.new(f'arc{i}', cu); C_FLAT.objects.link(arc); arc.parent = r; arc.color = BLUE
    cu.materials.append(TXT)
    key(cu, 'bevel_factor_end', 0, 0.0); key(cu, 'bevel_factor_end', 21.2 + i * 0.35, 0.0); key(cu, 'bevel_factor_end', 22.2 + i * 0.35, 1.0)
SCN3_HUD = [h3]

r, trays = cluster('s4', None); WIN[r.name] = (25, 32); SC.append(r); frame_on(25, 1, 1)
h4 = hud('data moves between executors', 540, 130, 44, INK, 'ExtraBold', 'CENTER')
fill = [0, 0, 0, 0]
for i in range(4):
    for k in range(6):
        kk = keyof(i, k); dst = KEYS.index(kk); slot = fill[dst]; fill[dst] += 1
        c = cube(f'c4_{i}{k}', row4(i) - 1 + (k % 3) * 1.0, 4.4 + (k // 3) * 1.2, 3.2, kk, r)
        hop(c, 25.6 + k * 0.28 + i * 0.12, (row4(dst) - 1 + (slot % 3) * 1.0, -5.5 + (slot // 3) * 1.0, 0.71), 5, 0.9)
done = text('✓ every key in one place', (-9, -8.6, 0.01), (0, 0, 0), 0.75, GREEN, 'ExtraBold', r); alpha(done, 29.4, 0, 1, .5)
SCN4_HUD = [h4]

# 32-46 one record through the six steps of a shuffle
def rig_scene(name, t0, t1, stage=False):
    r = empty(name); WIN[r.name] = (t0, t1); SC.append(r); frame_on(t0, 0, 0)
    box(name + '_a', -10, 2, 4, 3, 3.2, BOX, 'EXECUTOR 2', r)
    disk(name + '_d', -2, 9, r)
    switch(name + '_n', 1, -8, r)
    box(name + '_b', 10, 1, 4, 3, 3.2, STAGE if stage else BOX, 'STAGE 2' if stage else 'EXECUTOR 4', r)
    return r
STEP_HUD = []
def step_hud(n, title, t0, t1, cost):
    items = ['serialize', 'disk I/O', 'network I/O', 'deserialize']
    hs = [hud(f'STEP {n} / 6', 62, 124, 18, MUTED, 'Bold'), hud(title, 62, 152, 44, INK, 'ExtraBold'),
          hud_card(782, 124, 240, 180, name=f'cost{n}'), hud('SHUFFLE COST', 800, 138, 12, MUTED, 'Bold')]
    for i, it in enumerate(items):
        on = i < cost
        hs.append(hud(('■ ' if on else '□ ') + it, 800, 166 + i * 32, 18, RED if on else rgb('#c9c1ae'), 'Bold'))
    for h in hs: STEP_HUD.append((h, t0, t1))
r = rig_scene('s5a', 32, 33.2)
card = hud_card(520, 140, 480, 170, name='rec'); rec = [hud('ORDER RECORD', 548, 158, 13, MUTED, 'Bold'), hud('order_id  48213907', 548, 190, 26, INK, 'Regular'),
                                                hud('country   IN', 548, 228, 26, INK, 'Regular'), hud('amount    ₹1,240', 548, 266, 26, INK, 'Regular')]
for h in [card] + rec: STEP_HUD.append((h, 32, 33.2))
pk = cube('pk0', -10, 2, 3.2, 'in', r); drop(pk, 32.3, 3)
r = rig_scene('s5b', 33.2, 34.2); step_hud(1, 'serialize', 33.2, 34.2, 1)
pk = cube('pk1', -10, 2, 3.2, 'in', r); alpha(pk, 33.5, 1, 0, .4)
bits = hud_card(60, 880, 330, 44, DARK, 'bits1'); bt = hud('01001001 01001110 00110100', 76, 892, 18, rgb('#e8e2d0'), 'Regular')
for h in (bits, bt): STEP_HUD.append((h, 33.6, 34.2))
r = rig_scene('s5c', 34.2, 36.2); step_hud(2, 'write to disk', 34.2, 36.2, 2)
pk = cube('pk2', -10, 2, 3.2, 'in', r); hop(pk, 34.5, (-2, 9, 1.44), 4, .9); alpha(pk, 35.5, 1, 0, .3)
r = rig_scene('s5d', 36.2, 41.2); step_hud(3, 'send over network', 36.2, 41.2, 3)
pk = cube('pk3', -2, 9, 1.44, 'in', r); hop(pk, 37.6, (1, -8, 0.9), 4, .9); hop(pk, 38.6, (10, 1, 3.2), 4, .9)
r = rig_scene('s5e', 41.2, 43.2, True); step_hud(4, 'fetch', 41.2, 43.2, 3)
for k in range(4):
    c = cube(f'pk4{k}', 1, -8, 0.9, KEYS[k], r); hop(c, 41.4 + k * .3, (9.5 + (k % 2) * 1.0, 0.5 + (k // 2) * 1.0, 3.2), 3, .7)
r = rig_scene('s5f', 43.2, 44.2, True); step_hud(5, 'deserialize', 43.2, 44.2, 4)
for k in range(4): cube(f'pk5{k}', 9.5 + (k % 2) * 1.0, 0.5 + (k // 2) * 1.0, 3.2, KEYS[k], r)
r = rig_scene('s5g', 44.2, 46, True); step_hud(6, 'aggregate', 44.2, 46, 4)
aggc = hud_card(400, 300, 380, 110, name='aggc'); agg = hud('0', 590, 322, 58, INK, 'ExtraBold', 'CENTER')
live(agg, lambda t: comma(ramp(t, 44.5, 1.2, 0, 2003311)))
for h in (aggc, agg): STEP_HUD.append((h, 44.4, 46))

# 46-56 shuffle overhead: cost blocks pile up on the belt, stage 2 waits
r = scene(46, 56, 's6'); SC.append(r); frame_on(46, 0, 0)
oh = hud('shuffle overhead', 540, 130, 50, INK, 'ExtraBold', 'CENTER')
box('s6_a', -8, 0, 4, 3, 3.2, BOX, 'STAGE 1', r); box('s6_b', 8, 0, 4, 3, 3.2, STAGE, 'STAGE 2', r)
bb = empty('belt', (0, 0, 0), r); solid('belt_b', cube_mesh, (0, 0, 0.25), (10, 2.3, 0.5), BELT, bb)
for i, t in enumerate(['serial.', 'disk', 'network', 'deser.']):
    c = empty(f'cost{i}', (-3 + i * 2, 0, 0.5), r)
    solid(f'cost{i}_b', cube_mesh, (0, 0, 0.8), (1.45, 1.45, 1.6), RED, c)
    text(t, (-0.68, -0.735, 1.15), (90, 0, 0), 0.24, rgb('#ffffff'), 'Bold', c)
    drop(c, 46.3 + i * .35, 3.5, .9)
key(r, 'scale', 48.6, (1, 1, 1)); key(r, 'scale', 49.6, (0.6, 0.6, 0.6))
key(r, 'location', 48.6, (0, 0, 0)); key(r, 'location', 49.6, (-7.5, 7.5, 0))
TASKS = []
for i in range(1, 5):
    y = 470 + (i - 1) * 132; w0 = (54 - i * 8) / 100
    TASKS += [hud(f'STAGE 2 · TASK {i}', 150, y - 30, 14, MUTED, 'Bold'), hud_card(150, y, 780, 50, rgb('#f8f5ee'), f'tb{i}')]
    wbar = hud_card(150, y, 780 * w0, 50, rgb('#ddd5c2'), f'tw{i}', -20.04, border=False); wbar.location.x = (150 - 540) * U
    for v in wbar.data.vertices: v.co.x += 0.5
    key(wbar, 'scale', 49.2 + i * .25, 0.0001, index=0); key(wbar, 'scale', 51.6 + i * .25, 780 * w0 * U, index=0)
    dbar = hud_card(150 + 780 * w0, y, 780 * (0.12 + i * 0.02), 50, GREEN, f'td{i}', -20.03, border=False); dbar.location.x = (150 + 780 * w0 - 540) * U
    for v in dbar.data.vertices: v.co.x += 0.5
    key(dbar, 'scale', 52.4 + i * .4, 0.0001, index=0); key(dbar, 'scale', 53.4 + i * .4, 780 * (0.12 + i * 0.02) * U, index=0)
    wt = hud('waiting for data…', 166, y + 14, 18, MUTED, 'Regular')
    TASKS += [wbar, dbar, wt]
    for o in TASKS[-5:]: alpha(o, 49.2 + i * 0.25, 0, 1, .4)
key(oh, 'location', 48.6, oh.location.copy()); oh.location.y += 18 * U; key(oh, 'location', 49.6, oh.location.copy())
SCN6_HUD = [oh] + TASKS

# 56-63 fix: move less data, or spread it over more executors
r = scene(56, 63, 's7'); SC.append(r); frame_on(56, 0, -1)
f1 = [hud('FIX THE OVERHEAD', 62, 124, 18, MUTED, 'Bold'), hud('– less data', 62, 152, 44, GREEN, 'ExtraBold'), hud('+ more executors', 62, 212, 44, RED, 'ExtraBold')]
alpha(f1[1], 57.8, 0, 1); alpha(f1[2], 61, 0, 1)
box('s7_a', -8, 0, 4, 3, 3.2, BOX, 'STAGE 1', r); box('s7_b', 8, 0, 4, 3, 3.2, STAGE, 'STAGE 2', r)
bb = empty('belt7', (0, 0, 0), r); solid('belt7_b', cube_mesh, (0, 0, 0.25), (10, 2.3, 0.5), BELT, bb)
for i in range(12):
    c = cube(f'c7_{i}', -4 + (i % 6) * 1.0, -0.5 + (i // 6) * 1.0, 0.5, KEYS[i % 4], r); drop(c, 56.2 + i * .05, 3, .5)
    if i % 3 == 0: alpha(c, 58.4 + i * .02, 1, 0, .4)
    else: hop(c, 59 + (i % 6) * .06, (4 + (i % 6) * 0.2, c.location.y, 0.5), 0.3, 1.6)
for i, (x, y) in enumerate([(-8, -8), (8, -8), (-8, 8), (8, 8)]):
    b = box(f's7_x{i}', x, y, 4, 3, 3.2, BOX, None, r); drop(b, 61.3 + i * .15)
SCN7_HUD = f1

# 63-67 fix: filter rows and columns before the shuffle
r = scene(63, 67, 's8'); SC.append(r); frame_on(63, 0, 0)
f2 = [hud('BEFORE THE SHUFFLE', 62, 124, 18, MUTED, 'Bold'), hud('filter rows + columns', 62, 152, 44, INK, 'ExtraBold')]
cols = ['order_id', 'country', 'amount', 'item', 'ts', 'notes']
for rr in range(7):
    for q in range(6):
        o = empty(f'row{rr}{q}', ((q - 2.5) * 3.4, (3 - rr) * 1.7, 0), r)
        solid(f'row{rr}{q}_b', cube_mesh, (0, 0, 0.15), (3.1, 1.4, 0.3), rgb('#dfe6f6') if rr == 0 else BOX, o)
        if rr == 0: text(cols[q], (-1.4, -0.3, 0.31), (0, 0, 0), 0.42, BLUE, 'ExtraBold', o)
        else: solid(f'row{rr}{q}_l', cube_mesh, (0, 0, 0.305), (2.2, 0.12, 0.01), rgb('#c2b9a3'), o, outlined=False)
        drop(o, 63.1 + (rr * 6 + q) * 0.012, 2, .5)
        if q > 2 or (rr and rr % 2 == 0): alpha(o, 65.3 if q > 2 else 64.4, 1, 0.1, .6)
SCN8_HUD = f2

# 67-70 avoid unnecessary shuffles
r = scene(67, 70, 's9'); SC.append(r); frame_on(67, 0, 0)
f3 = [hud('AND', 62, 124, 18, MUTED, 'Bold'), hud('avoid unnecessary shuffles', 62, 152, 44, RED, 'ExtraBold'),
      hud_card(310, 360, 460, 320, name='xcard'), hud('⇄', 540, 420, 130, INK, 'ExtraBold', 'CENTER'), hud('extra shuffle', 540, 590, 32, INK, 'ExtraBold', 'CENTER')]
strike = hud_card(300, 515, 480, 10, RED, 'strike', -20.0, border=False); strike.rotation_euler.z = math.radians(30)
key(strike, 'scale', 68.3, 0.0001, index=0); key(strike, 'scale', 68.8, 480 * U, index=0)
f3 += [strike, hud('✓ less data moved · faster job', 540, 740, 17, GREEN, 'Bold', 'CENTER')]
alpha(f3[-1], 68.9, 0, 1)
SCN9_HUD = f3

# 70-78 the powerful trick: copy the small table to every executor (broadcast), no shuffle
r = scene(70, 78, 's10'); SC.append(r); frame_on(70, 0, 0)
f4 = [hud('LESS DATA MOVEMENT', 62, 124, 18, MUTED, 'Bold'), hud('a powerful trick…', 62, 152, 44, INK, 'ExtraBold')]
spots = [(-8, -5), (8, -5), (-8, 6), (8, 6)]
for i, (x, y) in enumerate(spots):
    b = box(f's10_e{i}', x, y, 4, 3, 3.2, BOX, f'EXEC {i + 1}', r); drop(b, 70.4 + i * .12)
lk = empty('lookup', (0, 0.5, 0), r); solid('lookup_b', cube_mesh, (0, 0, 0.3), (2.4, 1.7, 0.6), rgb('#e3eaf8'), lk)
text('countries', (-1.05, -0.2, 0.61), (0, 0, 0), 0.36, BLUE, 'ExtraBold', lk); drop(lk, 71.3, 3)
for i, (x, y) in enumerate(spots):
    cp = empty(f'lk{i}', (0, 0.5, 0.6), r); solid(f'lk{i}_b', cube_mesh, (0, 0, 0.2), (1.7, 1.2, 0.4), rgb('#e3eaf8'), cp)
    alpha(cp, 72.6 + i * .35, 0, 1, .2); hop(cp, 72.6 + i * .35, (x, y, 3.2), 5, .8)
bj = text('broadcast join · no shuffle', (-4.5, 10.5, 0.01), (0, 0, 0), 0.7, BLUE, 'ExtraBold', r); alpha(bj, 75, 0, 1, .5)
SCN10_HUD = f4

# 78-end title card with an isometric cube logo
r = scene(78, DUR + 1, 's11'); SC.append(r); frame_on(78, 0, 0)
logo = empty('logo', (0, 0, 0), r); solid('logo_b', cube_mesh, (0, 0, 1.4), (2.8, 2.8, 2.8), rgb('#3560c0'), logo)
logo.location = (-3.5, 3.5, 3.0); drop(logo, 78.1, 3, .8)
f5 = [hud('DATA / 03', 540, 600, 22, MUTED, 'Bold', 'CENTER'), hud('Spark Performance', 540, 640, 66, INK, 'ExtraBold', 'CENTER'),
      hud('Episode 3 · Shuffling', 540, 730, 28, MUTED, 'Regular', 'CENTER'), hud('IN    US    BR    DE', 540, 800, 24, BLUE, 'ExtraBold', 'CENTER'),
      hud('BinaryHabit.', 540, 880, 44, INK, 'ExtraBold', 'CENTER')]
for i, h in enumerate(f5): alpha(h, 78.3 + i * .2, 0, 1, .4)
SCN11_HUD = f5

# ---------------- visibility windows ----------------
for r in SC: visible(r, *WIN[r.name])
HUD_WINDOWS = [(SCN0_HUD, (0, 8)), (SCN1_HUD, (8, 14)), (SCN2_HUD, (14, 20)), (SCN3_HUD, (20, 25)), (SCN4_HUD, (25, 32)),
                       (SCN6_HUD, (46, 56)), (SCN7_HUD, (56, 63)), (SCN8_HUD, (63, 67)), (SCN9_HUD, (67, 70)), (SCN10_HUD, (70, 78)), (SCN11_HUD, (78, DUR + 1))]
for objs, (t0, t1) in HUD_WINDOWS:
    for o in objs: visible(o, t0, t1)
for o, t0, t1 in STEP_HUD: visible(o, t0, t1)

# ---------------- keep only what the rendered frames need ----------------
def prune(f0, f1):
    """Delete scenes and on-screen text that never show in frames f0..f1 (far fewer objects to process per frame)."""
    t0, t1 = (f0 - 1) / FPS, (f1 - 1) / FPS
    groups = [(r, WIN[r.name]) for r in SC]
    groups += [(o, w) for objs, w in HUD_WINDOWS for o in objs] + [(o, (a, b)) for o, a, b in STEP_HUD]
    doomed = set()
    for o, (a, b) in groups:
        if b < t0 - 0.5 or a > t1 + 0.5:
            doomed.update([o] + list(o.children_recursive))
    LIVE[:] = [(o, fn) for o, fn in LIVE if o not in doomed]
    for o in doomed: bpy.data.objects.remove(o, do_unlink=True)

# ---------------- export for the web (glTF) ----------------
def keys_of(o, path, index):
    ad = o.animation_data
    if not ad or not ad.action: return []
    for fc in ad.action.fcurves:
        if fc.data_path == path and fc.array_index == index:
            return [[round((k.co[0] - 1) / FPS, 3), round(k.co[1], 4)] for k in fc.keyframe_points]
    return []

def export_gltf(path):
    """Write the 3D scene (no on-screen text) as .glb plus an animation file (.anim.json) read from the keyframes.

    Every object is renamed o<N> so the names survive the trip to the browser unchanged. The animation file holds,
    per object: frames where its location/scale change, its show windows, colour keys and (arcs) draw-on keys."""
    import json
    LIVE.clear(); bpy.app.handlers.frame_change_pre.clear()
    for o in set(cam.children_recursive): bpy.data.objects.remove(o, do_unlink=True)
    bpy.data.objects.remove(bpy.data.objects['gridlines'], do_unlink=True)
    sc.frame_set(1)
    # text and curves become meshes (same parent, transform and animation)
    dg = bpy.context.evaluated_depsgraph_get()
    for o in list(bpy.data.objects):
        if o.type not in ('FONT', 'CURVE'): continue
        if o.type == 'CURVE': o.data.bevel_factor_end = 1.0; dg.update()
        me = bpy.data.meshes.new_from_object(o.evaluated_get(dg))
        m = bpy.data.objects.new(o.name + '_m', me); (C_LINE if o.name in C_LINE.objects else C_FLAT).objects.link(m)
        m.parent = o.parent; m.matrix_parent_inverse = o.matrix_parent_inverse.copy()
        m.location, m.rotation_euler, m.scale = o.location, o.rotation_euler, o.scale
        m.color = o.color
        if o.animation_data and o.animation_data.action:
            m.animation_data_create(); m.animation_data.action = o.animation_data.action
        if o.type == 'CURVE':
            ad = o.data.animation_data
            m['draw'] = [[(k.co[0] - 1) / FPS, k.co[1]] for fc in ad.action.fcurves for k in fc.keyframe_points] if ad and ad.action else []
        m['kind'] = 'text' if o.type == 'FONT' else 'arc'
        bpy.data.objects.remove(o, do_unlink=True)
    anim, mats, N = {'fps': FPS, 'dur': DUR, 'objects': {}}, {}, sc.frame_end
    for i, o in enumerate(list(bpy.data.objects)):
        outlined = o.name in C_LINE.objects       # check before renaming: collections look objects up by name
        o.name = 'o%d' % i
        a = {'kind': o.get('kind', 'line' if outlined else 'flat'), 'color': [round(c, 4) for c in o.color]}
        if o.type == 'MESH':
            col = tuple(round(c, 3) for c in o.color[:3])
            if col not in mats:
                mm = bpy.data.materials.new('m%d' % len(mats)); mm.use_nodes = True
                mm.node_tree.nodes['Principled BSDF'].inputs['Base Color'].default_value = (*col, 1); mats[col] = mm
            o.data.materials.clear(); o.data.materials.append(mats[col])
        if 'draw' in o: a['draw'] = [[round(t, 3), round(v, 3)] for t, v in o['draw']]
        ad = o.animation_data
        if ad and ad.action:
            fcs = {(fc.data_path, fc.array_index): fc for fc in ad.action.fcurves}
            hr = fcs.get(('hide_render', 0))
            if hr:
                win, on = [], None
                for k in hr.keyframe_points:
                    t, v = (k.co[0] - 1) / FPS, k.co[1]
                    if v < .5 and on is None: on = t
                    elif v >= .5 and on is not None: win.append([round(on, 3), round(t, 3)]); on = None
                if on is not None: win.append([round(on, 3), 999])
                a['show'] = win
            for ch, idx in zip('rgba', range(4)):
                fc = fcs.get(('color', idx))
                if fc: a[ch] = [[round((k.co[0] - 1) / FPS, 3), round(k.co[1], 4)] for k in fc.keyframe_points]
            tr = [fcs.get((p, j)) for p in ('location', 'scale') for j in range(3)]
            if any(tr):
                base = list(o.location) + list(o.scale); frames, last = [], None
                for f in range(1, N + 1):
                    v = [round(fc.evaluate(f), 3) if fc else base[j] for j, fc in enumerate(tr)]
                    if v != last: frames.append([f - 1] + v); last = v
                a['tr'] = frames
        anim['objects'][o.name] = a
    with open(path.replace('.glb', '.anim.json'), 'w') as fh: json.dump(anim, fh, separators=(',', ':'))
    bpy.ops.export_scene.gltf(filepath=path, export_format='GLB', export_cameras=True, export_lights=False,
                              export_animations=False, export_apply=False, export_yup=True, export_texcoords=False,
                              export_normals=True, export_materials='EXPORT', export_extras=False)
    print('EXPORTED', path)

if '--export' in argv:
    export_gltf(os.path.abspath(argv[argv.index('--export') + 1]))
    sys.exit(0)

# ---------------- render ----------------
os.makedirs(OUT, exist_ok=True)
if '--frames' in argv:
    a_, b_ = argv[argv.index('--frames') + 1].split('-'); prune(int(a_), int(b_))
if '--still' in argv:
    for t in [float(x) for x in argv[argv.index('--still') + 1].split(',')]:
        sc.frame_set(F(t)); on_frame(sc)
        sc.render.filepath = os.path.join(OUT, f'still_{t:05.1f}.png')
        bpy.ops.render.render(write_still=True)
else:
    if '--frames' in argv:
        a, b = argv[argv.index('--frames') + 1].split('-'); sc.frame_start, sc.frame_end = int(a), int(b)
    sc.render.filepath = os.path.join(OUT, 'f')
    bpy.ops.render.render(animation=True)
if '--save' in argv:
    bpy.ops.wm.save_as_mainfile(filepath=os.path.join(HERE, 'shuffle.blend'))
