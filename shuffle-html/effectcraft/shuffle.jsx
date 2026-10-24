// EffectCraft script: builds the top-half motion graphics of the Spark shuffle short.
// Run:  effectcraft-cli script shuffle.jsx --empty --save-as shuffle.ecproj
// Then: effectcraft-cli exec file.exportLottie --project shuffle.ecproj --params '{"comp":"Graphics","path":"graphics.json","textAsShapes":true}'
// Times are seconds in the source footage (shuffle.mp4); the comp is the top half, 1080x960.

var W = 1080, H = 960, DUR = 80.4, FPS = 30;
var comp = app.project.items.addComp("Graphics", W, H, 1, DUR, FPS);

// palette
var PAPER = [0.925, 0.91, 0.863], GRIDC = [0.866, 0.847, 0.788], INK = [0.114, 0.106, 0.09];
var MUTED = [0.54, 0.52, 0.46], BOXC = [0.965, 0.953, 0.918], TOPC = [0.984, 0.976, 0.953], EDGE = [0.49, 0.463, 0.4];
var BLUE = [0.12, 0.25, 0.56], RED = [0.7, 0.23, 0.165], GREEN = [0.184, 0.42, 0.333], DARK = [0.122, 0.114, 0.102];
var AMBER = [0.784, 0.47, 0.184], WHITE = [1, 1, 1], SHADE = [0.84, 0.815, 0.75];
var MONO = "JetBrainsMono-Bold", MONO_R = "JetBrainsMono-Regular";

// ---------- low-level helpers ----------
function tg(l) { return l.property("ADBE Transform Group"); }
function vecs(g) { return g.property("ADBE Vectors Group"); }

function life(l, t0, t1) { l.inPoint = Math.max(0, t0); l.outPoint = Math.min(DUR, t1); }

// entrance animation on a layer: kind = fade | up | drop | pop, starts at `at`
function enter(l, kind, at, len) {
  len = len || 0.45;
  var o = tg(l).property("ADBE Opacity");
  o.setValueAtTime(at, 0); o.setValueAtTime(at + len * 0.7, 100);
  var p = tg(l).property("ADBE Position"), base = p.value;
  if (kind === "up")   { p.setValueAtTime(at, [base[0], base[1] + 30]); p.setValueAtTime(at + len, base); }
  if (kind === "drop") { p.setValueAtTime(at, [base[0], base[1] - 60]); p.setValueAtTime(at + len, base); }
  if (kind === "pop") {
    var s = tg(l).property("ADBE Scale");
    s.setValueAtTime(at, [60, 60]); s.setValueAtTime(at + len, [100, 100]);
  }
  easeAll(l);
}
function easeAll(l) {
  var props = ["ADBE Opacity", "ADBE Position", "ADBE Scale"];
  for (var i = 0; i < props.length; i++) {
    var p = tg(l).property(props[i]);
    for (var k = 1; k <= p.numKeys; k++) {
      var e = [new KeyframeEase(0, 66)];
      if (props[i] !== "ADBE Opacity") e = p.value.length === 3 ? [e[0], e[0], e[0]] : [e[0], e[0]];
      try { p.setTemporalEaseAtKey(k, e, e); } catch (err) {}
    }
  }
}

// a shape layer whose origin is (x, y); content is drawn by `draw(contents)`
function shapeLayer(name, x, y, t0, t1, draw) {
  var l = comp.layers.addShape(); l.name = name;
  tg(l).property("ADBE Position").setValue([x, y]);
  tg(l).property("ADBE Anchor Point").setValue([0, 0]);
  draw(l.property("ADBE Root Vectors Group"));
  life(l, t0, t1);
  return l;
}
function group(root) { return root.addProperty("ADBE Vector Group"); }
function fillStroke(g, fill, stroke, sw) {
  if (fill) vecs(g).addProperty("ADBE Vector Graphic - Fill").property("ADBE Vector Fill Color").setValue(fill);
  if (stroke) {
    var s = vecs(g).addProperty("ADBE Vector Graphic - Stroke");
    s.property("ADBE Vector Stroke Color").setValue(stroke);
    s.property("ADBE Vector Stroke Width").setValue(sw || 2);
    return s;
  }
}
// rectangle with top-left at (x, y)
function rect(root, x, y, w, h, fill, stroke, sw, round) {
  var g = group(root), r = vecs(g).addProperty("ADBE Vector Shape - Rect");
  r.property("ADBE Vector Rect Size").setValue([w, h]);
  r.property("ADBE Vector Rect Position").setValue([x + w / 2, y + h / 2]);
  if (round) r.property("ADBE Vector Rect Roundness").setValue(round);
  fillStroke(g, fill, stroke, sw);
  return g;
}
function ellipse(root, cx, cy, w, h, fill, stroke, sw) {
  var g = group(root), e = vecs(g).addProperty("ADBE Vector Shape - Ellipse");
  e.property("ADBE Vector Ellipse Size").setValue([w, h]);
  e.property("ADBE Vector Ellipse Position").setValue([cx, cy]);
  fillStroke(g, fill, stroke, sw);
  return g;
}
// free path; pts = [[x,y],...], optional bezier tangents
function path(root, pts, closed, fill, stroke, sw, ins, outs) {
  var g = group(root), sh = new Shape();
  sh.vertices = pts; sh.closed = !!closed;
  if (ins) sh.inTangents = ins;
  if (outs) sh.outTangents = outs;
  vecs(g).addProperty("ADBE Vector Shape - Group").property("ADBE Vector Shape").setValue(sh);
  var s = fillStroke(g, fill, stroke, sw);
  return { group: g, stroke: s };
}
// line-drawing animation (Trim Paths end 0 -> 100)
function drawOn(g, at, len) {
  var t = vecs(g).addProperty("ADBE Vector Filter - Trim");
  var e = t.property("ADBE Vector Trim End");
  e.setValueAtTime(at, 0); e.setValueAtTime(at + (len || 1), 100);
}
function dash(stroke, d, gap) {
  try {
    var ds = stroke.property("ADBE Vector Stroke Dashes");
    ds.addProperty("ADBE Vector Stroke Dash 1").setValue(d);
    ds.addProperty("ADBE Vector Stroke Gap 1").setValue(gap);
  } catch (e) {}
}

function text(str, x, y, size, color, t0, t1, opt) {
  opt = opt || {};
  var l = comp.layers.addText(str);
  var src = l.property("ADBE Text Properties").property("ADBE Text Document"), td = src.value;
  td.fontSize = size; td.fillColor = color; td.font = opt.font || MONO;
  td.tracking = opt.tracking || 0;
  td.justification = opt.center ? ParagraphJustification.CENTER_JUSTIFY :
                     opt.right ? ParagraphJustification.RIGHT_JUSTIFY : ParagraphJustification.LEFT_JUSTIFY;
  src.setValue(td);
  tg(l).property("ADBE Position").setValue([x, y]);
  life(l, t0, t1);
  return l;
}
// animated number: sourceText keyed every frame-ish from a to b over [at, at+len]
function counter(a, b, x, y, size, color, t0, t1, at, len, opt) {
  var l = text(fmt(a), x, y, size, color, t0, t1, opt);
  var src = l.property("ADBE Text Properties").property("ADBE Text Document");
  var steps = Math.round(len * 15);
  for (var i = 0; i <= steps; i++) {
    var k = i / steps, e = 1 - Math.pow(1 - k, 3);
    var td = src.value; td.text = (opt && opt.pad ? pad2(Math.round(a + (b - a) * e)) : fmt(Math.round(a + (b - a) * e)));
    src.setValueAtTime(at + len * k, td);
  }
  return l;
}
// typewriter: reveal `str` one character at a time
function typewriter(str, x, y, size, color, t0, t1, at, len, opt) {
  var l = text(" ", x, y, size, color, t0, t1, opt);
  var src = l.property("ADBE Text Properties").property("ADBE Text Document");
  for (var i = 0; i <= str.length; i++) {
    var td = src.value; td.text = str.substr(0, i) + (i < str.length ? "▌" : "");
    if (td.text === "") td.text = "▌";
    src.setValueAtTime(at + len * i / str.length, td);
  }
  return l;
}
function fmt(n) { var s = String(n), o = ""; while (s.length > 3) { o = "," + s.slice(-3) + o; s = s.slice(0, -3); } return s + o; }
function pad2(n) { return n < 10 ? "0" + n : String(n); }

// ---------- scene furniture ----------
function paper(t0, t1) {
  var bg = comp.layers.addSolid(PAPER, "paper", W, H, 1); life(bg, t0, t1);
  shapeLayer("grid", 0, 0, t0, t1, function (root) {
    for (var x = 0; x <= W; x += 43) rect(root, x, 0, 1.5, H, GRIDC);
    for (var y = 0; y <= H; y += 43) rect(root, 0, y, W, 1.5, GRIDC);
  });
  text("> BINARY HABIT / SPARK PERF / 03", 32, 62, 26, [0.33, 0.33, 0.33], t0, t1, { font: MONO_R, tracking: 300 });
  shapeLayer("rule", 0, 0, t0, t1, function (root) { rect(root, 32, 82, W - 64, 2, [0.79, 0.76, 0.69]); });
  var prog = shapeLayer("progress", 32, 82, t0, t1, function (root) { rect(root, 0, -1, W - 64, 4, BLUE); });
  var s = tg(prog).property("ADBE Scale");
  s.setValueAtTime(0, [0, 100]); s.setValueAtTime(DUR, [100, 100]);
}
// a faux-3D box (front + skewed top) at (x, y), size w x h, with label
function box(name, x, y, w, h, label, t0, t1, at, opt) {
  opt = opt || {};
  var l = shapeLayer(name, x, y, t0, t1, function (root) {
    path(root, [[0, 0], [w, 0], [w + 24, -26], [24, -26]], true, TOPC, EDGE, 2);
    rect(root, 0, 0, w, h, BOXC, EDGE, 2, 4);
    if (opt.dot !== false) ellipse(root, w - 16, 16, 12, 12, GREEN);
    if (opt.bar) rect(root, 14, h - 14, w - 28, 8, GREEN, null, 0, 3);
    if (opt.cells) for (var j = 0; j < opt.cells.length; j++) {
      var c = opt.cells[j];
      rect(root, 14 + (j % 3) * 50, 12 + Math.floor(j / 3) * 30, 40, 24, c ? BLUE : WHITE, c ? BLUE : EDGE, 1.5);
    }
  });
  if (label) {
    var t = text(label, x + 14, y + h - (opt.bar ? 26 : 16), 17, INK, t0, t1, { tracking: 120 });
    if (at !== null && at !== undefined) enter(t, "drop", at);
  }
  if (at !== null && at !== undefined) enter(l, "drop", at);
  return l;
}
function disk(x, y, t0, t1, at) {
  var l = shapeLayer("disk", x, y, t0, t1, function (root) {
    for (var i = 3; i >= 0; i--) ellipse(root, 0, i * 9, 170, 62, i ? [0.81, 0.795, 0.75] : [0.913, 0.902, 0.87], [0.53, 0.53, 0.53], 1.5);
    ellipse(root, 0, 0, 24, 12, [0.13, 0.13, 0.13]);
  });
  if (at !== null) enter(l, "pop", at);
}
function card(x, y, w, h, t0, t1, at) {
  var l = shapeLayer("card", x, y, t0, t1, function (root) {
    rect(root, 6, 8, w, h, [0.82, 0.8, 0.74], null, 0, 6); // shadow
    rect(root, 0, 0, w, h, BOXC, [0.6, 0.57, 0.5], 2, 6);
  });
  if (at !== null) enter(l, "pop", at);
  return l;
}
function terminal(x, y, w, h, t0, t1, at) {
  var l = shapeLayer("terminal", x, y, t0, t1, function (root) { rect(root, 0, 0, w, h, DARK, null, 0, 6); });
  if (at !== null) enter(l, "up", at);
}
function headline(str, y, t0, t1, at, color) { enter(text(str, W / 2, y, 46, color || INK, t0, t1, { center: true }), "up", at); }
function stepHead(n, title, t0, t1) {
  enter(text("STEP " + n + " / 6", 40, 150, 22, MUTED, t0, t1, { font: MONO_R, tracking: 200 }), "fade", t0);
  enter(text(title, 40, 205, 46, INK, t0, t1), "up", t0 + 0.1);
}
function costCard(step, t0, t1, at) {
  var x = 790, y = 130, items = ["serialize", "disk I/O", "network I/O", "deserialize"];
  card(x, y, 260, 210, t0, t1, at);
  text("SHUFFLE COST", x + 16, y + 30, 15, MUTED, t0, t1, { font: MONO_R, tracking: 150 });
  shapeLayer("cost-track", x + 16, y + 44, t0, t1, function (r) { rect(r, 0, 0, 228, 10, null, [0.53, 0.53, 0.53], 1.5); });
  var bar = shapeLayer("cost-bar", x + 16, y + 44, t0, t1, function (r) { rect(r, 0, 0, 228 * step / 4, 10, RED); });
  var s = tg(bar).property("ADBE Scale"); s.setValueAtTime(at, [step > 1 ? (step - 1) / step * 100 : 0, 100]); s.setValueAtTime(at + 0.6, [100, 100]);
  for (var i = 0; i < 4; i++) {
    var on = i < step;
    shapeLayer("chk", x + 16, y + 74 + i * 34, t0, t1, function (r) { rect(r, 0, 0, 18, 18, on ? RED : null, [0.53, 0.53, 0.53], 1.5); });
    var t = text(items[i], x + 46, y + 90 + i * 34, 20, on ? INK : [0.75, 0.73, 0.68], t0, t1, { font: MONO_R });
  }
}
function networkArc(t0, t1, at) {
  var l = shapeLayer("arc", 0, 0, t0, t1, function (root) {
    var p = path(root, [[230, 640], [910, 610]], false, null, INK, 3, [[0, 0], [260, -300]], [[260, -300], [0, 0]]);
    p.stroke.property("ADBE Vector Stroke Line Cap").setValue(2);
    dash(p.stroke, 12, 8);
    if (at !== null) drawOn(p.group, at, 1.2);
  });
  enter(text("NETWORK", 545, 345, 18, MUTED, t0, t1, { center: true, font: MONO_R, tracking: 200 }), "fade", at === null ? t0 : at);
}
function bits(x, y, t0, t1, at) {
  terminal(x, y, 360, 44, t0, t1, at);
  enter(text("01001001 01001110 00110100", x + 18, y + 29, 19, [0.9, 0.9, 0.9], t0, t1, { font: MONO_R }), "fade", at);
}

// ================= SCENES =================
var S;

// 0-4  executors count + isometric field
paper(0, DUR);
S = [0, 4];
counter(24, 100, W / 2, 220, 150, INK, S[0], S[1], 0, 2.2, { center: true, font: "JetBrainsMono-ExtraBold" });
text("SPARK EXECUTORS", W / 2, 270, 24, MUTED, S[0], S[1], { center: true, font: MONO_R, tracking: 250 });
function isoField(t0, t1, at, red) {
  for (var r = 0; r < 10; r++) {
    var row = shapeLayer("row" + r, 0, 0, t0, t1, function (root) {
      for (var c = 0; c < 10; c++) {
        var cx = 540 + (c - r) * 46, cy = 420 + (c + r) * 25;
        path(root, [[cx, cy - 18], [cx + 40, cy], [cx + 40, cy + 12], [cx, cy + 30], [cx - 40, cy + 12], [cx - 40, cy]], true, TOPC, EDGE, 1.5);
        if (red && (r * 10 + c) % 3) path(root, [[cx, cy - 6], [cx + 13, cy], [cx, cy + 6], [cx - 13, cy]], true, RED);
      }
    });
    if (at !== null) enter(row, "drop", at + r * 0.12, 0.4);
  }
}
isoField(S[0], S[1], 0.2, false);

// 4-7  groupBy hits the field; job timer
S = [4, 7];
text("100", W / 2, 220, 150, INK, S[0], S[1], { center: true, font: "JetBrainsMono-ExtraBold" });
text("SPARK EXECUTORS", W / 2, 270, 24, MUTED, S[0], S[1], { center: true, font: MONO_R, tracking: 250 });
isoField(S[0], S[1], null, true);
terminal(130, 300, 460, 66, S[0], S[1], 4);
typewriter('df.groupBy("country")', 155, 344, 32, [0.93, 0.93, 0.93], S[0], S[1], 4.1, 0.8, { font: MONO_R });
card(240, 290, 500, 150, S[0], S[1], 5);
text("SPARK JOB · STAGE 1 OF 2", 262, 322, 15, MUTED, S[0], S[1], { font: MONO_R, tracking: 150 });
text("groupBy · count", 262, 370, 36, INK, S[0], S[1]);
text("00:", 610, 370, 36, GREEN, S[0], S[1]);
counter(10, 35, 668, 370, 36, GREEN, S[0], S[1], 5.1, 1.8, { pad: true });
shapeLayer("jobtrack", 262, 395, S[0], S[1], function (r) { rect(r, 0, 0, 456, 20, TOPC, EDGE, 1.5, 4); });
var jb = shapeLayer("jobbar", 262, 395, S[0], S[1], function (r) { rect(r, 0, 0, 320, 20, GREEN, null, 0, 4); });
tg(jb).property("ADBE Scale").setValueAtTime(5.2, [0, 100]); tg(jb).property("ADBE Scale").setValueAtTime(7, [100, 100]);

// 8-14  one billion orders, four executors
S = [8, 14];
counter(985661228, 1000000000, W / 2, 210, 96, INK, S[0], S[1], 8, 1.6, { center: true, font: "JetBrainsMono-ExtraBold" });
text("ORDERS", W / 2, 250, 22, MUTED, S[0], S[1], { center: true, font: MONO_R, tracking: 250 });
for (var i = 0; i < 4; i++) box("ex" + i, 40 + i * 250, 560, 200, 110, "EXECUTOR " + (i + 1), S[0], S[1], 9.8 + i * 0.12, { bar: true });
enter(text("+ 96 more executors", 1040, 470, 22, MUTED, S[0], S[1], { right: true, font: MONO_R }), "fade", 11.5);

// 14-20 the line of code, executors fill with rows
S = [14, 20];
terminal(40, 130, 1000, 76, S[0], S[1], 14);
text(">>>", 64, 180, 32, [0.55, 0.55, 0.55], S[0], S[1], { font: MONO_R });
typewriter('df.groupBy("country").count()', 150, 180, 32, [0.93, 0.93, 0.93], S[0], S[1], 14.6, 3.4, { font: MONO_R });
for (i = 0; i < 4; i++) box("ex" + i, 40 + i * 250, 560, 200, 110, "EXECUTOR " + (i + 1), S[0], S[1], null,
  { bar: true, cells: [0, 0, 0, 0, 0, 0] });

// 20-25 same key -> same partition
var KEYS = ["IN", "US", "BR", "DE"];
S = [20, 25];
headline("same key → same partition", 200, S[0], S[1], 20);
for (i = 0; i < 4; i++) {
  var cs = []; for (var j = 0; j < 6; j++) cs.push(KEYS[(i + j) % 4] === "IN" ? 1 : 0);
  box("ex" + i, 40 + i * 250, 330, 200, 110, "EXECUTOR " + (i + 1), S[0], S[1], null, { cells: cs });
  box("pt" + i, 40 + i * 250, 640, 200, 80, "PARTITION · " + KEYS[i], S[0], S[1], 20.4 + i * 0.15, { dot: false });
}
shapeLayer("keyarcs", 0, 0, S[0], S[1], function (root) {
  for (var k = 0; k < 4; k++) {
    var x0 = 100 + k * 250, p = path(root, [[x0, 330], [130, 640]], false, null, BLUE, 2.5, [[0, 0], [-60, -200]], [[(30 - x0) / 2, -260], [0, 0]]);
    dash(p.stroke, 6, 6); drawOn(p.group, 21.2 + k * 0.35, 1);
  }
});

// 25-29 data moves between executors
S = [25, 29];
headline("data moves between executors", 200, S[0], S[1], 25);
for (i = 0; i < 4; i++) {
  box("ex" + i, 40 + i * 250, 330, 200, 110, "EXECUTOR " + (i + 1), S[0], S[1], null, { cells: [0, 0, 0, 0, 0, 0] });
  box("pt" + i, 40 + i * 250, 640, 200, 80, "PARTITION · " + KEYS[i], S[0], S[1], null, { dot: false });
  for (j = 0; j < 6; j++) {
    var cl = shapeLayer("row", 54 + i * 250 + (j % 3) * 50, 652 + Math.floor(j / 3) * 30, S[0], S[1], function (r) {
      rect(r, 0, 0, 40, 24, i === 0 ? BLUE : WHITE, i === 0 ? BLUE : EDGE, 1.5);
    });
    var p = tg(cl).property("ADBE Position"), end = p.value, at = 25.4 + j * 0.25 + i * 0.12;
    p.setValueAtTime(at, [54 + ((i + j) % 4) * 250 + (j % 3) * 50, 345 + Math.floor(j / 3) * 30]); p.setValueAtTime(at + 0.9, end);
    tg(cl).property("ADBE Opacity").setValueAtTime(at - 0.15, 0); tg(cl).property("ADBE Opacity").setValueAtTime(at, 100);
    easeAll(cl);
  }
}

// 32-46 the six steps of a shuffle, one record at a time
function stage2(t0, t1, labelRight) {
  box("exA", 60, 600, 200, 120, "EXECUTOR 2", t0, t1, null);
  box("exB", 780, 540, 200, 120, labelRight || "EXECUTOR 4", t0, t1, null);
}
S = [32, 33];
card(260, 130, 460, 190, S[0], S[1], 32);
text("ORDER RECORD", 284, 165, 15, MUTED, S[0], S[1], { font: MONO_R, tracking: 150 });
text("order_id  48213907\rcountry   IN\ramount    ₹1,240", 284, 215, 28, INK, S[0], S[1], { font: MONO_R });
stage2(S[0], S[1]);

S = [33, 34]; stepHead(1, "serialize", S[0], S[1]); costCard(1, S[0], S[1], 33.1);
stage2(S[0], S[1]); bits(60, 480, S[0], S[1], 33.3); disk(540, 760, S[0], S[1], 33.4);

S = [34, 36]; stepHead(2, "write to disk", S[0], S[1]); costCard(2, S[0], S[1], 34.1);
stage2(S[0], S[1]); disk(540, 760, S[0], S[1], null);
var wr = shapeLayer("write", 455, 700, S[0], S[1], function (r) { rect(r, 0, 0, 170, 16, [0.74, 0.72, 0.66]); });
enter(wr, "drop", 34.5, 0.6);

S = [36, 41]; stepHead(3, "send over network", S[0], S[1]); costCard(3, S[0], S[1], 36.1);
stage2(S[0], S[1]); disk(540, 760, S[0], S[1], null); networkArc(S[0], S[1], 36.4); bits(700, 420, S[0], S[1], 37.4);
enter(text("● network I/O", 545, 480, 20, RED, S[0], S[1], { center: true, font: MONO_R }), "pop", 38.2);

S = [41, 43]; stepHead(4, "fetch", S[0], S[1]); costCard(3, S[0], S[1], 41);
stage2(S[0], S[1], "STAGE 2 · EXEC"); disk(540, 760, S[0], S[1], null); networkArc(S[0], S[1], null); bits(700, 420, S[0], S[1], 41);

S = [43, 44]; stepHead(5, "deserialize", S[0], S[1]); costCard(4, S[0], S[1], 43.1);
stage2(S[0], S[1], "STAGE 2 · EXEC"); disk(540, 760, S[0], S[1], null); networkArc(S[0], S[1], null);

S = [44, 46]; stepHead(6, "aggregate", S[0], S[1]); costCard(4, S[0], S[1], 44);
stage2(S[0], S[1], "STAGE 2 · EXEC"); disk(540, 760, S[0], S[1], null);
card(330, 360, 380, 110, S[0], S[1], 44.3);
counter(0, 2003311, 520, 440, 60, INK, S[0], S[1], 44.4, 1.2, { center: true, font: "JetBrainsMono-ExtraBold" });

// 46-54 shuffle overhead: stage 2 waits for data
S = [46, 54];
enter(text("shuffle", 470, 170, 46, INK, S[0], S[1], { right: true }), "up", 46);
enter(text(" overhead", 470, 170, 46, RED, S[0], S[1]), "up", 46.2);
box("st1", 80, 240, 170, 95, "STAGE 1", S[0], S[1], null, { w: 170 });
box("st2", 820, 225, 170, 95, "STAGE 2", S[0], S[1], null);
var costs = ["serialize", "disk I/O", "network", "deser."];
for (i = 0; i < 4; i++) {
  var hh = 75 + i * 15, blk = shapeLayer("cost" + i, 290 + i * 125, 335 - hh, S[0], S[1], function (r) { rect(r, 0, 0, 110, hh, RED, [0.42, 0.125, 0.086], 2); });
  enter(blk, "drop", 46.3 + i * 0.3);
  enter(text(costs[i], 298 + i * 125, 360 - hh, 14, WHITE, S[0], S[1], { font: MONO_R }), "drop", 46.3 + i * 0.3);
}
for (i = 1; i <= 3; i++) {
  var yy = 380 + i * 135, w0 = 0.55 - i * 0.07, at = 49 + i * 0.3;
  enter(text("STAGE 2 · TASK " + i, 40, yy - 14, 17, MUTED, S[0], S[1], { font: MONO_R, tracking: 150 }), "up", at);
  shapeLayer("track", 40, yy, S[0], S[1], function (r) { rect(r, 0, 0, 1000, 44, TOPC, EDGE, 1.5, 4); });
  var wt = shapeLayer("wait", 40, yy, S[0], S[1], function (r) { rect(r, 0, 0, 1000 * w0, 44, SHADE, null, 0, 4); });
  tg(wt).property("ADBE Scale").setValueAtTime(at, [0, 100]); tg(wt).property("ADBE Scale").setValueAtTime(at + 2, [100, 100]);
  enter(text("waiting for data…", 56, yy + 29, 18, MUTED, S[0], S[1], { font: MONO_R }), "fade", at);
  var dn = shapeLayer("done", 40 + 1000 * w0, yy, S[0], S[1], function (r) { rect(r, 0, 0, 80 + i * 20, 44, GREEN, null, 0, 4); });
  tg(dn).property("ADBE Scale").setValueAtTime(52 + i * 0.4, [0, 100]); tg(dn).property("ADBE Scale").setValueAtTime(53 + i * 0.4, [100, 100]);
}
enter(text("⌛", 1000, 470, 60, INK, S[0], S[1], { center: true }), "pop", 50);

// 56-63 less data flows; more executors
function funnel(root, y, h) {
  path(root, [[20, y], [330, y], [500, y + h * 0.4], [580, y + h * 0.4], [750, y], [1060, y], [1060, y + h], [750, y + h], [580, y + h * 0.6], [500, y + h * 0.6], [330, y + h], [20, y + h]],
    true, [0.933, 0.918, 0.878], INK, 3,
    [[0,0],[0,0],[-90,0],[0,0],[-90,0],[0,0],[0,0],[0,0],[90,0],[0,0],[90,0],[0,0]],
    [[0,0],[90,0],[0,0],[90,0],[0,0],[0,0],[0,0],[-90,0],[0,0],[-90,0],[0,0],[0,0]]);
}
S = [56, 63];
shapeLayer("pipe", 0, 0, S[0], S[1], function (r) { funnel(r, 220, 260); });
for (i = 0; i < 10; i++) {
  var rr = shapeLayer("row", 60 + (i % 5) * 46, 250 + Math.floor(i / 5) * 32, S[0], S[1], function (r) { rect(r, 0, 0, 40, 24, i % 3 ? WHITE : BLUE, i % 3 ? EDGE : BLUE, 1.5); });
  enter(rr, "pop", 56.2 + i * 0.08, 0.3);
  var rp = tg(rr).property("ADBE Position"), b0 = rp.value;
  rp.setValueAtTime(59, b0); rp.setValueAtTime(61, [b0[0] + 820, b0[1] + 20]);
}
enter(text("– less data", 40, 190, 40, GREEN, S[0], S[1]), "up", 58.5);
enter(text("✓ flows", 1040, 190, 26, GREEN, S[0], S[1], { right: true }), "pop", 59.5);
enter(text("+ more executors", 40, 590, 40, RED, S[0], S[1]), "up", 60.5);
var p2 = shapeLayer("pipe2", 0, 0, S[0], S[1], function (r) { funnel(r, 620, 220); });
enter(p2, "fade", 60.5);
for (i = 0; i < 20; i++) enter(shapeLayer("row", 50 + (i % 10) * 42, 650 + Math.floor(i / 10) * 30, S[0], S[1],
  function (r) { rect(r, 0, 0, 36, 22, i % 2 ? BLUE : WHITE, i % 2 ? BLUE : EDGE, 1.5); }), "pop", 61 + i * 0.04, 0.3);

// 63-67 filter rows + columns
S = [63, 67];
enter(text("filter", 40, 190, 46, INK, S[0], S[1]), "up", 63.5);
enter(text("rows", 230, 190, 46, GREEN, S[0], S[1]), "up", 64.2);
enter(text("+ columns", 380, 190, 46, INK, S[0], S[1]), "up", 65);
for (var r0 = 0; r0 < 6; r0++) for (var c0 = 0; c0 < 6; c0++) {
  var ox = 200 + c0 * 105 - r0 * 30, oy = 300 + r0 * 58 + c0 * 14 * -1;
  var cell = shapeLayer("tbl", ox, oy, S[0], S[1], function (r) {
    path(r, [[0, 0], [105, -14], [75, 44], [-30, 58]], true, TOPC, [0.6, 0.57, 0.5], 1.5);
  });
  enter(cell, "pop", 63 + (r0 * 6 + c0) * 0.02, 0.3);
  if (r0 % 2 || c0 > 3) {
    var gray = shapeLayer("cut", ox, oy, S[0], S[1], function (r) { path(r, [[0, 0], [105, -14], [75, 44], [-30, 58]], true, SHADE); });
    enter(gray, "fade", c0 > 3 ? 65.2 : 64.4);
  }
}

// 67-70 avoid unnecessary shuffles
S = [67, 70];
enter(text("AND", 40, 150, 20, MUTED, S[0], S[1], { font: MONO_R, tracking: 200 }), "fade", 67);
enter(text("avoid unnecessary shuffles", 40, 205, 44, RED, S[0], S[1]), "up", 67.1);
card(290, 280, 500, 320, S[0], S[1], 67.5);
enter(text("⤨", 540, 470, 150, INK, S[0], S[1], { center: true }), "pop", 67.7);
text("extra shuffle", 540, 560, 34, INK, S[0], S[1], { center: true });
var strike = shapeLayer("strike", 0, 0, S[0], S[1], function (r) {
  var p = path(r, [[270, 590], [810, 300]], false, null, RED, 9); drawOn(p.group, 68.3, 0.5);
});

// 70-75 a powerful trick…
S = [70, 75];
enter(text("a powerful trick…", 40, 200, 46, INK, S[0], S[1]), "up", 70);
var spots = [[90, 360], [320, 290], [580, 250], [820, 330], [200, 600], [470, 540], [750, 610]];
for (i = 0; i < spots.length; i++) box("trick" + i, spots[i][0], spots[i][1], 150, 85, "", S[0], S[1], 70.3 + i * 0.12, { dot: false });

// 78-80.4 end card
S = [78, DUR];
enter(text("DATA / 03", W / 2, 300, 22, MUTED, S[0], S[1], { center: true, font: MONO_R, tracking: 250 }), "fade", 78);
enter(text("Spark Performance", W / 2, 390, 72, INK, S[0], S[1], { center: true }), "up", 78.2);
enter(text("Episode 3 · Shuffling", W / 2, 450, 30, MUTED, S[0], S[1], { center: true, font: MONO_R }), "fade", 78.5);
for (i = 0; i < 4; i++) {
  var pl = shapeLayer("pill", 300 + i * 130, 530, S[0], S[1], function (r) { rect(r, 0, 0, 100, 50, i ? BOXC : BLUE, i ? INK : BLUE, 2, 6); });
  enter(pl, "up", 78.8 + i * 0.08);
  enter(text(KEYS[i], 350 + i * 130, 564, 24, i ? INK : WHITE, S[0], S[1], { center: true }), "up", 78.8 + i * 0.08);
}
enter(text("BinaryHabit.", W / 2, 840, 48, INK, S[0], S[1], { center: true }), "up", 79.3);

comp.openInViewer && comp.openInViewer();
writeLn("layers: " + comp.numLayers);
