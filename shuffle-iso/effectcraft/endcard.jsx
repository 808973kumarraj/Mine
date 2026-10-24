// EffectCraft script: the end card. An isometric cube assembles face by face, then the title rises.
// Run:  effectcraft-cli script endcard.jsx --empty --save-as endcard.ecproj
// Then: effectcraft-cli exec file.exportLottie --project endcard.ecproj --params '{"comp":"EndCard","path":"endcard.json"}'
var W = 1080, H = 1100, DUR = 3;
var comp = app.project.items.addComp("EndCard", W, H, 1, DUR, 30);
var INK = [0.17, 0.153, 0.13], MUTED = [0.54, 0.52, 0.46], AMBER = [0.784, 0.47, 0.184];
var C = Math.cos(Math.PI / 6), S = Math.sin(Math.PI / 6), R = 90;   // cube edge in px

function tg(l) { return l.property("ADBE Transform Group"); }
function iso(x, y, z) { return [(x - y) * C * R, (x + y) * S * R - z * R]; }
function face(name, pts, fill, at) {
  var l = comp.layers.addShape(); l.name = name;
  tg(l).property("ADBE Position").setValue([W / 2, 360]);
  tg(l).property("ADBE Anchor Point").setValue([0, 0]);
  var g = l.property("ADBE Root Vectors Group").addProperty("ADBE Vector Group");
  var sh = new Shape(); sh.vertices = pts; sh.closed = true;
  g.property("ADBE Vectors Group").addProperty("ADBE Vector Shape - Group").property("ADBE Vector Shape").setValue(sh);
  g.property("ADBE Vectors Group").addProperty("ADBE Vector Graphic - Fill").property("ADBE Vector Fill Color").setValue(fill);
  var st = g.property("ADBE Vectors Group").addProperty("ADBE Vector Graphic - Stroke");
  st.property("ADBE Vector Stroke Color").setValue(INK); st.property("ADBE Vector Stroke Width").setValue(5);
  st.property("ADBE Vector Stroke Line Join").setValue(2);
  var p = tg(l).property("ADBE Position"), o = tg(l).property("ADBE Opacity");
  p.setValueAtTime(at, [W / 2, 300]); p.setValueAtTime(at + 0.45, [W / 2, 360]);
  o.setValueAtTime(at, 0); o.setValueAtTime(at + 0.2, 100);
  for (var k = 1; k <= p.numKeys; k++) try { p.setTemporalEaseAtKey(k, [new KeyframeEase(0, 75)], [new KeyframeEase(0, 75)]); } catch (e) {}
}
// left, right, top faces of a unit cube centred on the floor
face("left",  [iso(-.5, .5, 0), iso(.5, .5, 0), iso(.5, .5, 1), iso(-.5, .5, 1)], [0.165, 0.3, 0.61], 0.0);
face("right", [iso(.5, -.5, 0), iso(.5, .5, 0), iso(.5, .5, 1), iso(.5, -.5, 1)], [0.11, 0.22, 0.47], 0.15);
face("top",   [iso(-.5, -.5, 1), iso(.5, -.5, 1), iso(.5, .5, 1), iso(-.5, .5, 1)], [0.23, 0.385, 0.74], 0.3);

function text(str, y, size, color, font, at, tracking) {
  var l = comp.layers.addText(str);
  var src = l.property("ADBE Text Properties").property("ADBE Text Document"), td = src.value;
  td.fontSize = size; td.fillColor = color; td.font = font; td.tracking = tracking || 0;
  td.justification = ParagraphJustification.CENTER_JUSTIFY; src.setValue(td);
  var p = tg(l).property("ADBE Position"), o = tg(l).property("ADBE Opacity");
  p.setValueAtTime(at, [W / 2, y + 40]); p.setValueAtTime(at + 0.5, [W / 2, y]);
  o.setValueAtTime(at, 0); o.setValueAtTime(at + 0.35, 100);
  for (var k = 1; k <= p.numKeys; k++) try { p.setTemporalEaseAtKey(k, [new KeyframeEase(0, 75)], [new KeyframeEase(0, 75)]); } catch (e) {}
}
text("DATA / 03", 560, 24, MUTED, "JetBrainsMono-Regular", 0.6, 300);
text("Spark Performance", 650, 76, INK, "JetBrainsMono-ExtraBold", 0.75);
text("Episode 3 · Shuffling", 720, 32, MUTED, "JetBrainsMono-Regular", 1.0);
text("BinaryHabit.", 900, 48, INK, "JetBrainsMono-Bold", 1.4);
writeLn("layers " + comp.numLayers);
