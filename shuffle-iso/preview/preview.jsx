// EffectCraft script: the recorded frames of the HTML animation + the FilmCraft mix, as one comp to render previews from.
// Run: effectcraft-cli script preview.jsx --empty --save-as preview.ecproj   (frames in FRAMES, mix in ../audio/mix.m4a)
var FRAMES = "/tmp/claude-0/-home-user-Mine/3c235e19-b9c8-5e53-b032-8c65f2fd4111/scratchpad/frames24/f00000.png", MIX = "/home/user/Mine/shuffle-iso/audio/mix.m4a";
var io = new ImportOptions(new File(FRAMES)); io.sequence = true;
var seq = app.project.importFile(io);
try { seq.mainSource.conformFrameRate = 24; } catch (e) {}
var mix = app.project.importFile(new ImportOptions(new File(MIX)));
var comp = app.project.items.addComp("Preview", 540, 550, 1, 80.4, 24);
comp.layers.add(mix);
comp.layers.add(seq);
writeLn(seq.name + " " + seq.duration + "s, " + mix.name + " " + mix.duration + "s");
