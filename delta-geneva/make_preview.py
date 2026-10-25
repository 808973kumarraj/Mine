"""Pack the Geneva scenes into one standalone HTML preview, to approve before rendering the MP4.

usage: python3 make_preview.py ../delta-lake-preview.html
Each scene's HTML (the same files Geneva renders) plays in its own iframe; the soundtrack is the clock:
the active scene is shown and its CSS animations are seeked to the time into that scene.
Fonts, audio and subtitles are embedded, so the file opens offline.
"""
import base64, json, pathlib, sys

HERE = pathlib.Path(__file__).parent
b64 = lambda p: base64.b64encode(pathlib.Path(p).read_bytes()).decode()
meta = json.load(open(HERE / 'voice/scenes.json'))
subs = json.load(open(HERE / 'voice/narration.subtitles.json'))
fonts = {w: 'data:font/ttf;base64,' + b64(HERE / f'fonts/JetBrainsMono-{w}.ttf') for w in ('Regular', 'Bold', 'ExtraBold')}
scenes = []
for s in meta['scenes']:
    h = (HERE / f"scenes/{s['name']}.html").read_text()
    for w, uri in fonts.items(): h = h.replace(f'../fonts/JetBrainsMono-{w}.ttf', uri)
    scenes.append({**s, 'html': h})

page = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Delta Lake Preview</title>
<style>
:root{--bg:#151412;--fg:#fff}
*{box-sizing:border-box;margin:0}
body{background:var(--bg);color:var(--fg);font:13px system-ui,sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;padding:16px}
.stage{position:relative;width:min(1100px,100%,calc((100vh - 120px)*16/9));aspect-ratio:16/9;overflow:hidden;border-radius:12px;background:#ece6d6;container-type:inline-size}
.stage iframe{position:absolute;left:0;top:0;width:1920px;height:1080px;border:0;transform-origin:0 0;visibility:hidden}
#subs{position:absolute;left:11%;right:11%;bottom:5%;text-align:center;pointer-events:none}
#subs span{display:inline-block;background:rgba(30,27,23,.85);color:#fff;font:600 clamp(10px,2cqw,20px)/1.35 ui-monospace,monospace;padding:.4em .8em;border-radius:8px}
.ctrl{display:flex;gap:8px;flex-wrap:wrap;justify-content:center;align-items:center;color:#aaa}
button{background:#262421;color:#fff;border:1px solid #444;border-radius:99px;padding:7px 14px;font-weight:600;cursor:pointer}
#cc.on{background:#f8f5ee;color:#1e1b17}
input[type=range]{width:min(1100px,92vw)}
</style></head><body>
<div class="stage" id="stage"><div id="subs"></div></div>
<audio id="v" src="AUDIO_SRC" preload="auto"></audio>
<input type="range" id="seek" min="0" max="DUR" step="0.05" value="0">
<div class="ctrl"><button id="play">▶ Play</button><button id="cc" class="on">CC</button><span id="clock">0.0s</span> · <span id="scene"></span></div>
<script>
const SCENES = SCENES_JSON, SUBS = SUBS_JSON, DUR = DUR_NUM;
const stage = document.getElementById('stage'), v = document.getElementById('v'), subsEl = document.getElementById('subs');
const frames = SCENES.map(s => { const f = document.createElement('iframe'); f.srcdoc = s.html; stage.insertBefore(f, subsEl); return f; });
let subsOn = true, cur = -1;
function fit() { const k = stage.clientWidth / 1920; frames.forEach(f => f.style.transform = `scale(${k})`); }
addEventListener('resize', fit); fit();
function seekScene(i, t) {
  const doc = frames[i].contentDocument; if (!doc || !doc.getAnimations) return;
  doc.getAnimations().forEach(a => { a.pause(); a.currentTime = Math.max(0, t) * 1000; });
}
function frame() {
  const t = Math.min(v.currentTime, DUR);
  let i = SCENES.findIndex(s => t >= s.start && t < s.start + s.duration); if (i < 0) i = SCENES.length - 1;
  if (i !== cur) { frames.forEach((f, k) => f.style.visibility = k === i ? 'visible' : 'hidden'); cur = i; document.getElementById('scene').textContent = SCENES[i].name; }
  seekScene(i, t - SCENES[i].start);
  const c = SUBS.find(c => t >= c.start && t < c.end), txt = c && subsOn ? c.text : '';
  if (subsEl.dataset.t !== txt) { subsEl.dataset.t = txt; subsEl.innerHTML = txt ? '<span></span>' : ''; if (txt) subsEl.firstChild.textContent = txt; }
  document.getElementById('seek').value = t; document.getElementById('clock').textContent = t.toFixed(1) + 's';
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);
document.getElementById('seek').oninput = e => { v.currentTime = +e.target.value; };
const pb = document.getElementById('play'); pb.onclick = () => v.paused ? v.play() : v.pause();
v.onplay = () => pb.textContent = '❚❚ Pause'; v.onpause = () => pb.textContent = '▶ Play';
document.getElementById('cc').onclick = e => { subsOn = !subsOn; e.target.classList.toggle('on', subsOn); };
</script></body></html>'''
page = (page.replace('SCENES_JSON', json.dumps(scenes).replace('</', '<\\/')).replace('SUBS_JSON', json.dumps(subs))
        .replace('DUR_NUM', str(meta['duration'])).replace('max="DUR"', f'max="{meta["duration"]}"')
        .replace('AUDIO_SRC', 'data:audio/mp4;base64,' + b64(HERE / 'audio/mix.m4a')))
pathlib.Path(sys.argv[1]).write_text(page)
print(sys.argv[1], round(len(page) / (1 << 20), 1), 'MB')
