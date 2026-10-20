// Builds edit/index.html from beats.json + measured voice durations. Every tween is keyed to a beat.
import fs from 'fs';
const beats = JSON.parse(fs.readFileSync('beats.json', 'utf8'));
const durs = fs.readFileSync('voice/durations.txt', 'utf8').trim().split('\n').map(Number);
const LEAD = 0.4, PAD = 0.5, TAIL = 1.6;
let t = LEAD;
beats.forEach((b, i) => { b.start = +t.toFixed(3); b.dur = +(durs[i] + PAD).toFixed(3); t += b.dur; });
const TOTAL = +(t + TAIL).toFixed(3);
const T = n => beats[n - 1].start, E = n => +(beats[n - 1].start + beats[n - 1].dur).toFixed(3);
fs.writeFileSync('timing.json', JSON.stringify({ total: TOTAL, beats: beats.map(b => ({ n: b.n, start: b.start, dur: b.dur })) }, null, 1));

const chapters = [];
beats.forEach(b => { let c = chapters.at(-1); if (!c || c.name !== b.ch) chapters.push(c = { name: b.ch, beats: [] }); c.beats.push(b.n); });
const cs = c => T(c.beats[0]), ce = c => c === chapters.at(-1) ? TOTAL : E(c.beats.at(-1));
const sceneOpen = (i, inner) => { const c = chapters[i]; return `<div class="clip scene" id="s${i + 1}" data-start="${cs(c)}" data-duration="${+(ce(c) - cs(c)).toFixed(3)}" data-track-index="1">${inner}</div>`; };

const box = (id, x, y, w, h, label, cls = '') => `<div class="box ${cls}" id="${id}" style="left:${x}px;top:${y}px;width:${w}px;height:${h}px"><span>${label}</span></div>`;
const execs = (pre, y) => [0, 1, 2].map(k => box(`${pre}${k}`, 300 + k * 480, y, 360, 220, `executor ${k + 1}`, 'exec') ).join('');
const words = [['the', 'cat', 'sat'], ['the', 'dog', 'sat'], ['the', 'cat', 'ran']];
const colors = ['#ff8a3d', '#4cc9f0', '#b388ff'];

const html = [];
// 1. The problem
html.push(sceneOpen(0, `
 <div class="h1 big" id="s1t">Too much data</div>
 <div id="laptop" class="laptop"><div id="file" class="file">data.txt</div></div>
 ${[0, 1, 2].map(k => box(`srv${k}`, 330 + k * 480, 560, 300, 260, `machine ${k + 1}`, 'srv')).join('')}
 ${[0, 1, 2].map(k => `<div class="piece" id="pc${k}" style="left:${380 + k * 480}px;top:650px;background:${colors[k]}">part ${k + 1}</div>`).join('')}
 <div class="logo" id="logo">Apache <b>Spark</b></div>
 <div class="chip" id="hist">UC Berkeley AMPLab 2009 &rarr; Apache top-level 2014</div>`));
// 2. Who's who
html.push(sceneOpen(1, `
 <div class="h1" id="s2t">Who's who</div>
 ${box('drv', 760, 190, 400, 200, 'driver', 'drv')}
 <div class="code small" id="drvcode">main() &middot; makes the plan</div>
 ${execs('ex', 640)}
 ${[0, 1, 2].map(k => `<div class="mem" id="mem${k}" style="left:${340 + k * 480}px;top:800px"><i></i><em>memory</em></div>`).join('')}
 ${box('cm', 80, 250, 360, 140, 'cluster manager', 'cm')}
 <div class="note" id="cmn">YARN &middot; Kubernetes &middot; Standalone</div>
 <svg class="wires" width="1920" height="1080"><g id="wires" stroke="#8b93a7" stroke-width="4" fill="none">
  ${[0, 1, 2].map(k => `<path class="wire" d="M960 390 L ${480 + k * 480} 640"/>`).join('')}
  <path class="wire" d="M440 320 L 760 290"/></g></svg>`));
// 3. Follow one job
html.push(sceneOpen(2, `
 <div class="h1" id="s3t">Follow one job: count words</div>
 <div class="line" id="rawtext">the cat sat &nbsp;/&nbsp; the dog sat &nbsp;/&nbsp; the cat ran</div>
 ${words.map((w, k) => `<div class="part" id="pt${k}" style="left:${300 + k * 480}px;border-color:${colors[k]}"><b style="color:${colors[k]}">partition ${k + 1}</b><span>${w.join(' ')}</span><small>task ${k + 1}</small></div>`).join('')}
 <div class="code" id="code"><span id="codetxt"></span></div>
 <div class="tag" id="wait">nothing runs yet</div>
 <div class="recipe" id="recipe"><b>recipe</b><ol><li id="r1">read text</li><li id="r2">split into words</li><li id="r3">pair each word with 1</li><li id="r4">add up by word</li></ol></div>
 <div class="stamp" id="stamp">collect()</div>
 <div class="tag go" id="go">go! Spark plans the whole job</div>`));
// 4. Stages and the shuffle
const grp = { the: 0, cat: 1, sat: 2, dog: 1, ran: 2 };
let chips = '', cnt = [0, 0, 0]; const gy = {};
words.forEach((w, k) => w.forEach((wd, j) => { const g = grp[wd]; const id = `w${k}${j}`; gy[id] = 640 + cnt[g] * 62; chips += `<div class="wchip" id="${id}" data-gx="${380 + g * 480}" data-gy="${640 + cnt[g]++ * 62}" style="left:${380 + k * 480}px;top:${640 + j * 62}px;border-color:${colors[k]}">${wd}</div>`; }));
html.push(sceneOpen(3, `
 <div class="h1" id="s4t">Stages and the shuffle</div>
 <div class="dag" id="dag">
  <div class="stage" id="st1"><b>stage 1</b><div class="n">read</div><div class="n">split words</div><div class="n">pair with 1</div></div>
  <div class="wall" id="wall"><span>shuffle</span></div>
  <div class="stage" id="st2"><b>stage 2</b><div class="n">add up by word</div><div class="n">collect</div></div>
 </div>
 ${[0, 1, 2].map(k => box(`sx${k}`, 300 + k * 480, 560, 360, 380, `executor ${k + 1}`, 'exec col')).join('')}
 ${chips}
 <div class="local" id="local">local work, no talking</div>
 <div class="cost" id="cost">disk &middot; network &middot; time</div>
 <div class="rule" id="rule">fewer shuffles = faster jobs</div>`));
// 5. Why it's fast
html.push(sceneOpen(4, `
 <div class="h1" id="s5t">Why it's fast</div>
 <div class="flow" id="memflow"><div class="mstep">step 1</div><div class="arrow">&rarr;</div><div class="mbox" id="mbox">memory</div><div class="arrow">&rarr;</div><div class="mstep">step 2</div><div class="arrow">&rarr;</div><div class="mstep">step 3</div></div>
 <div class="disk" id="disk">disk every step<s></s></div>
 <div class="bars" id="bars"><div class="bar"><i id="bh"></i><span>Hadoop</span></div><div class="bar"><i id="bs"></i><span>Spark</span></div><div class="big20" id="b20">up to <b id="n20">20</b>x faster</div><div class="fine" id="fine">iterative jobs, RDD paper (NSDI 2012)</div></div>
 <div class="plan" id="plan"><b>Catalyst</b><div class="pn" id="p1">read all rows</div><div class="pn" id="p2">join</div><div class="pn hl" id="p3">filter</div></div>`));
// 6. When things break
html.push(sceneOpen(5, `
 <div class="h1" id="s6t">When a machine dies</div>
 ${[0, 1, 2].map(k => box(`fx${k}`, 300 + k * 480, 280, 360, 420, `executor ${k + 1}`, 'exec')).join('')}
 ${[0, 1, 2].map(k => `<div class="piece fixed" id="fp${k}" style="left:${400 + k * 480}px;top:430px;background:${colors[k]}">part ${k + 1}</div>`).join('')}
 <div class="piece fixed" id="fpn" style="left:1360px;top:570px;background:${colors[1]}">part 2</div>
 <div class="lineage" id="lineage"><span>read</span>&rarr;<span>split words</span>&rarr;<span>pair with 1</span></div>
 <div class="note center" id="keep">no copies, just the recipe</div>`));
// 7. Wrap
html.push(sceneOpen(6, `
 <div class="h1" id="s7t">Recap</div>
 <div class="grid" id="grid">
  <div class="cell" id="g1"><b>driver</b> plans</div><div class="cell" id="g2"><b>executors</b> work</div>
  <div class="cell" id="g3"><b>shuffles</b> split stages</div><div class="cell" id="g4"><b>memory</b> keeps it fast</div></div>
 <div class="stamp final" id="fstamp">collect()</div>
 <div class="logo final" id="flogo">Apache <b>Spark</b></div>`));

// chrome: chapter chip, subtitles, progress rail
chapters.forEach((c, i) => html.push(`<div class="clip chap" data-start="${cs(c)}" data-duration="${+(ce(c) - cs(c)).toFixed(3)}" data-track-index="2"><b>${i + 1}</b> ${c.name}</div>`));
beats.forEach(b => html.push(`<div class="clip sub" data-start="${b.start}" data-duration="${b.dur}" data-track-index="3"><span>${b.text}</span></div>`));
html.push(`<div class="rail"><i id="railfill"></i></div>`);

// tweens
const tw = [];
const add = s => tw.push(s);
const typed = 'lines.flatMap(split).map(w => (w, 1)).reduceByKey(add)';
// 1
add(`tl.fromTo('#laptop',{opacity:0,scale:.9},{opacity:1,scale:1,duration:.5},${T(1)})`);
add(`tl.fromTo('#file',{width:120,height:80},{width:520,height:330,duration:${(E(1) - T(1) - .6).toFixed(2)},ease:'power1.in'},${T(1) + .3})`);
add(`tl.to('#laptop',{borderColor:'#ff5d5d',duration:.3},${E(1) - .6})`);
add(`tl.to(['#laptop','#s1t'],{opacity:0,y:-30,duration:.4},${T(2)})`);
add(`tl.fromTo('.srv',{opacity:0,y:40},{opacity:1,y:0,stagger:.12,duration:.5},${T(2) + .2})`);
[0, 1, 2].forEach(k => add(`tl.fromTo('#pc${k}',{x:${480 - k * 480},y:-350,opacity:0},{x:0,y:0,opacity:1,duration:.8,ease:'power2.inOut'},${T(2) + .6 + k * .15})`));
add(`tl.to(['.srv','.piece:not(.fixed)'],{y:140,scale:.8,opacity:.5,duration:.5},${T(3)})`);
add(`tl.fromTo('#logo',{opacity:0,scale:.85},{opacity:1,scale:1,duration:.6,ease:'back.out(1.6)'},${T(3) + .2})`);
add(`tl.fromTo('#hist',{opacity:0,y:20},{opacity:1,y:0,duration:.5},${T(3) + 2.6})`);
// 2
add(`tl.fromTo('#drv',{opacity:.25,scale:.92},{opacity:1,scale:1,duration:.5},${T(4)})`);
add(`tl.fromTo('#drvcode',{opacity:0},{opacity:1,duration:.4},${T(4) + 1.2})`);
add(`tl.fromTo('.exec',{opacity:0,y:40},{opacity:1,y:0,stagger:.15,duration:.5},${T(5)})`);
add(`tl.fromTo('.mem i',{width:0},{width:'70%',stagger:.15,duration:.8},${T(5) + 1.2})`);
add(`tl.fromTo('.mem',{opacity:0},{opacity:1,duration:.3},${T(5) + 1})`);
add(`tl.fromTo('#cm',{opacity:0,x:-40},{opacity:1,x:0,duration:.5},${T(6)})`);
add(`tl.fromTo('#cmn',{opacity:0},{opacity:1,duration:.4},${T(6) + 1})`);
add(`tl.fromTo('.wire',{strokeDashoffset:600},{strokeDashoffset:0,stagger:.15,duration:.7},${T(6) + .8})`);
// 3
add(`tl.fromTo('#rawtext',{opacity:0,y:20},{opacity:1,y:0,duration:.4},${T(7)})`);
add(`tl.to('#rawtext',{opacity:0,duration:.4},${T(7) + 2})`);
add(`tl.fromTo('.part',{opacity:0,y:-120},{opacity:1,y:0,stagger:.2,duration:.6,ease:'power2.out'},${T(7) + 2})`);
add(`tl.to('.part',{y:-80,scale:.8,opacity:.55,duration:.4},${T(8)})`);
add(`tl.fromTo('#code',{opacity:0},{opacity:1,duration:.3},${T(8)})`);
add(`tl.to('#codetxt',{text:${JSON.stringify(typed)},duration:2,ease:'none'},${T(8) + .3})`);
add(`tl.fromTo('#wait',{opacity:0},{opacity:1,duration:.3},${T(8) + 2.6})`);
add(`tl.to('#wait',{opacity:.35,repeat:3,yoyo:true,duration:.35},${T(8) + 3})`);
add(`tl.to(['#code','#wait'],{opacity:0,y:-20,duration:.4},${T(9)})`);
add(`tl.fromTo('#recipe',{opacity:0,y:30},{opacity:1,y:0,duration:.5},${T(9)})`);
add(`tl.fromTo('#recipe li',{opacity:0,x:-20},{opacity:1,x:0,stagger:.5,duration:.4},${T(9) + .6})`);
add(`tl.fromTo('#stamp',{opacity:0,scale:2.4,rotation:-12},{opacity:1,scale:1,rotation:-6,duration:.35,ease:'power3.in'},${T(10) + .3})`);
add(`tl.fromTo('#go',{opacity:0},{opacity:1,duration:.4},${T(10) + 2.4})`);
// 4
add(`tl.fromTo('#st1',{opacity:0,x:-30},{opacity:1,x:0,duration:.5},${T(11)})`);
add(`tl.fromTo('#wall',{opacity:0,scaleY:0},{opacity:1,scaleY:1,duration:.5},${T(11) + 1.6})`);
add(`tl.fromTo('#st2',{opacity:0,x:30},{opacity:1,x:0,duration:.5},${T(11) + 2.2})`);
add(`tl.to('#s4t',{opacity:0,duration:.3},${T(12)})`);
add(`tl.to('#dag',{scale:.7,y:-120,duration:.5},${T(12)})`);
add(`tl.fromTo('.col',{opacity:0},{opacity:1,duration:.4},${T(12)})`);
add(`tl.fromTo('.wchip',{opacity:0,scale:.5},{opacity:1,scale:1,stagger:.06,duration:.3},${T(12) + .5})`);
add(`tl.fromTo('#local',{opacity:0},{opacity:1,duration:.4},${T(12) + 1.6})`);
add(`tl.to('#local',{opacity:0,duration:.3},${T(13)})`);
add(`tl.to('#st1',{opacity:.4,duration:.3},${T(13)})`);
add(`tl.to('#wall',{backgroundColor:'#ff8a3d',duration:.3},${T(13)})`);
words.forEach((w, k) => w.forEach((wd, j) => add(`tl.to('#w${k}${j}',{x:${(grp[wd] - k) * 480},y:${gy[`w${k}${j}`] - (640 + j * 62)},duration:.9,ease:'power2.inOut'},${T(13) + .8 + (k * 3 + j) * .08})`)));
add(`tl.to('.col',{borderColor:'#ff5d5d',duration:.3},${T(14)})`);
add(`tl.fromTo('#cost',{opacity:0,y:20},{opacity:1,y:0,duration:.4},${T(14) + .2})`);
add(`tl.fromTo('#rule',{opacity:0},{opacity:1,duration:.4},${T(14) + 2.4})`);
// 5
add(`tl.fromTo('#memflow',{opacity:0,y:20},{opacity:1,y:0,duration:.5},${T(15)})`);
add(`tl.fromTo('#disk',{opacity:0},{opacity:1,duration:.3},${T(15) + 1.6})`);
add(`tl.fromTo('#disk s',{width:0},{width:'100%',duration:.4},${T(15) + 3})`);
add(`tl.to('#disk',{opacity:.35,duration:.3},${T(15) + 3.4})`);
add(`tl.to(['#memflow','#disk'],{opacity:0,y:-40,duration:.4},${T(16)})`);
add(`tl.fromTo('#bars',{opacity:0},{opacity:1,duration:.3},${T(16)})`);
add(`tl.fromTo('#bh',{height:0},{height:24,duration:.5},${T(16) + .4})`);
add(`tl.fromTo('#bs',{height:0},{height:480,duration:1.6,ease:'power2.out'},${T(16) + 2.4})`);
add(`tl.fromTo('#b20',{opacity:0},{opacity:1,duration:.2},${T(16) + 2.4})`);
add(`tl.fromTo('#n20',{innerText:1},{innerText:20,snap:{innerText:1},duration:1.6,ease:'power2.out'},${T(16) + 2.4})`);
add(`tl.fromTo('#fine',{opacity:0},{opacity:1,duration:.3},${T(16) + 3})`);
add(`tl.to('#bars',{opacity:0,duration:.4},${T(17)})`);
add(`tl.fromTo('#plan',{opacity:0,y:30},{opacity:1,y:0,duration:.5},${T(17)})`);
add(`tl.to('#p3',{y:-236,duration:.8,ease:'power2.inOut'},${T(17) + 3})`);
add(`tl.to(['#p1','#p2'],{y:118,duration:.8,ease:'power2.inOut'},${T(17) + 3})`);
add(`tl.to('#p1',{text:'read matching rows',duration:.5},${T(17) + 4})`);
// 6
add(`tl.fromTo('#s6 .exec, #fp0, #fp1, #fp2',{opacity:0},{opacity:1,duration:.4},${T(18)})`);
add(`tl.to('#fx1',{borderColor:'#ff5d5d',backgroundColor:'rgba(255,93,93,.25)',duration:.2,repeat:3,yoyo:true},${T(18) + 2.6})`);
add(`tl.to(['#fx1','#fp1'],{opacity:.12,duration:.4},${T(18) + 3.6})`);
add(`tl.fromTo('#keep',{opacity:0},{opacity:1,duration:.3},${T(18) + 1.8})`);
add(`tl.to('#keep',{opacity:0,duration:.3},${T(19)})`);
add(`tl.fromTo('#lineage span',{opacity:0,y:10},{opacity:1,y:0,stagger:.4,duration:.3},${T(19) + .2})`);
add(`tl.fromTo('#lineage',{opacity:0},{opacity:1,duration:.2},${T(19)})`);
add(`tl.fromTo('#fpn',{opacity:0,scale:.3},{opacity:1,scale:1,duration:.5,ease:'back.out(2)'},${T(19) + 1.8})`);
// 7
add(`tl.fromTo('.cell',{opacity:0,scale:.9},{opacity:1,scale:1,stagger:.6,duration:.4},${T(20)})`);
add(`tl.to(['#grid','#s7t'],{opacity:.2,scale:.9,duration:.5},${T(21)})`);
add(`tl.fromTo('#fstamp',{opacity:0,scale:2},{opacity:1,scale:1,duration:.35},${T(21) + .2})`);
add(`tl.to('#fstamp',{boxShadow:'0 0 60px #ff8a3d',duration:.6,yoyo:true,repeat:1},${T(21) + 1})`);
add(`tl.fromTo('#flogo',{opacity:0,y:20},{opacity:1,y:0,duration:.6},${E(21) - .4})`);
// rail
add(`tl.fromTo('#railfill',{width:0},{width:'100%',duration:${TOTAL},ease:'none'},0)`);

const css = fs.readFileSync('style.css', 'utf8');
const page = `<!doctype html>
<html lang="en"><head><meta charset="UTF-8" /><meta name="viewport" content="width=1920, height=1080" />
<script src="https://cdn.jsdelivr.net/npm/gsap@3.14.2/dist/gsap.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/gsap@3.14.2/dist/TextPlugin.min.js"></script>
<style>${css}</style></head><body>
<div id="root" data-composition-id="main" data-start="0" data-duration="${TOTAL}" data-width="1920" data-height="1080">
${html.join('\n')}
</div>
<script>
gsap.registerPlugin(TextPlugin);
const tl = gsap.timeline({ paused: true });
${tw.join(';\n')};
window.__timelines["main"] = tl;
tl.seek(0);
</script></body></html>`;
fs.writeFileSync('edit/index.html', page);
console.log('total', TOTAL, 'beats', beats.length);
