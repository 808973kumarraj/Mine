// usage: node check_frames.js  (Playwright) - steps every scene every 0.25 s and reports boxes over the title or out of frame
// Every scene every 0.25 s: report any box that overlaps the title/kicker or leaves the frame.
const {chromium}=require('playwright');(async()=>{const b=await chromium.launch();const p=await b.newPage({viewport:{width:1920,height:1080}});
const sc=JSON.parse(require('fs').readFileSync('/home/user/Mine/delta-geneva/voice/scenes.json')).scenes;const out=new Set();
for(const s of sc){await p.goto('file:///home/user/Mine/delta-geneva/scenes/'+s.name+'.html');await p.waitForTimeout(200);
 for(let t=0;t<s.duration;t+=0.25){const r=await p.evaluate(t=>{document.getAnimations().forEach(a=>{a.pause();a.currentTime=t*1000});
   const T=[...document.querySelectorAll('.title,.kicker')].map(e=>e.getBoundingClientRect()).filter(r=>r.width);
   const bad=[];document.querySelectorAll('body > .o').forEach(e=>{const cs=getComputedStyle(e);if(+cs.opacity<.1)return;
     const q=e.getBoundingClientRect();
     // visible pixels of a box: shrink its bbox a little (bboxes include padding)
     const x0=q.left+8,x1=q.right-8,y0=q.top+8,y1=q.bottom-8;
     if(x0<0||x1>1920||y1>1080)bad.push('OUT '+(e.textContent||'box').trim().slice(0,20));
     for(const r of T)if(x0<r.right&&x1>r.left&&y0<r.bottom&&y1>r.top)bad.push('TITLE '+(e.textContent||'box').trim().slice(0,20));});
   return bad;},t);
   r.forEach(x=>out.add(s.name+': '+x));}}
console.log([...out].join('\n')||'clean');await b.close();})();
