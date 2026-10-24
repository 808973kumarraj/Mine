// usage: node audit.js  (needs Playwright; checks ../shuffle-iso-standalone.html)
// Steps through the timeline and reports visible elements that leave the frame or collide with text.
const {chromium}=require('playwright');(async()=>{const b=await chromium.launch();const p=await b.newPage({viewport:{width:1200,height:1300}});
await p.goto('file://'+require('path').resolve(__dirname,'../shuffle-iso-standalone.html'));await p.waitForTimeout(1500);
const out=await p.evaluate(async()=>{
  const panel=document.getElementById('panel');panel.style.transform='none';
  const res=[];
  for(let t=0;t<=80.4;t+=0.25){tl.time(t);
    const P=panel.getBoundingClientRect();
    const vis=e=>{for(let n=e;n&&n!==panel;n=n.parentElement){const cs=getComputedStyle(n);if(cs.visibility==='hidden'||cs.display==='none'||+cs.opacity<0.05)return false;}return true;};
    const items=[...panel.querySelectorAll('.spr,.itext,.o')].filter(vis).map(e=>{
      const r=(e.classList.contains('spr')?e.querySelector('svg'):e).getBoundingClientRect();
      return {e,cls:e.className,txt:(e.textContent||'').trim().slice(0,24),r:{l:r.left-P.left,t:r.top-P.top,R:r.right-P.left,B:r.bottom-P.top}};});
    for(const it of items){const r=it.r;if(r.R-r.l<2)continue;
      if(r.l<-2||r.R>1082||r.t<100&&it.cls.includes('spr')||r.B>1102)res.push(`${t.toFixed(2)} OUT ${it.cls} "${it.txt}" ${Math.round(r.l)},${Math.round(r.t)}-${Math.round(r.R)},${Math.round(r.B)}`);}
    // 2D text overlapping sprites
    const texts=items.filter(i=>i.cls.startsWith('o ')),sprs=items.filter(i=>i.cls.includes('spr'));
    for(const a of texts)for(const s of sprs){const A=a.r,B=s.r;const ix=Math.min(A.R,B.R)-Math.max(A.l,B.l),iy=Math.min(A.B,B.B)-Math.max(A.t,B.t);
      if(ix>12&&iy>12)res.push(`${t.toFixed(2)} HIT "${a.txt}" x sprite ${Math.round(ix)}x${Math.round(iy)}`);}
  }
  return res;});
const seen=new Map();for(const l of out){const k=l.replace(/^\S+ /,'').replace(/-?\d+,-?\d+-\d+,\d+$/,'').replace(/\d+x\d+$/,'');if(!seen.has(k))seen.set(k,[]);seen.get(k).push(l.split(' ')[0]);}
for(const [k,ts] of seen)console.log(ts[0]+'..'+ts[ts.length-1]+' ('+ts.length+')',k);
await b.close();})();
