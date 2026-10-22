/* S0_header — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s0_header(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  const H='BINARY HABIT / SPARK PERF / 03',n=Math.ceil(H.length*cl(t/1.1));
  s+=T(36,104,'&gt; '+esc(H.slice(0,n))+cur(blink),{size:10.5,w:700,fill:C.muted,ls:1.8,raw:true});
  s+=`<line x1="36" y1="114" x2="${r1(36+340*E(t,0,1.4))}" y2="114" stroke="${C.blue}" stroke-width="1.5"/>`;

  return s;
});
