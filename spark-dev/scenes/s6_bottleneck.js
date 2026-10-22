/* S6 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s6(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S6 54.5–63: bottleneck */
  if(t>54.3&&t<63.3){const o=W(t,54.5,63.1);let g='';
    const y1=276-E(t,58.8,59.4)*80;
    g+=`<path d="${pipePath(60,y1,420,92)}" fill="${C.card}" stroke="${C.ink}" stroke-width="1.2"/>`;
    if(t<58.8){for(let i=0;i<40;i++){const a=54.8+i*.04,x=66+(i%8)*16+cl(t-55,0,3)*(i%8>5?6:1.5),y=y1+10+Math.floor(i/8)*15;
        g+=R(Math.min(x,186+(i%3)),y,13,9,{rx:1.5,fill:i%3?C.blue:C.card,stroke:C.blue,sw:.6,op:cl((t-a)/.2)})}
      g+=`<rect x="250" y="${r1(y1+40)}" width="40" height="12" rx="6" fill="${C.red}" opacity="${r1((.2+.3*Math.abs(Math.sin(t*4)))*E(t,55.6,56))}"/>`;
      g+=pill(270,y1-14,'bottleneck',C.red,{op:E(t,55.6,56)})}
    else{for(let i=0;i<6;i++){const p=((t-58.8)*.17+i*.17)%1,x=66+p*400,neck=Math.abs(p-.5)<.12;g+=R(x,neck?y1+40:y1+14+(i%3)*22,13,9,{rx:1.5,fill:C.blue})}
      g+=T(66,y1-12,'− less data',{size:11,w:700,fill:C.green})+pill(440,y1-14,'✓ flows',C.green,{op:E(t,60.4,60.7)});
      const y2=420;g+=`<g opacity="${r1(E(t,60.8,61.2))}">`+T(66,y2-12,'+ more executors',{size:11,w:700,fill:C.red})+`<path d="${pipePath(60,y2,420,92)}" fill="${C.card}" stroke="${C.ink}" stroke-width="1.2"/>`;
      for(let i=0;i<56;i++)g+=R(66+(i%8)*16+(i%8>5?20:0),y2+8+Math.floor(i/8)*11,13,8,{rx:1.5,fill:i%3?C.blue:C.card,stroke:C.blue,sw:.6});
      g+=pill(440,y2-14,'✗ still stuck',C.red,{op:E(t,61.8,62.1)})+'</g>'}
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
