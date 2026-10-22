/* S2 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s2(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S2 7.9–14.2: a billion orders */
  if(t>7.7&&t<14.5){const o=W(t,7.9,14.3);let g='';
    g+=T(270,166,Math.round(1e9*E(t,8,8.8)).toLocaleString('en-US'),{size:30,w:800,a:'middle'});
    g+=T(270,184,'ORDERS',{size:7.5,fill:C.muted,a:'middle',ls:1.4});
    g+=T(492,262,'+ 96 more executors',{size:7.5,fill:C.muted,a:'end',op:E(t,10,10.4)});
    for(let e=0;e<4;e++){const a=e===0?8.7:9.6+e*.25,p=bk(t,a,.5);
      g+=box(EXX[e],EXY,EW,EH,ED,{op:cl((t-a)/.2),dy:-(1-p)*40,label:'EXECUTOR '+(e+1),bar:E(t,10.6,13.4),dot:C.green});
      for(let b=0;b<6;b++){const ba=10.5+e*.2+b*.12,bp=bk(t,ba,.4),bx=EXX[e]+6+(b%3)*25+Math.floor(b/3)*8,by=EXY-6-Math.floor(b/3)*11;
        g+=box(bx,by,18,5,8,{op:cl((t-ba)/.15),dy:-(1-bp)*18})}}
    g+=pill(270,500,'✓ PARALLEL · NO PROBLEM',C.green,{op:E(t,12.8,13.1)});
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
