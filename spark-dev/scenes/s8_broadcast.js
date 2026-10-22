/* S8 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s8(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S8 70–77.5: next trick */
  if(t>70.2&&t<77.7){const o=W(t,70.4,77.5);let g='';const next=t>=74.6;
    g+=T(63,142,next?'NEXT EPISODE':'LESS DATA MOVEMENT',{size:7,fill:C.muted,ls:1});
    g+=T(63,170,next?'broadcast join':'a powerful trick…',{size:19,w:700});
    const B=[[44,360],[292,282],[130,446],[384,350]];
    B.forEach((b,i)=>{const a=70.5+i*.2;g+=box(b[0],b[1],82,52,48,{op:cl((t-a)/.2),dy:-(1-bk(t,a))*24,label:'EXEC '+(i+1),dot:C.green})});
    if(t<73.4)g+=box(226,370,58,10,26,{op:cl((t-71.4)/.2),dy:-(1-bk(t,71.4,.5))*60,front:C.blueL,top:'#dfe5f2'});
    B.forEach((b,i)=>{const a=73.2+i*.18,p=E(t,a,a+.8);if(t>a)g+=box(226+(b[0]+12-226)*p,370+(b[1]-14-370)*p,46,8,20,{front:C.blueL,top:'#dfe5f2'})});
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
