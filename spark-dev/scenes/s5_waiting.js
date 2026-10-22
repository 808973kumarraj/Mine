/* S5 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s5(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S5 47–54.5: waiting */
  if(t>46.8&&t<54.8){const o=W(t,47,54.5);let g='';
    g+=T(270,166,'shuffle <tspan fill="'+C.red+'">overhead</tspan>',{size:15,w:700,a:'middle',raw:true});
    for(let i=0;i<6;i++){const a=47.2+i*.15,mid=i>0&&i<5;g+=box(120+i*48,200,40,22,18,{op:cl((t-a)/.2),dy:-(1-bk(t,a))*14,front:mid?'#c98f84':C.front,top:mid?'#d9a59a':C.card})}
    g+=T(120,252,'STAGE 1',{size:7,fill:C.muted,ls:1,op:E(t,47.5,47.8)});
    g+=T(270,280,'stage 2 waits for all of this',{size:9,fill:C.red,a:'middle',op:E(t,48.2,48.5)});
    for(let i=0;i<4;i++){const a=49.8+i*.25,dx=(1-bk(t,a))*30,y=310+i*44;
      g+=`<g opacity="${r1(cl((t-a)/.2))}" transform="translate(${r1(dx)} 0)">`+R(62,y+3,416,32,{rx:3,fill:'#000',op:.06})+R(60,y,416,32,{rx:3,stroke:C.edge,sw:.8})
        +T(74,y+20,'STAGE 2 · TASK '+(i+1),{size:9})+T(462,y+20,'waiting for data'+'...'.slice(0,1+(Math.floor(t*3)+i)%3),{size:9,fill:C.amber,a:'end'})+'</g>'}
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
