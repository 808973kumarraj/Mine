/* S3 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s3(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S3 14.2–29.1: groupBy → shuffle */
  if(t>14&&t<29.4){const o=W(t,14.2,29.2);let g='';const L='df.groupBy("country").count()',k=Math.floor(L.length*E(t,14.25,17.4));
    g+=term(63,143,412,'&gt;&gt;&gt; '+code(L.slice(0,k))+cur(blink),{op:1-E(t,18.6,18.9)});
    const ey=EXY-E(t,18.7,19.3)*48,focusIN=t<24.8;
    g+=T(270,166,focusIN?'same key → same partition':'data moves between executors',{size:16,w:700,a:'middle',op:E(t,19,19.4)});
    for(let e=0;e<4;e++){g+=box(EXX[e],ey,EW,EH,ED,{label:'EXECUTOR '+(e+1),bar:1,dot:C.green});if(t<19.4)for(let b=0;b<6;b++)g+=box(EXX[e]+6+(b%3)*25+Math.floor(b/3)*8,ey-6-Math.floor(b/3)*11,18,5,8,{op:1-E(t,19,19.4)})}
    for(let p=0;p<4;p++){const a=p?20.8+p*.18:19.2,pp=bk(t,a,.5),hl=p===0&&t>21&&t<26;
      g+=box(EXX[p],430,EW,20,ED,{op:cl((t-a)/.2),dy:(1-pp)*20,front:hl?C.blueL:C.front,top:hl?'#dfe5f2':C.card});
      g+=T(EXX[p]+6,445+(1-pp)*20,'PARTITION · '+KEYS[p],{size:6.5,fill:C.muted,ls:.5,op:cl((t-a)/.2)})}
    let arcs='',chips='';
    CHIPS.forEach((c,i)=>{const sx=EXX[c.e]+18+(c.j%2)*30+Math.floor(c.j/2)*10,sy=ey-6-Math.floor(c.j/2)*9;
      const ki=KEYS.indexOf(c.k),tx=EXX[ki]+18+(c.slot%3)*24,ty=437-Math.floor(c.slot/3)*12;
      const st=c.k==='IN'?21+i*.08:25+ki*.5+(i%4)*.12,pp=E(t,st,st+1.2),mx=(sx+tx)/2,my=Math.min(sy,ty)-90;
      const x=(1-pp)**2*sx+2*(1-pp)*pp*mx+pp*pp*tx,y=(1-pp)**2*sy+2*(1-pp)*pp*my+pp*pp*ty;
      if(t>st-.3&&t<st+3)arcs+=`<path d="M${r1(sx)} ${r1(sy)} Q${r1(mx)} ${r1(my)} ${r1(tx)} ${r1(ty)}" fill="none" stroke="${C.blue}" stroke-width="1" stroke-dasharray="3 3" opacity="${r1(.7*W(t,st-.3,st+3,.4))}"/>`;
      chips+=chip(x,y,c.k,{op:E(t,19.4+i*.03,19.7+i*.03)})});
    g+=arcs+chips+pill(270,520,'SHUFFLE',C.red,{op:E(t,27.8,28.1),size:10});
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
