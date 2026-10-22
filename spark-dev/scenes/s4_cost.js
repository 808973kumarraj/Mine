/* S4 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s4(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S4 29.1–47: shuffle cost */
  if(t>28.9&&t<47.3){const o=W(t,29.1,47.1);let g='';
    const ST=[[29.4,'serialize'],[33.6,'write to disk'],[35.8,'send over network'],[40.2,'fetch'],[42.8,'deserialize'],[45.2,'aggregate']];
    let st=0;ST.forEach((x,i)=>{if(t>=x[0])st=i});
    g+=T(63,142,`STEP ${st+1} / 6`,{size:7,fill:C.muted,ls:1});
    g+=T(63,170,ST[st][1],{size:19,w:700,op:cl((t-ST[st][0])/.3)});
    const items=['serialize','disk I/O','network I/O','deserialize','aggregate'],done=[29.6,34,36.2,43,45.4];
    g+=R(354,124,126,118,{rx:3,stroke:C.edge,sw:.8})+T(364,140,'SHUFFLE COST',{size:6,fill:C.muted,ls:1});
    g+=R(364,147,106,6,{rx:1,fill:'none',stroke:C.edge,sw:.6})+R(364,147,106*done.filter(d=>t>=d).length/5,6,{rx:1,fill:C.red});
    items.forEach((it,i)=>{const on=t>=done[i];g+=R(364,162+i*15,7,7,{rx:1,fill:on?C.red:'none',stroke:on?C.red:C.edge,sw:.7})+T(377,169+i*15,it,{size:7.5,fill:on?C.ink:C.muted})});
    g+=box(48,370,104,56,40,{label:'EXECUTOR 4',dot:C.green});
    g+=box(356,350,104,56,40,{label:t>40?'STAGE 2 · EXEC 2':'EXECUTOR 2',dot:C.green});
    const dk=bk(t,33.6,.5),dop=cl((t-33.6)/.2);
    g+=`<g opacity="${r1(dop)}" transform="translate(270 ${r1(470+(1-dk)*20)})"><ellipse cx="0" cy="10" rx="54" ry="18" fill="${C.side}" stroke="${C.edge}" stroke-width=".8"/><rect x="-54" y="0" width="108" height="10" fill="${C.side}"/><ellipse cx="0" cy="0" rx="54" ry="18" fill="${C.card}" stroke="${C.edge}" stroke-width=".8"/><ellipse cx="0" cy="0" rx="9" ry="3.4" fill="${C.ink}"/>${t>=34&&t<35.8?`<ellipse cx="0" cy="0" rx="58" ry="21" fill="none" stroke="${C.red}" stroke-width="1" opacity="${r1(.5+.5*Math.sin(t*6))}"/>`:''}</g>`;
    g+=T(270,512,'DISK',{size:7,fill:C.muted,a:'middle',ls:1.2,op:dop});
    const nop=E(t,35.8,36.2);
    g+=`<path d="M112 360 Q270 230 420 340" fill="none" stroke="${C.ink}" stroke-width="1.2" stroke-dasharray="4 4" opacity="${r1(nop)}"/>`;
    g+=T(270,262,'NETWORK',{size:7,fill:C.muted,a:'middle',ls:1.2,op:nop})+pill(270,282,'+ network I/O',C.red,{op:E(t,36.6,36.9)});
    if(t>=34)g+=pill(270,540,'+ disk I/O',C.red,{op:E(t,34.2,34.5)});
    let rx=60,ry=300;const bits='01001001 01001110 00110100 00111000';
    if(t>=33.6&&t<35.8){const p=E(t,33.6,34.8);rx=60+154*p;ry=300+108*p}
    if(t>=35.8&&t<40.2){const p=E(t,36.4,39.8),x0=214,y0=408,x1=380,y1=282;rx=(1-p)**2*x0+2*(1-p)*p*270+p*p*x1;ry=(1-p)**2*y0+2*(1-p)*p*200+p*p*y1}
    if(t>=40.2){rx=372;ry=282}
    if(t>=30.6&&t<42.8){const sh=t<32?bits.slice(0,Math.ceil(bits.length*E(t,30.6,31.8))):bits;g+=R(rx,ry,170,22,{rx:2,fill:'#1e1e1c'})+T(rx+8,ry+15,sh,{size:7.5,fill:'#e9e6d8'})}
    else{const agg=t>=45.2;g+=`<g transform="translate(${r1(rx)} ${r1(ry)}) scale(${r1(agg?1+.05*Math.sin(cl((t-45.2)/.5)*Math.PI):1)})">`+R(0,0,142,agg?84:68,{rx:3,stroke:C.edge,sw:.8})
      +T(10,16,'ORDER RECORD',{size:7.5,w:700})+T(10,30,'order_id 48213907',{size:7.5})+T(10,43,'country IN',{size:7.5})+T(10,56,'amount €1,240',{size:7.5})
      +(agg?T(10,74,'→ sum(IN)  ✓',{size:7.5,w:700,fill:C.green}):'')+'</g>'}
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
