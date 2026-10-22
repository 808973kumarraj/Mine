/* S4 — shuffle cost (source 31.55–46.9 s). Only this file is yours to edit. */
(function(){
const s4K={ink:'#1f1c14',mut:'#8d877b',faint:'#d2cdbf',red:'#a23c26',redD:'#6e2717',blue:'#2f4b8e',
  card:'#ece9dc',edge:'#2a271f',strip:'#1e1a10',stripTx:'#e6e2d3',top:'#ece9dc',front:'#e1dbc9',side:'#e4ddcb',
  line:'#b9b2a2',pillBg:'#f1ebe2',green:'#22b04a',blueDot:'#4a5fae'};
const s4F={title:FONT.title,body:FONT.body};
const s4lerp=(a,b,p)=>a+(b-a)*p;
const s4eo=x=>1-Math.pow(1-cl(x),3);
const s4ei=x=>Math.pow(cl(x),3);
/* piecewise-linear keyframe track: [[t,v0,v1,...],...] */
function s4track(K,t){if(t<=K[0][0])return K[0].slice(1);if(t>=K[K.length-1][0])return K[K.length-1].slice(1);
  for(let i=0;i<K.length-1;i++){const a=K[i],b=K[i+1];if(t>=a[0]&&t<=b[0]){const p=(t-a[0])/(b[0]-a[0]);return a.slice(1).map((v,j)=>s4lerp(v,b[j+1],p))}}}
const s4pts=a=>a.map(p=>r1(p[0])+','+r1(p[1])).join(' ');
function s4poly(a,fill,stroke,sw,extra=''){return `<polygon points="${s4pts(a)}" fill="${fill}" stroke="${stroke||'none'}" stroke-width="${sw||1}" stroke-linejoin="round"${extra}/>`}

/* --- executor boxes (fixed measured geometry) --- */
function s4exec(top,front,side,lab,ang,dot,dotCol,t){
  let g=`<g filter="url(#s4shB)">`;
  if(side)g+=s4poly(side,s4K.side,s4K.edge,1);
  g+=s4poly(top,s4K.top,s4K.edge,1)+s4poly(front,s4K.front,s4K.edge,1.1)+'</g>';
  /* inner bevel line on top face */
  const c=[0,1,2,3].map(i=>top[i]),cx=(c[0][0]+c[1][0]+c[2][0]+c[3][0])/4,cy=(c[0][1]+c[1][1]+c[2][1]+c[3][1])/4;
  g+=s4poly(c.map(p=>[cx+(p[0]-cx)*.94,cy+(p[1]-cy)*.86]),'none','#c9c3b4',.6);
  if(side)g+=`<line x1="${side[3][0]}" y1="${side[3][1]}" x2="${side[0][0]}" y2="${side[0][1]}" stroke="${s4K.edge}" stroke-width="1.1" stroke-dasharray="3 2"/>`;
  const [lx,ly]=lab[0];
  g+=`<g transform="translate(${lx} ${ly}) rotate(${ang})">`+T(0,0,lab[1],{size:8.6,w:700,font:s4F.title,ls:.9,fill:s4K.ink});
  for(let i=0;i<3;i++)g+=`<line x1="2" y1="${7+i*7}" x2="85" y2="${7+i*7}" stroke="${s4K.line}" stroke-width=".7"/>`;
  g+='</g>';
  g+=`<circle cx="${dot[0]}" cy="${dot[1]}" r="3.6" fill="${dotCol}" opacity=".45"/><circle cx="${dot[0]}" cy="${dot[1]}" r="2.4" fill="${dotCol}"/>`;
  return g}

/* --- ORDER RECORD card mapped onto a quad (TL,TR,BR,BL); base size 298x99 --- */
function s4card(q,op,bw=298){const W0=bw,H0=99;
  const bil=(u,v)=>{const a=q[0],b=q[1],c=q[2],d=q[3];const x=(1-v)*((1-u)*a[0]+u*b[0])+v*((1-u)*d[0]+u*c[0]);const y=(1-v)*((1-u)*a[1]+u*b[1])+v*((1-u)*d[1]+u*c[1]);return [x,y]};
  let g=`<g opacity="${r1(op)}">`;
  g+=s4poly(q,s4K.card,s4K.edge,1.1,' filter="url(#s4sh)"');
  const txt=(px,py,s,o)=>{const u=px/W0,v=py/H0,p=bil(u,v),e=.02;const pu=bil(u+e,v),pv=bil(u,v+e);
    const a=(pu[0]-p[0])/(e*W0),b=(pu[1]-p[1])/(e*W0),c=(pv[0]-p[0])/(e*H0),d=(pv[1]-p[1])/(e*H0);
    return `<g transform="matrix(${a.toFixed(4)} ${b.toFixed(4)} ${c.toFixed(4)} ${d.toFixed(4)} ${r1(p[0])} ${r1(p[1])})">`+T(0,0,s,o)+'</g>'};
  g+=txt(14,21,'ORDER RECORD',{size:9,ls:1.1,fill:s4K.mut});
  [['order_id','48213907',s4K.ink],['country','IN',s4K.blue],['amount','₹1,240',s4K.ink]].forEach((r,i)=>{
    g+=txt(14,41+i*20.7,r[0],{size:13,fill:s4K.mut})+txt(88.5,41+i*20.7,r[1],{size:13,fill:r[2]})});
  return g+'</g>'}
const s4rect=(x,y,w,h)=>[[x,y],[x+w,y],[x+w,y+h],[x,y+h]];

/* --- binary strip, centre (cx,cy), scale sc (base 240x27), sx extra x-scale --- */
function s4strip(cx,cy,sc,op,sx=1){if(op<=.001||sc<=0)return '';const w=240*sc*sx,h=27*sc,x=cx-w/2,y=cy-h/2;
  return `<g opacity="${r1(op)}"><rect x="${r1(x+1.5)}" y="${r1(y+3)}" width="${r1(w)}" height="${r1(h)}" fill="#000" opacity=".16" filter="url(#s4blur)"/>`+
  `<svg x="${r1(x)}" y="${r1(y)}" width="${r1(w)}" height="${r1(h)}" overflow="hidden"><rect width="100%" height="100%" fill="${s4K.strip}"/>`+
  `<g transform="scale(${sc.toFixed(4)})">`+T(7.5,19,'01001001 01001110 00110100 00111000',{size:11.4,fill:s4K.stripTx,font:s4F.body})+'</g></svg></g>'}

function s4pill(cx,cy,s,op){if(op<=.001)return '';const w=s.length*7.8+22,h=23.5;
  return `<g opacity="${r1(op)}"><rect x="${r1(cx-w/2)}" y="${r1(cy-h/2)}" width="${r1(w)}" height="${h}" rx="${h/2}" fill="${s4K.pillBg}" stroke="${s4K.red}" stroke-width="1" filter="url(#s4sh)"/>`+
  T(cx,cy+4.5,s,{size:13,fill:s4K.red,a:'middle'})+'</g>'}

function s4disk(cx,cy,dy,op){if(op<=.001)return '';const rx=56,ry=34,th=22,n=6;let g=`<g opacity="${r1(op)}" transform="translate(0 ${r1(dy)})">`;
  g+=`<ellipse cx="${cx+6}" cy="${cy+th+8}" rx="${rx+4}" ry="${ry+2}" fill="#000" opacity=".13" filter="url(#s4blur2)"/>`;
  g+=`<defs><linearGradient id="s4dg" x1="0" x2="1"><stop offset="0" stop-color="#b9b09e"/><stop offset=".55" stop-color="#8a8170"/><stop offset="1" stop-color="#5f5646"/></linearGradient>`+
     `<radialGradient id="s4dt" cx=".35" cy=".3" r=".9"><stop offset="0" stop-color="#ebe5d8"/><stop offset=".6" stop-color="#e2dbcc"/><stop offset="1" stop-color="#d3cbba"/></radialGradient></defs>`;
  for(let i=n-1;i>=0;i--){const yy=cy+th*(i+1)/n;
    g+=`<path d="M${cx-rx} ${r1(yy-th/n)} A${rx} ${ry} 0 0 0 ${cx+rx} ${r1(yy-th/n)} L${cx+rx} ${r1(yy)} A${rx} ${ry} 0 0 1 ${cx-rx} ${r1(yy)} Z" fill="url(#s4dg)" stroke="#3a342a" stroke-width=".8"/>`;
    g+=`<path d="M${cx-rx} ${r1(yy-th/n)} A${rx} ${ry} 0 0 0 ${cx+rx} ${r1(yy-th/n)}" fill="none" stroke="#ece6da" stroke-width=".9" opacity=".8"/>`}
  g+=`<ellipse cx="${cx}" cy="${cy}" rx="${rx}" ry="${ry}" fill="url(#s4dt)" stroke="#3a342a" stroke-width=".9"/>`;
  g+=`<ellipse cx="${cx}" cy="${cy}" rx="11.5" ry="7" fill="${s4K.strip}"/>`;
  return g+'</g>'}

const s4STEPS=[[32.45,'serialize'],[33.88,'write to disk'],[36.48,'send over network'],[42.0,'fetch'],[44.3,'deserialize'],[45.2,'aggregate']];
const s4CHK=[32.93,35.62,39.13,44.83];
const s4BAR=[32.85,35.57,39.1,44.8];
/* strip keyframes: t, cx, cy, scale, opacity, sx */
const s4SK=[
 [32.72,270,234.5,1,1,0],[32.8,270,234.5,1,1,.45],[32.9,270,234.5,1,1,.8],[33.0,270,234.5,1,1,.93],[33.12,270,234.5,1,1,1],
 [33.2,260.5,239,1,1,1],[33.25,231.5,252,1,1,1],[33.3,205.5,264,1,1,1],[33.35,160,290,1,1,1],[33.4,134,298.5,1,1,1],[33.47,115,307.5,1,1,1],
 [33.9,115,307.5,1,1,1],[33.95,115,298.5,1,1,1],[34.0,115,292.5,1,1,1],[34.07,118,286.5,1,1,1],
 [34.15,160,299.5,.89,1,1],[34.2,182,312,.83,1,1],[34.25,232.5,344,.7,.95,1],[34.3,250,357.5,.65,.85,1],[34.35,272.5,378.5,.59,.9,1],[34.4,278,387,.58,.8,1],[34.5,284,405,.57,.35,1],[34.6,285,414,.57,.25,1],
 [36.25,285,414,.57,.25,1],[36.5,286,375,.68,.4,1],[36.7,286,361,.71,.75,1],
 [36.75,134,335.5,.71,1,1],[36.85,135,334.5,.71,1,1],[36.9,135,333.5,.71,1,1],[36.95,138,330.5,.71,1,1],[37.0,141,328.5,.71,1,1],[37.05,148,320.5,.71,1,1],
 [37.1,153,316.5,.71,1,1],[37.15,167,303.5,.71,1,1],[37.2,175,296.5,.71,1,1],[37.25,197,281.5,.71,1,1],[37.3,210,273.5,.71,1,1],[37.35,241,261.5,.71,1,1],
 [37.4,260,257.5,.71,1,1],[37.45,296,258.5,.71,1,1],[37.5,311,261.5,.71,1,1],[37.55,337,271.5,.71,1,1],[37.6,348,277.5,.71,1,1],[37.65,364,288.5,.71,1,1],
 [37.7,370,293.5,.71,1,1],[37.75,380,301.5,.71,1,1],[37.8,383,305.5,.71,1,1],[37.85,388,309.5,.71,1,1],[37.9,389,310.5,.71,1,1],[37.95,391,312.5,.71,1,1],
 [42.27,392,313.5,.71,1,1],[42.35,395,316,.64,1,1],[42.4,397.5,318.5,.62,1,1],[42.45,410,329,.56,.95,1],[42.5,417,336,.53,.85,1],[42.55,426,344,.48,.6,1],[42.62,432,350,.44,0,1],
 [43.8,427,277.5,.85,0,1],[44.0,427,267.5,.85,1,1],[44.42,427,267.5,.85,1,1],[44.6,427,267.5,.85,0,1]];

SCENES.push(function s4(t){
  if(t<31.55||t>=46.9)return '';
  const fo=1-io((t-46.58)/.3);
  let g=`<defs><filter id="s4sh" x="-20%" y="-30%" width="140%" height="170%"><feDropShadow dx="1" dy="3" stdDeviation="3" flood-color="#3a3020" flood-opacity=".22"/></filter>`+
    `<filter id="s4shB" x="-30%" y="-30%" width="170%" height="170%"><feDropShadow dx="5" dy="7" stdDeviation="5" flood-color="#3a3020" flood-opacity=".2"/></filter>`+
    `<filter id="s4blur"><feGaussianBlur stdDeviation="2.5"/></filter><filter id="s4blur2" x="-30%" y="-50%" width="160%" height="200%"><feGaussianBlur stdDeviation="5"/></filter>`+
    `<clipPath id="s4clip"><rect x="33" y="0" width="474" height="960"/></clipPath></defs>`;

  /* step label + title */
  let si=-1;s4STEPS.forEach((s,i)=>{if(t>=s[0])si=i});
  if(si>=0){const a=s4STEPS[si][0],p=si===0?cl((t-a+.03)/.27):io((t-a)/.3),op=si===0?p:.3+.7*p;
    g+=T(64,144.5,`STEP ${si+1} / 6`,{size:11,ls:.7,fill:s4K.mut,op:r1(op)});
    g+=T(66,174+(1-p)*2,s4STEPS[si][1],{size:24,w:700,font:s4F.title,fill:s4K.ink,op:r1(op)})}

  /* stage group slowly rotates clockwise 38.5-45.5 */
  const rot=1.6*io((t-38.5)/7);
  g+=`<g transform="rotate(${rot.toFixed(3)} 270 300)">`;
  /* executors */
  const ph=t2=>.5+.5*Math.cos(2*Math.PI*(t2-32.95)/1.25);
  const mix=(c1,c2,p)=>{const h=c=>[1,3,5].map(i=>parseInt(c.slice(i,i+2),16));const A=h(c1),B=h(c2);return '#'+A.map((v,i)=>Math.round(v+(B[i]-v)*p).toString(16).padStart(2,'0')).join('')};
  g+=s4exec([[51.5,346.5],[157.5,338],[163.5,380],[52.5,390]],[[52.5,390],[163.5,380],[167,432.5],[59,442]],null,
    [[62,403.5],'EXECUTOR 2'],-5.1,[152.5,390.5],mix('#c9c4b6',s4K.green,.12+.18*ph(t)),t);
  const st2=t>=41.6;
  g+=s4exec([[363.5,320.5],[462.5,313],[484,353],[381,362]],[[381,362],[484,353],[477,402.5],[377,411.5]],[[363.5,320.5],[381,362],[377,411.5],[360,370]],
    [[387,373.5],st2?'STAGE 2 \u00b7 EXEC 4':'EXECUTOR 4'],-5,[472.5,361],st2?s4K.blueDot:mix('#c9c4b6',s4K.green,.12+.18*ph(t+.3)),t);

  /* disk + ghost strip behind */
  const sk=s4track(s4SK,t);
  const ghost=t>=34.45&&t<36.72;
  if(ghost)g+=`<g clip-path="url(#s4clip)">`+s4strip(sk[0],sk[1],sk[2],sk[3],sk[4])+'</g>';
  if(t>=33.27){const p=cl((t-33.27)/.55);g+=s4disk(284,440,(1-s4eo(p))*50,cl(p*1.6));
    g+=T(294.5,515,'DISK',{size:11,ls:1.4,fill:s4K.mut,a:'middle',op:r1(cl((t-33.4)/.2))})}
  g+=s4pill(295,535,'+ disk I/O',cl((t-35.4)/.25)*(1-cl((t-37.65)/.3)));

  /* network arc */
  if(t>=36.0){const p=cl((t-36.0)/.42),d='M136 349 Q272 200 394 332.5',L=330;
    const da=`stroke-dasharray="${r1(L*p)} ${L}"`;
    g+=`<path d="${d}" fill="none" stroke="${s4K.edge}" stroke-width="2.3" stroke-linecap="round" ${da}/>`;
    g+=`<g mask="url(#s4am)"><path d="${d}" fill="none" stroke="#f1ede2" stroke-width=".9" stroke-dasharray="3 3"/></g>`;
    g+=`<mask id="s4am"><path d="${d}" fill="none" stroke="#fff" stroke-width="3" ${da}/></mask>`;
    g+=T(270,256.5,'NETWORK',{size:11,ls:1.3,fill:s4K.mut,a:'middle',op:r1(cl((t-37.65)/.25))})}
  g+=s4pill(270,282,'+ network I/O',cl((t-38.9)/.25)*(1-cl((t-41.05)/.4)));
  g+='</g>';

  /* ORDER RECORD card (big) 31.55–32.95 */
  if(t<32.97){const K=[[32.45,121,178,419,178,419,277,121,277,1],[32.55,122.5,177.5,396.5,183.5,396.5,271,122.5,279,1],[32.7,139,175,367,188.5,367,267.5,139,280,1],
      [32.85,182,175,328,192.5,328,262.5,182,280,1],[32.97,232,180,270,200,270,250,232,276,0]];
    const v=s4track(K,t);g+=s4card([[v[0],v[1]],[v[2],v[3]],[v[4],v[5]],[v[6],v[7]]],v[8])}

  /* strip (main) */
  if(t>=32.72&&!ghost&&t<44.6)g+=`<g clip-path="url(#s4clip)">`+s4strip(sk[0],sk[1],sk[2],sk[3],sk[4])+'</g>';

  /* small record card: 44.45 appears at right, 45.25–45.6 moves up-left */
  if(t>=44.38){const fx=s4eo((t-44.38)/.17),m=io((t-45.26)/.42);
    const x=s4lerp(345,202,m),y=s4lerp(229,169,m),w=s4lerp(168,136,m),h=s4lerp(61,49.5,m),cx=x+w/2;
    const ww=w*fx;g+=`<g clip-path="url(#s4clip)">`+s4card(s4rect(cx-ww/2,y,ww,h),1-cl((t-46.15)/.2),272)+'</g>'}

  /* COUNT card */
  if(t>=45.3){const p=cl((t-45.3)/.32),sc=.97+.03*s4eo(p)+.065*Math.sin(Math.PI*cl((t-46.12)/.5));
    const n=t<46.198?1:Math.round(2.45e6*(1-Math.pow(1-cl((t-46.198)/.62),3)))||1;
    g+=`<g opacity="${r1(p)}" transform="translate(270 277.5) scale(${sc.toFixed(3)}) translate(-270 -277.5)">`+
      `<rect x="165" y="236" width="210" height="83" rx="5" fill="${s4K.card}" stroke="${s4K.edge}" stroke-width="1.1" filter="url(#s4sh)"/>`+
      T(270,256.5,`COUNT · COUNTRY = <tspan fill="${s4K.blue}">IN</tspan>`,{size:10,ls:1.2,fill:s4K.mut,a:'middle',raw:true})+
      T(270,297.5,n.toLocaleString('en-US'),{size:33,w:700,font:s4F.title,fill:s4K.ink,a:'middle'})+'</g>'}

  /* SHUFFLE COST panel (slides in 32.62) */
  if(t>=32.6){const off=(1-s4eo((t-32.63)/.28))*190;
    let pg=`<g transform="translate(${r1(off)} 0)"><rect x="351" y="129.5" width="131.5" height="112.5" rx="5" fill="${s4K.card}" stroke="${s4K.edge}" stroke-width="1.1" filter="url(#s4sh)"/>`;
    pg+=T(361.5,147.5,'SHUFFLE COST',{size:8,ls:1,fill:s4K.mut});
    let fill=0;s4BAR.forEach(b=>fill+=s4eo((t-b)/.12)*.25);
    pg+=`<rect x="361.5" y="155" width="110.5" height="7.5" rx="1.5" fill="#e3e0d3" stroke="${s4K.edge}" stroke-width=".8"/>`;
    if(fill>0)pg+=`<rect x="362" y="155.5" width="${r1(109.5*fill)}" height="6.5" rx="1" fill="${s4K.red}"/>`;
    ['serialize','disk I/O','network I/O','deserialize'].forEach((s,i)=>{const p=cl((t-s4CHK[i])/.1),y=174+i*17.5;
      pg+=`<rect x="361.5" y="${r1(y-5.75)}" width="11.5" height="11.5" rx="1.5" fill="${s4K.red}" fill-opacity="${r1(p)}" stroke="${p>0?s4K.redD:s4K.faint}" stroke-width="1"/>`;
      pg+=T(378.5,y+3.6,s,{size:10.5,fill:p>.5?s4K.ink:s4K.faint})});
    g+=pg+'</g>'}

  return `<g opacity="${r1(fo)}">${g}</g>`;
});
})();
