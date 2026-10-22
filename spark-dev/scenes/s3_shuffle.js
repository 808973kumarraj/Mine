/* S3 — groupBy → shuffle (source 14.2–31.6 s). Perspective scene fitted to the original frames. */
(function(){
  try{if(!document.getElementById('s3font')){const l=document.createElement('link');l.id='s3font';l.rel='stylesheet';
    l.href='https://fonts.googleapis.com/css2?family=Inter:wght@800&display=swap';document.head.appendChild(l)}}catch(e){}
  /* camera: 3x4 projection fitted (least squares) to box corners measured in the original */
  const PM=[[84.31157,-2.5065,-12.09694,265.79822],[-0.37228,-37.01824,29.36285,341.47358],[0.00959,-0.01181,-0.08721,1]];
  const H=1.27805,D=.52196,S=1.17498,WP=.99408,HP=.35482,DP=.46441,SP=1.16322,ZP=1.33836,YU=H/44.5;
  const pj=(x,y,z)=>{const w=PM[2][0]*x+PM[2][1]*y+PM[2][2]*z+PM[2][3];
    return [(PM[0][0]*x+PM[0][1]*y+PM[0][2]*z+PM[0][3])/w,(PM[1][0]*x+PM[1][1]*y+PM[1][2]*z+PM[1][3])/w]};
  const K={edge:'#4a463b',exFront:'#e6e1d2',exTop:'#edeadd',exSide:'#dcd6c6',pTop:'#d7d0be',pFront:'#cdc7b2',pSide:'#bfb8a3',
    blue:'#264272',blueF:'#1b3260',white:'#f3efe3',whiteF:'#d9d3c3',dim:'#dedbcf',dimF:'#cbc7b9',hlTop:'#c9d0e2',hlFront:'#d6dae6',
    ink:'#1e1b0e',term:'#1e1a10',arc:'#2c3d7a',cur:'#c38545',line:'#cfc8b6',ul:'#3a5a9a'};
  const f2=n=>Math.round(n*100)/100;
  const poly=(pts,fill,o={})=>`<polygon points="${pts.map(p=>f2(p[0])+','+f2(p[1])).join(' ')}" fill="${fill}" stroke="${o.stroke||K.edge}" stroke-width="${o.sw??1.1}" stroke-linejoin="round"${o.op!=null?` opacity="${f2(o.op)}"`:''}/>`;
  const area=p=>{let a=0;for(let i=0;i<p.length;i++){const q=p[(i+1)%p.length];a+=p[i][0]*q[1]-q[0]*p[i][1]}return a};
  /* affine matrix mapping local px-ish coords on a face: origin a, x-axis toward b, y-axis toward c */
  function fmat(a,b,c){const lx=Math.hypot(b[0]-a[0],b[1]-a[1]),ly=Math.hypot(c[0]-a[0],c[1]-a[1]);
    return {m:`matrix(${f2((b[0]-a[0])/lx)} ${f2((b[1]-a[1])/lx)} ${f2((c[0]-a[0])/ly)} ${f2((c[1]-a[1])/ly)} ${f2(a[0])} ${f2(a[1])})`,w:lx,h:ly}}
  /* generic box: x0..x1, y0..y1, z0..z1 (z+ toward viewer). returns {s, f:{front,top}} */
  function box3(x0,x1,y0,y1,z0,z1,c,o={}){
    const v=(x,y,z)=>pj(x,y,z);
    const F={front:[v(x0,y1,z1),v(x1,y1,z1),v(x1,y0,z1),v(x0,y0,z1)],
      top:[v(x0,y1,z0),v(x1,y1,z0),v(x1,y1,z1),v(x0,y1,z1)],
      left:[v(x0,y1,z0),v(x0,y1,z1),v(x0,y0,z1),v(x0,y0,z0)],
      right:[v(x1,y1,z1),v(x1,y1,z0),v(x1,y0,z0),v(x1,y0,z1)]};
    let s='';const sw=o.sw??1.1,st=o.stroke||K.edge;
    if(area(F.left)>0)s+=poly(F.left,c.side||c.front,{sw,stroke:st});
    if(area(F.right)>0)s+=poly(F.right,c.side||c.front,{sw,stroke:st});
    s+=poly(F.top,c.top,{sw,stroke:st});
    s+=poly(F.front,c.front,{sw,stroke:st});
    return {s,F}}
  function shadow(x0,x1,z0,z1,op){const p=[pj(x0+.02,0,z0),pj(x1+.06,0,z0),pj(x1+.06,0,z1+.12),pj(x0+.02,0,z1+.12)];
    return `<polygon points="${p.map(q=>f2(q[0]+2)+','+f2(q[1]+3)).join(' ')}" fill="#3a3020" opacity="${f2(.16*op)}" filter="url(#s3blur)"/>`}
  const exX=i=>(i-1.5)*S, pX=i=>(i-1.5)*SP;
  const LAY=[['IN','US','BR','IN','DE','US'],['US','IN','DE','BR','IN','US'],['BR','DE','IN','US','BR','IN'],['IN','BR','US','DE','IN','DE']];
  const KEYS3=['IN','US','BR','DE'];
  /* tile geometry: flat slabs laid in rows (back -> front), two columns, label on the top face */
  const CW=.37,CH=5.5*YU;
  const exSlot=(e,j)=>{const col=j%2,row=Math.floor(j/2),pd=D*.96/3;
    return {x:exX(e)+(col?.205:-.205),y:H,z:-D*.53+pd*(row+.5),d:pd*.94}};
  const pSlot=(p,k)=>{const layer=k>=6?1:0,kk=k%6,col=kk%2,row=Math.floor(kk/2),pd=DP*.34;
    return {x:pX(p)+(col?.205:-.205),y:HP+layer*CH,z:ZP-DP*.44+pd*row+(layer?pd*.5:0),d:pd*.94}};
  /* flights: order exec by exec, row-major */
  const FL=[];{const cnt={IN:0,US:0,BR:0,DE:0};
    for(let e=0;e<4;e++)for(let j=0;j<6;j++){const k=LAY[e][j],st=25.1+e*.72+j*.1;FL.push({e,j,k,st,slot:cnt[k]++})}}
  function tile(x,y,z,d,fill,ff,label,lc,op,stroke,hh){if(op<=0)return '';
    const b=box3(x-CW/2+.012,x+CW/2-.012,y,y+(hh||CH),z-d/2,z+d/2,{front:ff,top:fill,side:ff},{sw:.9,stroke});
    let s=b.s;if(label){const t=b.F.top,m=fmat(t[0],t[1],t[3]);
      s+=`<text transform="${m.m}" x="${f2(m.w/2)}" y="${f2(m.h/2+2.6)}" font-size="7.4" font-weight="700" fill="${lc}" text-anchor="middle" letter-spacing=".6" font-family="${FONT.body}">${label}</text>`}
    return `<g opacity="${f2(op)}">${s}</g>`}
  /* executor front decoration */
  function exFront(F,e,bar,op){const m=fmat(F[0],F[1],F[3]);let s=`<g transform="${m.m}">`;
    s+=`<text x="8" y="12.5" font-size="6.3" font-weight="700" fill="${K.ink}" letter-spacing=".9" font-family="${FONT.body}">EXECUTOR ${e+1}</text>`;
    s+=`<circle cx="${f2(m.w-10)}" cy="11" r="2.4" fill="#4f8a6c" opacity=".85"/>`;
    if(bar>0)s+=`<g opacity="${f2(bar)}"><rect x="8" y="${f2(m.h-11)}" width="${f2(m.w-18)}" height="5" rx="1" fill="#2f6d58" stroke="${K.edge}" stroke-width=".8"/></g>`;
    if(bar<1)for(let i=0;i<3;i++)s+=`<line x1="10" x2="${f2(m.w-14)}" y1="${f2(22+i*7)}" y2="${f2(22+i*7)}" stroke="${K.line}" stroke-width=".9" opacity="${f2(1-bar)}"/>`;
    return s+'</g>'}
  function pFront(F,p,hl){const m=fmat(F[0],F[1],F[3]);
    return `<text transform="${m.m}" x="5" y="${f2(m.h-3)}" font-size="7.6" font-weight="700" fill="${K.ink}" letter-spacing="1.1" font-family="${FONT.body}">PARTITION · ${KEYS3[p]}</text>`}
  function topRim(F,dash,col){const c=[(F[0][0]+F[2][0])/2,(F[0][1]+F[2][1])/2],k=.86;
    const q=F.map(p=>[c[0]+(p[0]-c[0])*k,c[1]+(p[1]-c[1])*(k-.04)]);
    return `<polygon points="${q.map(p=>f2(p[0])+','+f2(p[1])).join(' ')}" fill="none" stroke="${col}" stroke-width=".7"${dash?' stroke-dasharray="2.2 1.8"':''}/>`}
  const bez=(p0,p1,p2,p3,u)=>{const a=1-u;return [a*a*a*p0[0]+3*a*a*u*p1[0]+3*a*u*u*p2[0]+u*u*u*p3[0],a*a*a*p0[1]+3*a*a*u*p1[1]+3*a*u*u*p2[1]+u*u*u*p3[1]]};

  SCENES.push(function s3(t){
    if(t<14.2||t>=31.6)return '';
    let g='<defs><filter id="s3blur" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="3.5"/></filter>'+
      '<filter id="s3tsh" x="-20%" y="-50%" width="140%" height="250%"><feGaussianBlur stdDeviation="4"/></filter></defs>';
    /* terminal */
    const tOp=E(t,14.0,14.45)*(1-E(t,18.8,19.2));
    if(tOp>0){const ty=-12*E(t,18.75,19.1),L='df.groupBy("country").count()',n=cl(Math.floor((t-14.87)*14),0,L.length);
      const on=(t%1)<.55;
      g+=`<g opacity="${f2(tOp)}" transform="translate(0 ${f2(ty)})">`+
        `<rect x="66" y="150" width="408" height="50" fill="#000" opacity=".35" filter="url(#s3tsh)"/>`+
        `<rect x="63" y="140" width="414" height="53" fill="${K.term}"/>`+
        `<text x="81" y="175" font-size="17.3" fill="#8a8678" font-family="${FONT.body}">&gt;&gt;&gt;</text>`+
        `<text x="122" y="175" font-size="17.3" fill="#ece8da" font-family="${FONT.body}">${esc(L.slice(0,n))}</text>`+
        (on?`<rect x="${f2(122+n*10.55+3)}" y="160" width="8" height="16" fill="${K.cur}"/>`:'')+'</g>';
      const so=.75*E(t,18.5,18.75)*(1-E(t,18.8,19.15)),ssc=.68+.32*E(t,18.5,19.0);
      if(so>0)g+=`<g transform="translate(270 ${f2(232+ty)}) scale(${f2(ssc)})"><text x="0" y="0" font-size="12.9" fill="#a5a194" opacity="${f2(so)}" text-anchor="middle" font-family="${FONT.body}">every order now has a <tspan font-weight="700" fill="#5b6080">key</tspan>: its country</text></g>`}
    /* titles */
    const t1=E(t,19.6,19.85)*(1-E(t,24.85,25.1)),t2=E(t,24.95,25.2)*(1-E(t,28.5,28.8));
    if(t1>0)g+=T(271,168,'same key → same partition',{size:22.6,w:700,a:'middle',op:f2(t1),fill:K.ink,font:FONT.title});
    if(t2>0)g+=T(271,168,'data moves between executors',{size:22.6,w:700,a:'middle',op:f2(t2),fill:K.ink,font:FONT.title});
    const sh=E(t,28.55,28.9);
    if(sh>0){const sc=1+.08*(1-sh);g+=`<g opacity="${f2(sh)}" transform="translate(270 172) rotate(${f2(-2-1.5*(1-sh))}) scale(${f2(sc)})">`+
      `<text x="0" y="12" font-size="51" font-weight="800" fill="${K.ink}" text-anchor="middle" letter-spacing="2" font-family="Inter, system-ui, sans-serif">SHUFFLE</text>`+
      `<rect x="-129" y="25" width="264" height="4.2" fill="${K.ul}"/></g>`}
    /* executor rise: 2D offsets (measured) before 18.75 */
    const up=1-E(t,18.8,19.25),off=e=>[(-5-1.5*e)*up,(58.5-2.6*e)*up];
    /* chip states */
    const morph=e=>E(t,18.15+e*.13,18.5+e*.13);
    const dimP=E(t,21.15,21.35),lift=E(t,21.15,21.4);
    /* partitions */
    const hl=E(t,22.3,22.6);
    let back='',front='',arcs='',fly='';
    for(let p=0;p<4;p++){const a=[19.36,19.5,19.64,19.82][p];if(t<a)continue;const pp=1-Math.pow(1-cl((t-a)/.3),3);
      const dy=(1-pp)*34,op=cl((t-a)/.04);
      const isH=p===0;const hh=isH?hl:0;
      const mix=(c1,c2,k)=>{const h=s=>[1,3,5].map(i=>parseInt(s.slice(i,i+2),16));const A=h(c1),B=h(c2);return '#'+A.map((v,i)=>Math.round(v+(B[i]-v)*k).toString(16).padStart(2,'0')).join('')};
      const b=box3(pX(p)-WP/2,pX(p)+WP/2,0,HP,ZP-DP/2,ZP+DP/2,{top:mix(K.pTop,K.hlTop,hh),front:mix(K.pFront,K.hlFront,hh),side:K.pSide},{stroke:mix(K.edge,'#2a3f78',hh)});
      let s=shadow(pX(p)-WP/2,pX(p)+WP/2,ZP-DP/2,ZP+DP/2,1)+b.s+topRim(b.F.top,1,mix('#8f8877','#7f8fb8',hh))+pFront(b.F.front,p);
      /* landed chips */
      FL.forEach(c=>{if(KEYS3.indexOf(c.k)!==p)return;const q=(t-c.st)/.85;if(q<1)return;const sl=pSlot(p,c.slot);
        s+=c.k==='IN'?tile(sl.x,sl.y,sl.z,sl.d,K.blue,K.blueF,'IN','#fff',1):tile(sl.x,sl.y,sl.z,sl.d,K.white,K.whiteF,c.k,'#3a3730',1)});
      front+=`<g opacity="${f2(op)}" transform="translate(0 ${f2(dy)})">${s}</g>`}
    /* executors */
    for(let e=0;e<4;e++){const o=off(e),x=exX(e);
      const b=box3(x-.5,x+.5,0,H,-D/2,D/2,{top:K.exTop,front:K.exFront,side:K.exSide});
      let s=shadow(x-.5,x+.5,-D/2,D/2,1)+b.s+exFront(b.F.front,e,1-E(t,18.75,18.95),1);
      const mo=morph(e);
      for(let j=0;j<6;j++){const k=LAY[e][j],sl=exSlot(e,j),fl=FL[e*6+j];if(t>=fl.st)continue;
        const hop=Math.sin(Math.PI*mo)*8*YU*(j%2?1:.8);
        const isIN=k==='IN';let fill,ff,lc,lab=mo>.5?k:'';
        if(mo<.5){fill=K.white;ff=K.whiteF;lc='#3a3730'}
        else if(isIN){fill=K.blue;ff=K.blueF;lc='#fff'}
        else{fill=dimP>.5?K.dim:K.white;ff=dimP>.5?K.dimF:K.whiteF;lc=dimP>.5?'#8d897c':'#3a3730'}
        const ly=isIN?lift*4*YU:0;
        s+=tile(sl.x,sl.y+hop+ly,sl.z,sl.d*(1-.6*Math.sin(Math.PI*mo)),fill,ff,lab,lc,1,null,CH*(1+3.5*Math.sin(Math.PI*mo)))}
      back+=`<g transform="translate(${f2(o[0])} ${f2(o[1])})">${s}</g>`}
    /* arcs from partition IN to IN chips */
    const ar=E(t,21.3,21.8)*(1-E(t,25.4,25.7));
    if(ar>0){const p0=pj(pX(0),HP,ZP);
      for(let e=0;e<4;e++)for(let j=0;j<6;j++){if(LAY[e][j]!=='IN')continue;const sl=exSlot(e,j),p3=pj(sl.x,sl.y+CH+4*YU,sl.z);
        const pk=p3[1]-10-Math.abs(p3[0]-p0[0])*.03;
        const p1=[p0[0]+(p3[0]-p0[0])*.05,pk],p2=[p3[0]-(p3[0]-p0[0])*.05,pk];
        const n=40;const pts=[];for(let i=0;i<=n;i++)pts.push(bez(p0,p1,p2,p3,i/40));
        arcs+=`<polyline points="${pts.map(q=>f2(q[0])+','+f2(q[1])).join(' ')}" fill="none" stroke="${K.arc}" stroke-width="1.6" stroke-dasharray="3 2.2" stroke-linecap="round" opacity="${f2(ar*.95)}"/>`}}
    /* flying chips */
    FL.forEach(c=>{const q=(t-c.st)/.85;if(q<0||q>=1)return;const u=io(q);const a=exSlot(c.e,c.j),b=pSlot(KEYS3.indexOf(c.k),c.slot);
      const o=off(c.e);const x=a.x+(b.x-a.x)*u,z=a.z+(b.z-a.z)*u,y=(a.y+4*YU*(c.k==='IN'?1:0))+(b.y-a.y)*u+80*YU*Math.sin(Math.PI*Math.pow(q,.6));
      fly+=c.k==='IN'?tile(x,y,z,a.d+(b.d-a.d)*u,K.blue,K.blueF,'IN','#fff',1):tile(x,y,z,a.d+(b.d-a.d)*u,K.white,K.whiteF,c.k,'#3a3730',1)});
    g+=back+front+arcs+fly;
    return g});
})();
