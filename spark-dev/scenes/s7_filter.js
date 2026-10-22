/* S7 — filter rows + columns, then avoid unnecessary shuffles (63.0–70.4 s) */
function s7_hom(q){ /* projective map unit square -> quad q=[TL,TR,BR,BL] */
  const [p0,p1,p2,p3]=q,sx=p0[0]-p1[0]+p2[0]-p3[0],sy=p0[1]-p1[1]+p2[1]-p3[1];
  let g=0,h=0;const dx1=p1[0]-p2[0],dx2=p3[0]-p2[0],dy1=p1[1]-p2[1],dy2=p3[1]-p2[1];
  if(Math.abs(sx)>1e-6||Math.abs(sy)>1e-6){const d=dx1*dy2-dx2*dy1;g=(sx*dy2-dx2*sy)/d;h=(dx1*sy-sx*dy1)/d}
  const a=p1[0]-p0[0]+g*p1[0],b=p3[0]-p0[0]+h*p3[0],c=p0[0],d=p1[1]-p0[1]+g*p1[1],e=p3[1]-p0[1]+h*p3[1],f=p0[1];
  return (u,v)=>{const w=g*u+h*v+1;return [(a*u+b*v+c)/w,(d*u+e*v+f)/w]}}
const s7_lerpQ=(A,B,k)=>A.map((p,i)=>[p[0]+(B[i][0]-p[0])*k,p[1]+(B[i][1]-p[1])*k]);
const s7_pts=a=>a.map(p=>r1(p[0])+','+r1(p[1])).join(' ');
const s7_pl=(t,A)=>{if(t<=A[0][0])return A[0][1];for(let i=1;i<A.length;i++)if(t<=A[i][0]){const k=(t-A[i-1][0])/(A[i][0]-A[i-1][0]);return A[i-1][1]+(A[i][1]-A[i-1][1])*k}return A[A.length-1][1]};
const s7_eo=(t,a,b,n=2)=>1-Math.pow(1-cl((t-a)/(b-a)),n);
/* grid 6 cols x 8 rows (row 0 = header) */
const S7_Q1=[[90,316],[392.5,273.5],[473,421],[118,474.5]];          /* top faces at 63.5 */
const S7_Q2=[[98,355],[424,319],[446,465],[124.7,532]];               /* compact camera (65.65) */
const S7_SH2=[[88,322],[455,284],[468,414],[100,472]];                /* ground shadow stage 2 */
const S7_COLS=['order_id','country','amount','notes','browser','ts'];
const S7_LEN=[.55,.7,.45,.75,.6,.5,.8,.4,.65,.7,.5,.6,.75,.45,.55,.7,.6,.8,.5,.65,.7,.45,.6,.75];
SCENES.push(function s7(t){
  if(t<63.03||t>70.32)return '';
  const out=1-cl((t-70.1)/.24);
  let s='',tile='';
  /* ---------------- part 1: filter rows + columns ---------------- */
  if(t<67.3){
    let g='';
    const tin=cl((t-63.03)/.25),cam=(x=>x*x*(3-2*x))(cl((t-65.35)/.33)),Q=s7_lerpQ(S7_Q1,S7_Q2,cam),M0=s7_hom(Q);
    const M=(u,v)=>M0(u/6,v/8);
    /* ground shadow */
    const shop=(1-E(t,66.15,66.6))*tin;
    if(shop>0){const S1=[M(-.25,-.3),M(6.25,-.3),M(6.25,8.35),M(-.25,8.35)];const SQ=s7_lerpQ(S1,S7_SH2,cam);
      g+=`<polygon points="${s7_pts(SQ)}" fill="#a29e8f" opacity="${r1(shop*(.62+.1*cam)*100)/100}" filter="url(#s7blur)"/>`}
    /* table group */
    const gx=s7_pl(t,[[65.92,0],[66.0,17],[66.1,107],[66.2,222],[66.3,300],[66.4,365],[66.5,420]]),gy=s7_pl(t,[[65.92,0],[66.0,-3],[66.1,-38],[66.2,-68],[66.3,-78],[66.4,-60]]),gop=tin*(t<66.5?1:0);
    /* plate under table (visible in gaps once rows drop) */
    const plo=cl((t-64.38)/.1)*gop;
    if(plo>0&&gx<1){const PQ=[M(.02,.98),M(5.98,.98),M(5.98,8),M(.02,8)];g+=`<polygon points="${s7_pts(PQ)}" fill="#bcbaaf" opacity="${r1(plo*(1-cam)*100)/100}" filter="url(#s7pl)"/>`}
    if(gop>0){
      const sc=1+.06*(1-io(tin));
      g+=`<g opacity="${r1(gop*100)/100}" transform="translate(${r1(gx)} ${r1(gy)}) translate(280 390) scale(${r1(sc*1000)/1000}) translate(-280 -390)">`;
      const keep={0:0,3:1,7:2},drops=[1,2,4,5,6];
      const tiles=[];
      for(let r=0;r<8;r++){
        let vv=r,dy=0,op=1;
        if(r in keep){vv=r+(keep[r]-r)*cam}
        else{const i=drops.indexOf(r),a=[64.4,64.42,64.6,64.61,64.7][i],k=cl((t-a)/.1);if(k>=1)continue;dy=40*k;op=1-k*k}
        for(let c=0;c<6;c++){
          let uu=c,cop=op;
          if(c===3){cop*=1-cl((t-65.6)/.08);}
          if(c>=4){const k=cl((t-65.62)/.16);uu=c+2.6*k;cop*=1-k*k}
          if(cop<=0.01)continue;
          tiles.push({r,c,vv,uu,dy,cop})}}
      tiles.sort((a,b)=>a.vv-b.vv||a.uu-b.uu);
      const gp=.045,th=3.6;
      for(const z of tiles){const {r,c,vv,uu,dy,cop}=z;
        const A=M(uu+gp,vv+gp),B=M(uu+1-gp,vv+gp),Cc=M(uu+1-gp,vv+1-gp),D=M(uu+gp,vv+1-gp);
        [A,B,Cc,D].forEach(p=>p[1]+=dy);
        const hd=r===0;
        let q=`<g opacity="${r1(cop*100)/100}">`;
        q+=`<polygon points="${s7_pts([D,Cc,[Cc[0],Cc[1]+th],[D[0],D[1]+th]])}" fill="#d6d2c4" stroke="#6f6b60" stroke-width=".5" stroke-linejoin="round"/>`;
        q+=`<polygon points="${s7_pts([A,B,Cc,D])}" fill="${hd?'#dee5f1':'#eeeadd'}" stroke="#6f6b60" stroke-width=".55" stroke-linejoin="round"/>`;
        const ang=Math.atan2(B[1]-A[1],B[0]-A[0])*180/Math.PI;
        const cx=(A[0]+B[0]+Cc[0]+D[0])/4,cy=(A[1]+B[1]+Cc[1]+D[1])/4,w=Math.hypot(B[0]-A[0],B[1]-A[1]);
        if(hd)q+=`<text x="${r1(cx)}" y="${r1(cy+2.2)}" transform="rotate(${r1(ang)} ${r1(cx)} ${r1(cy)})" font-size="6.2" font-weight="700" fill="#4a5d82" text-anchor="middle" font-family="${FONT.body}">${S7_COLS[c]}</text>`;
        else{const L=S7_LEN[(r*6+c)%S7_LEN.length]*w*.72;q+=`<line x1="${r1(cx-w*.36)}" y1="${r1(cy)}" x2="${r1(cx-w*.36+L)}" y2="${r1(cy)}" transform="rotate(${r1(ang)} ${r1(cx)} ${r1(cy)})" stroke="#bcb8ab" stroke-width="2" stroke-linecap="round"/>`}
        g+=q+'</g>'}
      g+='</g>'}
    /* SHUFFLE tile */
    if(t>65.66){const k=s7_eo(t,65.66,65.98,2.5),ko=cl((t-67.03)/.27);const x=560-175*k+200*ko*ko;
      tile=`<g transform="translate(${r1(x-385)} 0)">`+R(387,294,96,87,{rx:5,fill:'#000',op:.08})+R(385,288,96,87,{rx:5,fill:'#edeadd',stroke:C.ink,sw:1.2})
        +shuffleIcon(433,317,1.1,C.ink)+T(433,351,'SHUFFLE',{size:9,fill:C.muted,a:'middle',ls:.6})
        +T(433,363.5,'light √',{size:9,fill:'#9a9a8c',a:'middle',op:cl((t-66.45)/.2)})+'</g>'}
    /* titles */
    const fo=1-cl((t-67.0)/.28),ti=cl((t-64.12)/.3)*fo;
    g+=T(64,145,'BEFORE THE SHUFFLE',{size:10.5,fill:C.muted,ls:.8,op:ti});
    const o1=cl((t-65.24)/.22),o2=cl((t-65.6)/.25);
    g+=T(64,175,`filter<tspan opacity="${r1(o1*100)/100}" fill="${C.green}"> rows</tspan><tspan opacity="${r1(o2*100)/100}"> + <tspan fill="${C.green}">columns</tspan></tspan>`,{size:23.7,w:700,raw:true,op:ti,font:FONT.title});
    s+=g}
  /* ---------------- part 2: avoid unnecessary shuffles ---------------- */
  if(t>66.8){
    let g='';const ci=cl((t-66.82)/.5),ti=cl((t-67.25)/.32);
    g+=T(64,145,'AND',{size:10.5,fill:C.muted,ls:.8,op:ti});
    g+=T(64,175,'avoid unnecessary shuffles',{size:23.7,w:700,fill:'#9f3e2a',op:ti,font:FONT.title});
    const fl0=s7_pl(t,[[69.3,0],[69.35,.08],[69.4,.26],[69.5,.7],[69.6,.95],[69.7,1]]),th=1.326*fl0,fl=1-Math.cos(th)/1,sn=Math.sin(th)/Math.sin(1.326);
    /* card corners: rect -> flat trapezoid */
    const R0=[[138,218],[402,218],[402,393],[138,393]],R1=[[174,350],[363.5,350],[387.5,392.5],[152.5,392.5]];
    const ty=393-175*Math.cos(th),ti2=37.25*Math.pow(sn,1.6),bi=14.5*sn,q=[[138+ti2,ty],[402-ti2,ty],[402-bi,393-.5*sn],[138+bi,393-.5*sn]];
    const sc=1;
    g+=`<g opacity="${r1(ci*ci*(3-2*ci)*100)/100}" filter="${ci<.98?'url(#s7cb)':''}" transform="translate(0 ${r1(38*Math.pow(1-ci,2))}) translate(270 305) scale(${r1(sc*1000)/1000}) translate(-270 -305)">`;
    g+=`<polygon points="${s7_pts(q.map(p=>[p[0],p[1]+10]))}" fill="#000" opacity="${r1((.28-.06*fl)*100)/100}" filter="url(#s7blur2)"/>`;
    g+=`<polygon points="${s7_pts(q)}" fill="#edeadd" stroke="#4a463a" stroke-width="1" stroke-linejoin="round"/>`;
    /* content: approximate projective with affine about card centre line */
    const Mc=s7_hom(q),cm=(u,v)=>Mc(u,v);
    const ic=cm(.5,.396),tx=cm(.5,.795),wsc=(cm(1,.4)[0]-cm(0,.4)[0])/264,ysc=(q[3][1]-q[0][1])/175;
    g+=`<g transform="translate(${r1(ic[0])} ${r1(ic[1])}) scale(${r1(wsc*1000)/1000} ${r1(ysc*1000)/1000})">`+shuffleIcon(0,0,2.3,C.ink)+'</g>';
    g+=`<g transform="translate(${r1(tx[0])} ${r1(tx[1])}) scale(${r1(wsc*1000)/1000} ${r1(ysc*1000)/1000})">`+T(0,0,'extra shuffle',{size:19.5,w:700,a:'middle',font:FONT.title})+'</g>';
    /* red strike */
    const sk=cl((t-67.33)/.42),sk2=1-Math.pow(1-sk,2);
    if(t>67.08){const a=cm(.096,.854),b=cm(.903,.15);const e=[a[0]+(b[0]-a[0])*sk2,a[1]+(b[1]-a[1])*sk2];
      g+=`<line x1="${r1(a[0])}" y1="${r1(a[1])}" x2="${r1(e[0])}" y2="${r1(e[1])}" stroke="#9f3e2a" stroke-width="${r1(6*(1-.55*fl)*10)/10}" stroke-linecap="round"/>`}
    g+='</g>';
    /* result pill */
    const po=cl((t-69.38)/.3);
    if(po>0)g+=`<g opacity="${r1(po*100)/100}" filter="${po<.95?'url(#s7pb)':''}" transform="translate(270 470) scale(${r1((1+.28*(1-io(po)))*1000)/1000}) translate(-270 -470)">`+R(118.5,456,303,28,{rx:14,fill:'#ebe9dc',stroke:'#7d9887',sw:1.2})+T(270,475,'√ less data moved · faster job',{size:15.5,w:500,fill:'#3d7a5c',a:'middle',font:FONT.body})+'</g>';
    s+=g}
  const po0=cl((t-69.38)/.3);const defs=`<defs><filter id="s7blur" x="-30%" y="-30%" width="160%" height="160%"><feGaussianBlur stdDeviation="11"/></filter><filter id="s7pb" x="-20%" y="-50%" width="140%" height="200%"><feGaussianBlur stdDeviation="${r1(3*(1-po0))}"/></filter><filter id="s7cb" x="-20%" y="-30%" width="140%" height="160%"><feGaussianBlur stdDeviation="${r1(4*(1-cl((t-66.82)/.36)))}"/></filter><filter id="s7pl" x="-10%" y="-10%" width="120%" height="120%"><feGaussianBlur stdDeviation="1.5"/></filter><filter id="s7blur2" x="-30%" y="-30%" width="160%" height="160%"><feGaussianBlur stdDeviation="7"/></filter></defs>`;
  return defs+`<clipPath id="s7clip"><rect x="32" y="0" width="476" height="960"/></clipPath><g opacity="${r1(out*100)/100}" clip-path="url(#s7clip)">${s}${tile}</g>`;
});
