/* S8 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
/* rotated 3D box: A = back-left top corner, N = near top corner, F = front-face vector, h = height */
function s8_pt(p){return r1(p[0])+','+r1(p[1])}
function s8_poly(pts,fill,stroke,sw,extra){return `<polygon points="${pts.map(s8_pt).join(' ')}" fill="${fill}" stroke="${stroke||'none'}" stroke-width="${sw||0}" stroke-linejoin="round"${extra||''}/>`}
function s8_xbox(A,N,F,h,o={}){
  const S=[N[0]-A[0],N[1]-A[1]],add=(p,v,k=1)=>[p[0]+v[0]*k,p[1]+v[1]*k],dn=p=>[p[0]+(255-p[0])*h/2100,p[1]+h];
  const NF=add(N,F),AF=add(A,F),ed=o.edge||'#2b2720',sw=o.sw||.85;let g='';
  /* soft ground shadow */
  const gy=o.lift||0,sh=[A,AF,NF,N].map(p=>[p[0]+9,p[1]+h+gy+7]);
  g+=s8_poly(sh,'#3a3420','none',0,` opacity="${r1((o.shOp??.22)*(1-Math.min(1,gy/-160)*.4))}" filter="url(#s8blur)"`);
  g+=`<g transform="translate(0 ${r1(o.lift||0)})">`;
  g+=s8_poly([A,N,dn(N),dn(A)],o.left||'#e6dfcf',ed,sw);
  g+=s8_poly([N,NF,dn(NF),dn(N)],o.front||'#dfd8c6',ed,sw);
  g+=s8_poly([A,AF,NF,N],o.top||'#edeadd','#7a7466',sw*.8);g+=`<polyline points="${[A,N,NF].map(s8_pt).join(' ')} ${s8_pt(dn(NF))}" fill="none" stroke="${ed}" stroke-width="${r1(sw*1.2)}" stroke-linejoin="round"/>`;g+=`<line x1="${r1(N[0])}" y1="${r1(N[1])}" x2="${r1(dn(N)[0])}" y2="${r1(dn(N)[1])}" stroke="${ed}" stroke-width="${r1(sw*1.3)}"/>`;
  if(o.inset){const k=o.inset,f=.07,s2=.12,p=[add(add(A,F,f),S,s2),add(add(A,F,1-f),S,s2),add(add(A,F,1-f),S,1-s2),add(add(A,F,f),S,1-s2)];
    g+=s8_poly(p,o.insetFill||'none',o.insetStroke||'#d6d0bf',o.insetSw||.7,` opacity="${r1(k)}"`)}
  if(o.label){const L=Math.hypot(F[0],F[1]),ux=F[0]/L,uy=F[1]/L,sc=o.ls||1;
    g+=`<g transform="matrix(${r1(ux*100)/100} ${r1(uy*100)/100} 0 1 ${r1(N[0])} ${r1(N[1])})">`;
    g+=T(8*sc,h*.27+3*sc,o.label,{size:6.4*sc,w:700,ls:.6*sc,fill:'#2b2720',font:FONT.body});
    for(let i=0;i<3;i++){const y=h*.47+i*h*.14;g+=`<line x1="${r1(9*sc)}" y1="${r1(y)}" x2="${r1(L*.86)}" y2="${r1(y)}" stroke="#c9c1ac" stroke-width=".7"/>`}
    g+=`<circle cx="${r1(L-8*sc)}" cy="${r1(h*.15)}" r="${r1(2.4*sc)}" fill="#3f8a63" opacity=".85"/><circle cx="${r1(L-8*sc)}" cy="${r1(h*.15)}" r="${r1(4*sc)}" fill="#3f8a63" opacity=".18"/>`;
    g+='</g>'}
  if(o.topText){const L=Math.hypot(F[0],F[1]),ux=F[0]/L,uy=F[1]/L,Ls=Math.hypot(S[0],S[1]),vx=S[0]/Ls,vy=S[1]/Ls;
    g+=`<g transform="matrix(${r1(ux*100)/100} ${r1(uy*100)/100} ${r1(vx*100)/100} ${r1(vy*100)/100} ${r1(A[0])} ${r1(A[1])})" opacity="${r1(o.topTextOp??1)}">`;
    g+=T(10,Ls*.36,o.topText[0],{size:6,w:700,fill:'#4a463c'})+T(10,Ls*.36+8,o.topText[1],{size:5.5,fill:'#9a9584'})+'</g>'}
  return g+'</g>'}
SCENES.push(function s8(t){
  if(t<70.24||t>=77.5)return '';
  let g='<defs><filter id="s8blur" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="5"/></filter><radialGradient id="s8vg"><stop offset="0" stop-color="#fbf9f0" stop-opacity=".55"/><stop offset="1" stop-color="#fbf9f0" stop-opacity="0"/></radialGradient></defs><ellipse cx="250" cy="320" rx="230" ry="200" fill="url(#s8vg)"/>';
  const next=t>=74.45;
  /* header text */
  if(!next||t<74.8){const o=next?1-E(t,74.45,74.65):E(t,70.35,70.9);
    g+=T(65,144.5,'LESS DATA MOVEMENT',{size:10,fill:'#86826f',ls:1,op:o*(next?1:1)});
    g+=T(65,174.5,'a powerful trick…',{size:23,w:700,fill:C.ink,op:o,ls:.35})}
  if(next){const n=Math.max(0,Math.min(14,Math.floor((t-74.635)/.045)+1));
    g+=T(65,144.5,'NEXT EPISODE →',{size:10,fill:'#86826f',ls:1,op:E(t,74.45,74.65)});
    g+=T(65,183.5,'BROADCAST JOIN'.slice(0,n),{size:31,w:700,fill:'#2c4c86'})}
  /* executors drop in */
  const BX=[[[46.5,355],[76,380],[76.5,-22.5],44,70.24,'EXEC 1'],[[294,282.5],[326,309],[62.5,-20],34.5,70.33,'EXEC 2'],
            [[131,442.5],[165,477.5],[83.5,-29],45,70.535,'EXEC 3'],[[395,351.5],[434,378.5],[67.5,-23.5],41.5,70.63,'EXEC 4']];
  const drop=(a,d,H)=>{const p=cl((t-a)/d);return -(1-p)*(1-p)*H};
  const items=[];
  BX.forEach((b,i)=>{if(t<b[4])return;items.push({y:b[1][1],s:s8_xbox(b[0],b[1],b[2],b[3],{lift:i==2?drop(b[4],.16,62):drop(b[4],.2,160),label:b[5],ls:b[3]/42,inset:1,
    shOp:.22})})});
  /* table slab */
  const SA=[217.5,366],SN=[248.5,391],SF=[74,-22],tray=E(t,73.3,73.9);
  if(t>=71.12){if(tray>0)g+=`<ellipse cx='292' cy='386' rx='34' ry='16' fill='#8ea4d8' opacity='${r1(.45*tray)}' filter='url(#s8blur)'/>`;items.push({y:SN[1],s:s8_xbox(SA,SN,SF,10.5,{lift:drop(71.12,.4,190),topText:tray>.5?['','100 rows']:['countries','200 rows'],topTextOp:tray>.5?(tray-.5)*2:1-2*tray,
    inset:Math.max(.6,tray),insetFill:tray>0?'#f4f1e7':'none',insetStroke:tray>0?'#8a8576':'#d6d0bf',insetSw:.6+.3*tray,top:tray>0?'#e2ddcf':undefined,shOp:.25})})}
  /* broadcast copies: rise as a fanned stack, then fly onto each executor's top */
  if(t>=74.62){const tg=BX.map(b=>{const sc=.62,cx=b[0][0]+b[2][0]*.5+(b[1][0]-b[0][0])*.5,cy=b[0][1]+b[2][1]*.5+(b[1][1]-b[0][1])*.5;
      return [cx-(SF[0]+SN[0]-SA[0])*sc/2,cy-(SF[1]+SN[1]-SA[1])*sc/2-46]});
    [0,1,3,2].forEach((ei,k)=>{const r=E(t,74.6+k*.04,74.76+k*.04),m=E(t,74.88+k*.07,75.18+k*.07);
      const lv=k+1,sx=SA[0]-lv*3*r,sy=SA[1]-r*lv*24;
      for(let gh=2;gh>=0;gh--){const mm=gh?Math.max(0,E(t-gh*.035,74.88+k*.07,75.18+k*.07)):m;if(gh&&(m<=0||m>=1))continue;
        const arc=-Math.sin(mm*Math.PI)*30,A=[sx+(tg[ei][0]-sx)*mm,sy+(tg[ei][1]-sy)*mm+arc],sc=1-.3*mm;
        const N=[A[0]+(SN[0]-SA[0])*sc,A[1]+(SN[1]-SA[1])*sc],F=[SF[0]*sc,SF[1]*sc];
        items.push({y:(m>=1?BX[ei][1][1]+1:900+k)-gh*.1,s:`<g opacity="${r1(cl((t-74.64-k*.06)/.1)*(gh?.25/gh:.85))}">`+s8_xbox(A,N,F,4.5*sc,{top:'#dfe7f4',front:'#cbd5ea',left:'#d3dcef',edge:'#56565a',sw:.7,shOp:gh?0:.1,inset:.8,insetStroke:'#9fb0d6'})+'</g>'})}})}
  const k=.031*Math.max(0,Math.min(75.4,t)-72.8);g+=`<g transform="matrix(1 0 ${-k.toFixed(4)} 1 ${r1(k*385)} 0)">`;items.sort((a,b)=>a.y-b.y).forEach(it=>g+=it.s);g+='</g>';
  return g;
});
