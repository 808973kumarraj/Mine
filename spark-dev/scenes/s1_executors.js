/* S1 — 0–7.9 s: 100 executors grid, ALL RUNNING, df.groupBy terminal, job card, SLOWING DOWN.
   The executor grid is a perspective-projected 10x10 lattice: its four corner cells are key-framed
   (measured from the original frames) and every box is mapped through the resulting homography. */
const s1_lerp=(a,b,x)=>a+(b-a)*x;
function s1_tab(tab,t){ /* piecewise-linear table [[t,v],...] (v number or array) */
  if(t<=tab[0][0])return tab[0][1];
  for(let i=1;i<tab.length;i++)if(t<=tab[i][0]){const a=tab[i-1],b=tab[i],x=(t-a[0])/(b[0]-a[0]);
    return Array.isArray(a[1])?a[1].map((v,k)=>Array.isArray(v)?v.map((w,j)=>s1_lerp(w,b[1][k][j],x)):s1_lerp(v,b[1][k],x)):s1_lerp(a[1],b[1],x)}
  return tab[tab.length-1][1]}
/* corners: [top(0,0), right(9,0), left(0,9), bottom(9,9)] — cell centres */
const S1_CORN=[
 [0.6,[[276,262],[482,382],[41,367],[257,506]]],
 [0.8,[[279,264.5],[503,379],[41.6,367],[255,533]]],
 [1.3,[[280.7,262.3],[507,381],[35,366],[253.3,533.8]]],
 [2.0,[[283.5,260.2],[511.8,381.8],[32.6,363.5],[248.4,536]]],
 [2.5,[[286.4,259.3],[512.5,384.2],[30.5,360.8],[244.5,535.7]]],
 [3.0,[[291,258],[515.5,387],[32.5,358.5],[237.5,536]]],
 [3.5,[[297.6,258.9],[517.5,391.4],[33.8,354.2],[228.5,535.6]]],
 [4.0,[[304,258.4],[517.5,395.7],[35.7,350.7],[220,533.8]]],
 [4.5,[[307.5,259.4],[517.4,398.2],[37.5,348.2],[214.7,534.2]]],
 [5.0,[[310.2,259],[517,399.1],[37.8,345.9],[210.8,533.1]]],
 [5.15,[[310.4,262],[516,402],[39.5,349],[210.6,535]]],
 [5.3,[[309,295],[505,428],[50,375],[212,558]]],
 [5.45,[[307,313],[494,449],[60,395],[214.5,576]]],
 [5.6,[[307.8,313.1],[492.2,450.2],[62.1,396.3],[214.4,577.3]]],
 [7.9,[[307.9,313.1],[492.3,450.3],[62.2,396.2],[214.3,577.3]]]];
function s1_homog(c){ /* unit square (u,v in 0..1) -> quad c[0]=(0,0) c[1]=(1,0) c[2]=(0,1) c[3]=(1,1) */
  const [x0,y0]=c[0],[x1,y1]=c[1],[x3,y3]=c[2],[x2,y2]=c[3];
  const dx1=x1-x2,dx2=x3-x2,dy1=y1-y2,dy2=y3-y2,sx=x0-x1+x2-x3,sy=y0-y1+y2-y3;
  const den=dx1*dy2-dx2*dy1,g=(sx*dy2-dx2*sy)/den,h=(dx1*sy-sx*dy1)/den;
  const a=x1-x0+g*x1,b=x3-x0+h*x3,d=y1-y0+g*y1,e=y3-y0+h*y3;
  return (u,v)=>{const w=g*u+h*v+1;return[(a*u+b*v+x0)/w,(d*u+e*v+y0)/w]}}
const s1_rnd=(i,j,k)=>{const x=Math.sin(i*127.1+j*311.7+k*74.7)*43758.5453;return x-Math.floor(x)};

SCENES.push(function s1(t){
  if(t<0||t>=7.9)return '';
  let g='';
  const defs=`<defs><radialGradient id="s1vig" cx="270" cy="300" r="330" gradientUnits="userSpaceOnUse"><stop offset="0" stop-color="#fbf8ea" stop-opacity=".6"/><stop offset=".55" stop-color="#fbf8ea" stop-opacity="0"/><stop offset="1" stop-color="#5a5545" stop-opacity=".1"/></radialGradient><clipPath id="s1clip"><rect x="32" y="59" width="476" height="513"/></clipPath>
<filter id="s1sh" x="-20%" y="-20%" width="140%" height="140%"><feGaussianBlur stdDeviation="3.2"/></filter>
<filter id="s1gl" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="1.6"/></filter>
<filter id="s1cs" x="-10%" y="-20%" width="120%" height="160%"><feGaussianBlur stdDeviation="4"/></filter>
<pattern id="s1stripe" width="9" height="9" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)"><rect width="9" height="9" fill="#3f7568"/><rect width="4" height="9" fill="#5f978a"/></pattern><pattern id="s1stripe2" width="9" height="9" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)"><rect width="9" height="9" fill="#7d7f76"/><rect width="4" height="9" fill="#6a6c63"/></pattern></defs>`;
  /* ---- counter ---- */
  const hdrOp=1-E(t,4.98,5.32);
  const cnt=Math.round(s1_tab([[.12,0],[.2,22],[.3,40],[.4,62],[.5,75],[.56,82],[.62,88],[.69,93],[.72,96],[.8,97],[.92,99],[1.0,100]],t));
  const numOp=s1_tab([[.1,0],[.12,.2],[.3,.6],[.45,1]],t)*hdrOp;
  if(numOp>0){
    const ny=-8*E(t,5.1,5.3);g+=T(270,196+ny,String(cnt),{size:66,w:700,a:'middle',font:FONT.title,op:r1(numOp),fill:C.ink,ls:-2,extra:` stroke="${C.paper}" stroke-width="3" paint-order="stroke"`});
    g+=T(270,218.5+ny,'SPARK EXECUTORS',{size:12.6,a:'middle',fill:'#7d7a6e',ls:1.1,font:FONT.body,op:r1(numOp)});}
  /* ---- grid ---- */
  const corn=s1_tab(S1_CORN,t),Hm=s1_homog(corn);
  const N=10,pt=(i,j)=>Hm(i/(N-1),j/(N-1));
  const items=[];
    for(let j=0;j<N;j++)for(let i=0;i<N;i++){
    /* build-in: drop from upper right, ordered from the left corner (i=0,j=9) */
    const d=i+9-j+s1_rnd(i,j,5)*1.6-.8,t0=.18+d*.04,p=cl((t-t0)/.12);
    if(p<=0)continue;
    const off=1-p*p,ox=0,oy=-off*160,zs=1+off*.12;
    const bs=.37-.05*E(t,5.15,5.45),c=pt(i,j),q=[[-bs,-bs],[bs,-bs],[bs,bs],[-bs,bs]].map(([a,b])=>{const r=pt(i+a,j+b);return[c[0]+(r[0]-c[0])*zs+ox,c[1]+(r[1]-c[1])*zs+oy]});
    const cx=c[0]+ox,cy=c[1]+oy;
    const sc=Math.hypot(pt(i+.5,j)[0]-pt(i-.5,j)[0],pt(i+.5,j)[1]-pt(i-.5,j)[1]);
    const h=sc*.34*zs;
    items.push({i,j,cx,cy,q,h,op:cl(p*4),sy:c[1],land:p,ox,oy});}
  items.sort((a,b)=>a.cy-b.cy);
  let sh='',bx='',gl='';
  for(const it of items){
    const {q,h}=it,b=q.map(p=>[p[0],p[1]+h]);
    /* shadow on ground (stays at landing spot) */
    const gq=q.map(p=>[p[0]-it.ox,p[1]-it.oy+h+2]);
    sh+=`<polygon points="${gq.map(p=>r1(p[0]+2)+','+r1(p[1]+3)).join(' ')}" opacity="${r1(it.land*it.land*.6)}"/>`;
    let s=`<g opacity="${r1(it.op)}">`;
    const mx=q.reduce((a,p)=>a+p[0],0)/4,my=q.reduce((a,p)=>a+p[1],0)/4;
    for(let k=0;k<4;k++){const a=q[k],c2=q[(k+1)%4];let nx=c2[1]-a[1],ny=-(c2[0]-a[0]);
      const ex=(a[0]+c2[0])/2-mx,ey=(a[1]+c2[1])/2-my;if(nx*ex+ny*ey<0){nx=-nx;ny=-ny}
      if(ny<=0)continue;
      const fill=nx<0?'#ebe9e0':'#e3e0d5';
      s+=P([a,c2,[c2[0],c2[1]+h],[a[0],a[1]+h]],fill,'#3d3b35',.75);}
    s+=P(q,'#f2f0e9','#3d3b35',.75);
    /* status dot */
    const dq=q.map(p=>[it.cx+(p[0]-it.cx)*.27,it.cy+(p[1]-it.cy)*.27]),hq=q.map(p=>[it.cx+(p[0]-it.cx)*.5,it.cy+(p[1]-it.cy)*.5]);
    const inten=cl(.25+.45*s1_rnd(it.i,it.j,2)+.35*(it.j-it.i+9)/18)*(.75+.25*Math.sin(t*3+it.i*1.3+it.j*.7));
    const tr=4.66+Math.hypot(it.i-4.5,it.j-4.5)*.05+s1_rnd(it.i,it.j,3)*.08,red=t>tr;
    const act=t>1.8&&(.6*s1_rnd(it.i,it.j,7)+.55*it.j/9+.2*Math.sin(t*2.2+s1_rnd(it.i,it.j,8)*6.28))>.62;
    const keepG=t<5.3&&s1_rnd(it.i,it.j,9)<.05+.06*(it.j-it.i)/9;const red2=red&&!keepG;const col=red2?(s1_rnd(it.i,it.j,4)>.55?'#a8282e':'#d98a8c'):(act||(keepG&&red))?'#2f7a5b':'#a9b5a4',a=red2?(.4+.6*s1_rnd(it.i,it.j,6)):red?.8:act?inten:.75;
    {const dop=r1(a);
      s+=`<polygon points="${hq.map(p=>r1(p[0])+','+r1(p[1])).join(' ')}" fill="${col}" opacity="${r1(dop*.25)}" filter="url(#s1gl)"/>`;
      s+=`<polygon points="${dq.map(p=>r1(p[0])+','+r1(p[1])).join(' ')}" fill="${col}" opacity="${dop}" stroke="${col}" stroke-width=".8" stroke-linejoin="round"/>`;}
    bx+=s+'</g>';}
  g=`<rect x="0" y="0" width="540" height="620" fill="url(#s1vig)"/>`+g;
  {const eo=E(t,4.9,4.97)*(1-E(t,5.05,5.13));if(eo>0)g+=`<ellipse cx="275" cy="450" rx="230" ry="110" fill="none" stroke="#8a877c" stroke-width="1" opacity="${r1(eo*.55)}"/>`;}
  g+=`<g clip-path="url(#s1clip)"><g fill="#000" opacity=".22" filter="url(#s1sh)">${sh}</g>${bx}</g>`;
  /* ---- status pill ---- */
  const pOp=E(t,2.52,2.68);
  if(pOp>0){const red=t>=4.73,col=red?'#9a3a3a':'#3f6e58',lab=red?'SLOWING DOWN':'ALL RUNNING';
    const w=red?130:120,x=270-w/2;
    const pk=red?1+.11*Math.sin(Math.PI*cl((t-4.73)/.35)):1;
    g+=`<g opacity="${r1(pOp)}" transform="translate(270 539.5) scale(${r1(pk*100)/100}) translate(-270 -539.5)">`+R(x+1,529,w,25,{rx:12.5,fill:'#000',op:.12})
      +R(x,527,w,25,{rx:12.5,fill:red?'#efe6dc':'#e8ede0',stroke:'#6f6c62',sw:1})
      +`<circle cx="${x+13}" cy="539.5" r="3" fill="${col}"/>`
      +T(x+24,544,lab,{size:13,fill:col,font:FONT.body,ls:.2})+'</g>';}
  /* ---- terminal ---- */
  if(t>=3.93&&t<5.65){
    const bot=s1_tab([[3.93,59],[4.0,71],[4.1,92],[4.2,122],[4.3,160],[4.4,209],[4.5,270],[4.567,307],[4.633,311],[4.733,314],[4.9,312.5],[6,312.5]],t);
    const sc=s1_lerp(1.075,1,E(t,3.95,4.6)),rot=-2.5*(1-E(t,4.0,4.6));
    const op=1-E(t,5.3,5.62);
    const cx=270,cy=bot-23;
    g+=`<g clip-path="url(#s1clip)"><g opacity="${r1(op)}" transform="translate(${cx} ${r1(cy)}) rotate(${r1(rot)}) scale(${r1(sc*100)/100})">`
      +`<rect x="-175" y="-17" width="350" height="46" fill="#000" opacity=".28" filter="url(#s1cs)"/>`
      +`<rect x="-175" y="-23" width="350" height="46" rx="1.5" fill="#1f1d14"/>`
      +`<text x="-160" y="10" font-size="18.5" letter-spacing=".4" fill="#e6e3d6" font-family="${FONT.body}">df.<tspan fill="#8c9ae0">groupBy</tspan>("country")</text></g></g>`;}
  /* ---- job card ---- */
  if(t>=5.1){
    const op=s1_tab([[5.1,0],[5.2,.4],[5.25,.6],[5.3,.78],[5.4,1]],t),dy=s1_tab([[5.1,13],[5.2,8],[5.25,4],[5.3,2],[5.4,0]],t),k=1+.002*dy;
    const clk=s1_tab([[5.1,2],[5.3,4],[5.4,7],[5.6,15],[5.9,25],[6.0,27],[6.3,37],[6.4,43],[6.5,66],[7.9,280]],t);
    const mm=String(Math.floor(clk/60)).padStart(2,'0')+':'+String(Math.floor(clk%60)).padStart(2,'0');
    const pc=s1_tab([[5.12,8],[5.3,15],[5.4,26],[5.5,36],[5.6,44],[5.75,53],[5.9,58],[6.0,60],[6.1,61],[6.25,62],[6.5,63]],t);
    const wait=t>6.43,stall=E(t,6.43,6.6)*.7;
    let c=`<g opacity="${r1(op)}" transform="translate(270 ${r1(236+dy)}) scale(${r1(k*1000)/1000}) translate(-270 -236)">`;
    c+=`<polygon points="83,172 460,172 465,315 78,315" fill="#000" opacity=".35" filter="url(#s1cs)"/>`;
    c+=`<path d="M86.5 166 H453.5 Q457.5 166 457.7 170 L461 303.5 Q461 307.5 457 307.5 H83 Q79 307.5 79.1 303.5 L82.5 170 Q82.6 166 86.5 166 Z" fill="#eeece2" stroke="#4a4840" stroke-width="1.3"/>`;
    c+=T(102,194,'SPARK JOB · STAGE 2 OF 3',{size:12.5,fill:'#a09d91',font:FONT.body});
    c+=T(101,235.5,'groupBy',{size:23.6,w:700,font:FONT.title})+T(225,235.5,'·',{size:23.6,w:700,a:'middle',font:FONT.title})+T(247,235.5,'count',{size:23.6,w:700,font:FONT.title});
    c+=T(438,235.5,mm,{size:28,w:700,a:'end',fill:'#3f7568',font:FONT.title});
    c+=R(100,260,340,23,{rx:2,fill:'#e3e1d4',stroke:'#2e2c27',sw:1.4});
    c+=`<rect x="100.7" y="260.7" width="${r1(338.6*pc/100)}" height="21.6" fill="url(#s1stripe)"/>`;
    if(stall>0)c+=`<rect x="100.7" y="260.7" width="${r1(338.6*pc/100)}" height="21.6" fill="url(#s1stripe2)" opacity="${r1(stall)}"/>`;
    c+=T(100,301.5,wait?'tasks waiting on data…':'running…',{size:12,fill:wait?'#6f6c62':'#5f8d80',font:FONT.body});
    c+=T(440,301.5,Math.round(pc)+'%',{size:12,fill:'#a8a598',a:'end',font:FONT.body});
    g+=c+'</g>';}
  return defs+g;
});
