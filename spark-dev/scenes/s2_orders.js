/* S2 7.9–14.2: a billion orders. Executors drop in as open trays seen from above, slabs fall in, bars fill. */
const s2_BOX=[ /* [BTL,BTR,TL,TR,BR,BL] in 540x960 coords, measured from source frames */
  [[55,317.5],[147,314],[51,355],[147,351],[149.5,397.5],[56.5,401.5]],
  [[163,312.5],[253.5,309],[163.5,349],[257.5,346],[258,392.5],[166,396]],
  [[270,305],[359,301],[274,345],[368.5,339],[366.5,385],[275.5,389.5]],
  [[373,300],[466,296],[382,338.5],[475,333],[471.5,380],[381.5,384.5]]];
const s2_DROP=[9.01,9.22,9.45,9.66];
const s2_ink='#3a3832',s2_frontC='#ddd7c5',s2_topC='#efede4',s2_slabT='#eceadf',s2_slabF='#d6d2c5',s2_grn='#2f7563',s2_bar='#457661';
const s2_pts=a=>a.map(p=>r1(p[0])+','+r1(p[1])).join(' ');
const s2_poly=(a,fill,sw=1.1,stroke=s2_ink,extra='')=>`<polygon points="${s2_pts(a)}" fill="${fill}" stroke="${stroke}" stroke-width="${r1(sw*.85)}" stroke-linejoin="round"${extra}/>`;
const s2_lerp=(a,b,k)=>[a[0]+(b[0]-a[0])*k,a[1]+(b[1]-a[1])*k];
/* bilinear point on top face: u across (0..1), v back->front (0..1) */
const s2_top=(B,u,v)=>s2_lerp(s2_lerp(B[0],B[1],u),s2_lerp(B[2],B[3],u),v);
/* slab order per box: back row first */
const s2_SLABS=(()=>{const L=[];let k=0;const ord=[[0,0],[0,1],[1,0],[1,1],[2,0],[2,1]];
  for(let r=0;r<6;r++)for(let e=0;e<4;e++){const j=(e*5+r*3)%4;L.push({e:j,r:ord[r][0],c:ord[r][1],t:10.6+k*0.058+((e*7+r*3)%5)*0.012});k++}
  return L})();
const s2_rnd=n=>{const x=Math.sin(n*127.1+31.7)*43758.5453;return x-Math.floor(x)};
s2_SLABS.forEach((sl,i)=>{sl.sx=(s2_rnd(i)-.5)*70;sl.sy=150+s2_rnd(i+50)*60});
const s2_EXTRA=[10.75,11.15,11.55,11.85];

function s2_slab(B,r,c,lift,op){
  const u0=.07+c*.44,u1=u0+.41,v0=.1+r*.3,v1=v0+.3,h=6.5;
  const a=s2_top(B,u0,v0),b=s2_top(B,u1,v0),cc=s2_top(B,u1,v1),d=s2_top(B,u0,v1);
  const up=p=>[p[0],p[1]-h-lift];const dn=p=>[p[0],p[1]-lift];
  return `<g opacity="${r1(op)}">`+s2_poly([up(d),up(cc),dn(cc),dn(d)],s2_slabF,.8)+s2_poly([up(a),up(b),up(cc),up(d)],s2_slabT,.8)+'</g>'}

const s2_BOX0=JSON.parse(JSON.stringify(s2_BOX));
function s2_box(t,e,full){
  const B=s2_BOX[e],[BTL,BTR,TL,TR,BR,BL]=B;let g='';
  const h=BL[1]-TL[1];
  /* soft drop shadow */
  g+=`<polygon points="${s2_pts([[TL[0]+4,TL[1]+10],[TR[0]+7,TR[1]+10],[BR[0]+7,BR[1]+6],[BL[0]+4,BL[1]+6]])}" fill="#3a3528" opacity=".22" filter="url(#s2blur)"/>`;
  if(BTL[0]<TL[0]-1)g+=s2_poly([BTL,TL,BL,[BTL[0],BTL[1]+h]],'#d7d2c4',.9);
  if(BTR[0]>TR[0]+1)g+=s2_poly([BTR,TR,BR,[BTR[0],BTR[1]+h]],'#d7d2c4',.9);
  g+=s2_poly([BTL,BTR,TR,TL],s2_topC,1.1);
  const i=[s2_top(B,.04,.06),s2_top(B,.96,.06),s2_top(B,.96,.94),s2_top(B,.04,.94)];
  g+=`<polygon points="${s2_pts(i)}" fill="none" stroke="#bdb8aa" stroke-width=".5" stroke-dasharray="1.5 1"/>`;
  for(const s of s2_SLABS){if(s.e!==e)continue;const p=(t-s.t)/.38;if(p<1)continue;g+=s2_slab(B,s.r,s.c,0,1)}
  /* front face with affine-mapped content (local 96x46) */
  g+=s2_poly([TL,TR,BR,BL],s2_frontC,1.2);
  const m=[(TR[0]-TL[0])/96,(TR[1]-TL[1])/96,(BL[0]-TL[0])/46,(BL[1]-TL[1])/46,TL[0],TL[1]].map(v=>+v.toFixed(4));
  let f=`<g transform="matrix(${m.join(' ')})">`;
  f+=T(8.5,13,'EXECUTOR '+(e+1),{size:7.4,w:700,ls:.9,fill:'#2a2924',font:FONT.body});
  const land=s2_DROP[e]+.2,blink=.55+.45*Math.cos((t-land)*4.2+e*1.7);
  f+=`<circle cx="87" cy="7" r="2.4" fill="${s2_grn}" opacity="${r1(t<land+.3?.35:blink)}"/>`;
  const tb=11.36,bon=t>=tb;
  f+=`<g stroke="#bcb6a6" stroke-width=".6"><line x1="9.5" y1="21" x2="81" y2="21"/><line x1="9.5" y1="27" x2="81" y2="27"/>${bon?'':'<line x1="9.5" y1="33" x2="81" y2="33"/>'}</g>`;
  if(bon){f+=`<rect x="10.5" y="34.5" width="78" height="5.5" rx="1.2" fill="#ece8dc" stroke="${s2_ink}" stroke-width="1"/>`;
    f+=`<rect x="10.5" y="34.5" width="${r1(Math.max(1.5,75*(1-Math.pow(1-cl((t-11.44)/1.31),2))))}" height="5.5" rx="1.2" fill="${s2_bar}" opacity="${E(t,tb,tb+.08)}" stroke="${s2_ink}" stroke-width="1"/>`}
  f+='</g>';
  g+=f;
  /* drop-in */
  if(t<s2_DROP[e])return '';
  const p=cl((t-s2_DROP[e])/.2),dy=-130*Math.pow(1-p,3);
  const sh=p<1?`<polygon points="${s2_pts([[TL[0]+2,TL[1]+12],[TR[0]+4,TR[1]+12],[BR[0]+4,BR[1]-2],[BL[0]+2,BL[1]-2]])}" fill="#2e2a20" opacity="${r1(.55*(1-p*.75))}" filter="url(#s2blur2)"/>`:'';
  return sh+`<g transform="translate(0 ${r1(dy)})">${g}</g>`}

SCENES.push(function s2(t){
  if(t<7.9||t>=14.2)return '';
  let s=`<defs><radialGradient id="s2vig" cx="50%" cy="38%" r="75%"><stop offset="0" stop-color="#5a5440" stop-opacity="0"/><stop offset=".6" stop-color="#5a5440" stop-opacity=".03"/><stop offset="1" stop-color="#5a5440" stop-opacity=".16"/></radialGradient><filter id="s2blur" x="-30%" y="-30%" width="160%" height="160%"><feGaussianBlur stdDeviation="3.5"/></filter><filter id="s2blur2" x="-30%" y="-50%" width="160%" height="200%"><feGaussianBlur stdDeviation="4"/></filter></defs><rect width="540" height="960" fill="url(#s2vig)"/>`;
  /* counter: quint ease-out 7.816 -> 8.49 */
  const cp=cl((t-7.85)/.67),cv=Math.round(1e9*(1-Math.pow(1-cp,5)));
  const co=cl((t-7.8)/.18)*(1-cl((t-13.9)/.18)),cdy=-5*io((t-13.55)/.25)-15*io((t-13.86)/.2);
  s+=`<g opacity="${r1(co)}" transform="translate(0 ${r1(cdy)})">`;
  s+=T(270,173,cv.toLocaleString('en-US'),{size:41.3,w:700,a:'middle',ls:-.3,font:FONT.title});
  s+=T(270,194,'ORDERS',{size:9.5,fill:C.muted,a:'middle',ls:1.6});
  s+='</g>';
  s+=T(470,257,'+ 96 more executors',{size:12.6,fill:C.muted,a:'end',op:E(t,10.58,10.8)*(1-cl((t-13.82)/.2))});
  /* executor group, shrinks toward the s3 layout at the end */
  const k=io((t-13.8)/.25),sc=1-.065*k,tx=(66-52*sc)*k,ty=(321-292*sc-7)*k;
  let g='';
  for(let e=0;e<4;e++)g+=s2_box(t,e);
  /* falling slabs */
  for(const sl of s2_SLABS){const p=(t-sl.t)/.38;if(p<0||p>=1)continue;
    const B=s2_BOX[sl.e],dst=s2_top(B,.07+sl.c*.44,.1+sl.r*.3),q=1-p*p;
    const lift=(dst[1]-sl.sy)*q;g+=`<g transform="translate(${r1(sl.sx*q)} 0)">`+s2_slab(B,sl.r,sl.c,lift,cl(p/.15))+'</g>'}
  s+=`<g transform="translate(${r1(tx)} ${r1(ty-2*(1-k))}) scale(${sc.toFixed(4)})">${g}</g>`;
  /* slabs heading to the 96 other executors (top right) */
  s2_EXTRA.forEach((a,i)=>{const p=(t-a)/.5;if(p<0||p>1)return;const x=320+140*p+(i*11)%20,y=275-115*p;
    const B=[[x,y],[x+38,y-2],[x,y+10],[x+38,y+8]];
    s+=`<g opacity="${r1(cl(p/.15)*cl((1-p)/.25))}">`+s2_poly([[x,y+10],[x+38,y+8],[x+38,y+14],[x,y+16]],s2_slabF,.8)+s2_poly([B[0],B[1],B[3],B[2]],s2_slabT,.8)+'</g>'});
  /* pill */
  const pp=E(t,13.42,13.62),po=pp*(1-.65*cl((t-13.86)/.2));
  if(po>0){const w=200,h=30;
    s+=`<g opacity="${r1(po)}" transform="translate(270 538) rotate(-2.2) scale(${(1.2-.2*pp).toFixed(3)}) ">`+
      `<rect x="${-w/2}" y="${-h/2}" width="${w}" height="${h}" rx="15" fill="#f1efe7" stroke="#9db3a6" stroke-width=".8"/>`+
      T(0,5,'v PARALLEL · NO PROBLEM',{size:12.6,w:400,fill:'#56876f',a:'middle',ls:.3})+'</g>'}
  return s;
});
