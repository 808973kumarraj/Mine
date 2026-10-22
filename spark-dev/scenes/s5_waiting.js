/* S5 — "shuffle overhead" / stage 2 waiting. Source window 46.85–57.3 s (presenter cut from ~54.5). */
const s5_EDGE='#2b2824';
function s5_poly(pts,fill,o={}){return `<polygon points="${pts.map(p=>r1(p[0])+','+r1(p[1])).join(' ')}" fill="${fill}" stroke="${o.stroke||s5_EDGE}" stroke-width="${o.sw||1.1}" stroke-linejoin="round"${o.dash?` stroke-dasharray="${o.dash}"`:''}/>`}
function s5_line(a,b,o={}){return `<line x1="${r1(a[0])}" y1="${r1(a[1])}" x2="${r1(b[0])}" y2="${r1(b[1])}" stroke="${o.stroke||s5_EDGE}" stroke-width="${o.sw||1.1}"${o.dash?` stroke-dasharray="${o.dash}"`:''} stroke-linecap="round"/>`}
const s5_add=(p,v)=>[p[0]+v[0],p[1]+v[1]];
const s5_ROT=-4.6;
function s5_txt(x,y,s,o){return T(x,y,s,Object.assign({},o,{extra:` transform="rotate(${s5_ROT} ${r1(x)} ${r1(y)})"`}))}
/* crates: front quad TL,TR,BR,BL + depth vector to the back */
const s5_CR=[
  {f:[[180,330],[223,326],[224,350],[182,354]],v:[-3,-23],l:'serialize',lx:202,ly:344,fr:'#a33d2b',tp:'#b25e4d'},
  {f:[[229,317],[270,313],[270,346],[229,350]],v:[-6,-23],l:'disk I/O',lx:250,ly:335,fr:'#a9442f',tp:'#b86a5a'},
  {f:[[276.7,307],[317.5,304],[317,343],[276.3,346.7]],v:[-6.7,-21],l:'network',lx:297,ly:327,fr:'#b0503f',tp:'#bb776a'},
  {f:[[323,314.7],[364.5,311.3],[362.8,338.3],[323,342]],v:[-7.5,-21.4],l:'deserialize',lx:343,ly:329,fr:'#b65f50',tp:'#c4877a'}];
function s5_crate(c,dy,op){const F=c.f.map(p=>[p[0],p[1]+dy]),v=c.v;
  let g=`<g opacity="${r1(op)}">`;
  g+=s5_poly([F[0],s5_add(F[0],v),s5_add(F[3],v),F[3]],c.tp);
  g+=s5_poly([F[0],F[1],s5_add(F[1],v),s5_add(F[0],v)],c.tp);
  g+=s5_poly(F,c.fr);
  g+=s5_txt(c.lx,c.ly+dy,c.l,{size:6.6,w:700,fill:'#f4e4dc',a:'middle'});
  return g+'</g>'}
function s5_stage(t){let g='';
  /* soft shadows */
  g+=`<polygon points="78,380 170,372 172,382 82,388" fill="#000" opacity=".10" filter="url(#s5_blur)"/>`;
  g+=`<polygon points="384,352 467,343 470,352 388,358" fill="#000" opacity=".10" filter="url(#s5_blur)"/>`;
  /* stage 1 */
  g+=s5_poly([[70,293],[158,286],[163,322],[69,329]],'#edeadd');
  g+=s5_poly([[75,296.5],[154,290],[157.5,319],[74.5,325.5]],'#f1eee3',{stroke:'#d2cdbf',sw:.8});
  g+=s5_line([69,329],[163,322]);g+=s5_line([158,286],[163,322]);
  g+=s5_poly([[69,329],[163,322],[166,369],[75,377]],'#e6e1d1');
  g+=s5_line([69,329],[163,322],{sw:1.3});
  g+=s5_txt(78,341,'STAGE 1',{size:7,w:700,fill:'#2b2824',ls:.5});
  const d1=t>47.75?C.green:'#bab7a7';
  g+=`<circle cx="155" cy="331" r="2.6" fill="${d1}"/>`;
  for(const L of [[[80,351],[148,345]],[[80,357.5],[148,351.7]],[[80,364],[150,358]]])g+=s5_line(L[0],L[1],{stroke:'#bfb9a9',sw:.7});
  /* tray / belt */
  g+=s5_poly([[175,357],[370,339],[370,345],[175,363]],'#d6cfbd');
  g+=s5_poly([[171,331],[366,314],[370,339],[175,357]],'#e4dfcf',{sw:.9});
  for(let i=1;i<26;i++){const a=i/26;g+=s5_line([171+195*a,331-17*a],[175+195*a,357-18*a],{stroke:'#c9c2b0',sw:.6})}
  /* stage 2 */
  g+=s5_poly([[368,269],[384,302],[381,349],[367,318]],'#e6dfcd',{sw:.9});
  g+=s5_line([367,269],[367,318],{dash:'4 2.5',sw:1});
  g+=s5_poly([[368,269],[450,262],[471,294],[384,302]],'#edeadd');
  g+=s5_line([368,269],[450,262],{dash:'4 2.5',sw:1.1,stroke:'#edeadd'});g+=s5_line([368,269],[450,262],{dash:'4 2.5'});
  g+=s5_poly([[384,302],[471,294],[465,340],[381,349]],'#e6e1d1');
  g+=s5_txt(391,314,'STAGE 2',{size:7,w:700,fill:'#2b2824',ls:.3});
  g+=`<circle cx="461.5" cy="305" r="2.6" fill="${t>49.0?'#b04a3a':'#bab7a7'}"/>`;
  for(const L of [[[389,323],[454,318]],[[389,329.5],[453,324]],[[388,336],[452.5,331]]])g+=s5_line(L[0],L[1],{stroke:'#bfb9a9',sw:.7});
  /* crates dropping in */
  for(let i=0;i<4;i++){const a=[47.05,47.44,47.83,48.25][i],p=cl((t-a)/[.32,.32,.32,.2][i]);if(p<=0)continue;
    g+=s5_crate(s5_CR[i],-190*(1-p*p),cl((t-a)/.08))}
  return g}
function s5_hourglass(op){return `<g opacity="${r1(op)}"><path d="M442 239.5 H468 L457.2 257.5 L468 275.5 H442 L452.8 257.5 Z" fill="#ebe8db" stroke="#1d1b18" stroke-width="2.2" stroke-linejoin="round"/><polygon points="448,242.6 462,242.6 455,247" fill="#8b887e" opacity=".5"/></g>`}
SCENES.push(function s5(t){
  if(t<46.85||t>=57.3)return '';
  const tt=Math.min(t,54.45);
  let s=`<defs><filter id="s5_blur" x="-20%" y="-50%" width="140%" height="200%"><feGaussianBlur stdDeviation="3"/></filter>`
    +`<filter id="s5_blur2" x="-10%" y="-50%" width="120%" height="200%"><feGaussianBlur stdDeviation="2.5"/></filter>`
    +`<pattern id="s5_hatch" width="9" height="9" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="9" height="9" fill="#e0dacb"/><rect width="4" height="9" fill="#cfc8b6"/></pattern></defs>`;
  const o=cl((t-46.85)/.3);
  /* shrink 49.42 -> 49.77 : scale eases in-out, position eases out */
  const kp=cl((tt-49.5)/.22),k=io(kp),kq=k;
  const sx=1+(.646-1)*k,sy=1+(.63-1)*k,ax=75+70*kq,ay=377+(228-377)*kq;
  const bo=cl((tt-48.55)/.25)*(1-cl((tt-49.58)/.14));
  let br='';
  if(bo>0)br=`<g opacity="${r1(bo)}"><path d="M149 387 V393 Q149 397 153 397 H400 Q404 397 404 393 V387" fill="none" stroke="#9c6b5f" stroke-width="1.1"/>`
    +T(277,415,'stage 2 waits for all of this',{size:13,fill:'#a26d60',a:'middle'})+'</g>';
  let gS=`<g transform="translate(${r1(ax)} ${r1(ay)}) scale(${sx.toFixed(4)} ${sy.toFixed(4)}) translate(-75 -377)">`+s5_stage(tt)+br+'</g>';
  /* title (fades in 46.95-47.35) */
  const ts=1+(.682-1)*k,tcx=270.5+.5*kq,tby=167+(152-167)*kq;
  let g=gS+`<g opacity="${r1(cl((t-46.95)/.4))}" transform="translate(${r1(tcx)} ${r1(tby)}) scale(${ts.toFixed(4)})">`+T(0,0,'shuffle <tspan fill="'+C.red+'">overhead</tspan>',{size:27.6,w:700,a:'middle',raw:true,font:FONT.title})+'</g>';
  /* task rows */
  const FIN=[349,381,318,365],HS=[51.27,51.39,51.56,51.7],WS=[53.71,53.8,53.9,53.94];
  for(let i=0;i<4;i++){const a=50.38+i*.12,p=cl((tt-a)/.36);if(p<=0)continue;
    const dx=-18*(1-io(p)),y=295.5+66*i;
    let r=`<g opacity="${r1(p)}" transform="translate(${r1(dx)} 0)">`;
    r+=T(72,y-6.5,'STAGE 2 · TASK '+(i+1),{size:10,fill:'#8d897d',ls:.6});
    r+=`<rect x="74" y="${y+4}" width="391" height="28" rx="3" fill="#000" opacity=".17" filter="url(#s5_blur2)"/>`;
    r+=R(72,y,395,29.5,{rx:3,fill:'#eeebdf'});
    const hp=cl((tt-HS[i])/1.9),hx=73+(FIN[i]-73)*(1-Math.pow(1-hp,2.85));
    const wp=cl((tt-WS[i])/.3),wx=hx+47*(1-Math.pow(1-wp,3));
    r+=`<clipPath id="s5_c${i}"><rect x="73" y="${y}" width="${r1(Math.max(0,hx-73))}" height="29.5"/></clipPath>`;
    r+=`<clipPath id="s5_w${i}"><rect x="${r1(hx)}" y="${y}" width="${r1(Math.max(0,wx-hx))}" height="29.5"/></clipPath>`;
    if(hp>0)r+=`<rect x="73" y="${y+.5}" width="${r1(hx-73)}" height="28.5" fill="url(#s5_hatch)"/>`+T(79.7,y+18.2,'waiting for data…',{size:10.5,fill:'#6f6b60',op:r1(cl((tt-HS[i]-.12)/.3))});
    if(wp>0)r+=`<rect x="${r1(hx)}" y="${y+.5}" width="${r1(wx-hx)}" height="28.5" fill="#3d7a66" opacity="${r1(cl(wp*4))}"/><g clip-path="url(#s5_w${i})">`+T(hx+5,y+18.2,'work',{size:10.5,fill:'#e6efe9'})+'</g>';
    r+=R(72,y,395,29.5,{rx:3,fill:'none',stroke:'#2f2b26',sw:1.1});
    g+=r+'</g>'}
  if(tt>52.03)g+=`<g transform="rotate(${r1(180*io(cl((tt-52.36)/.28)))} 455 257.5)">`+s5_hourglass(cl((tt-52.03)/.22))+'</g>';
  return s+`<g opacity="${r1(o)}">${g}</g>`;
});
