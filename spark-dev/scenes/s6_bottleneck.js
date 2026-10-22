/* S6 57.35–63.0: bottleneck. Hard cuts at both ends (matches original). */
const S6_PROF=[[63,0],[90,0],[110,1],[130,2.5],[150,4.5],[170,7],[190,11],[200,13.5],[210,17],[220,20.5],[230,24.5],[240,29],[248,31],[292,31],[300,28.5],[310,24],[320,20],[330,16.5],[340,13.5],[350,11],[360,9],[370,7],[380,5.5],[390,4.5],[400,3.5],[420,2],[440,1],[460,0],[477,0]];
function s6_cr(pts){let d='';for(let i=0;i<pts.length-1;i++){const p0=pts[Math.max(0,i-1)],p1=pts[i],p2=pts[i+1],p3=pts[Math.min(pts.length-1,i+2)];
  d+=` C${r1(p1[0]+(p2[0]-p0[0])/6)},${r1(p1[1]+(p2[1]-p0[1])/6)} ${r1(p2[0]-(p3[0]-p1[0])/6)},${r1(p2[1]-(p3[1]-p1[1])/6)} ${r1(p2[0])},${r1(p2[1])}`}return d}
function s6_pipe(y,op){
  const H=88,top=S6_PROF.map(([x,o])=>[x,y+o]),bot=S6_PROF.slice().reverse().map(([x,o])=>[x,y+H-o]);
  const d=`M63,${r1(y)}`+s6_cr(top)+` L477,${r1(y+H)}`+s6_cr(bot)+' Z';
  const hl=`M75,${r1(y+5)}`+s6_cr(S6_PROF.filter(p=>p[0]>=75&&p[0]<=465).map(([x,o])=>[x,y+o+4.5]));
  return `<g opacity="${r1(op)}"><path d="${d}" transform="translate(1.5,3)" fill="#000" opacity=".08" filter="url(#s6blur)"/>`+
    `<path d="${d}" fill="url(#s6grad)" stroke="#16140a" stroke-width="1.4" stroke-linejoin="round"/>`+
    `<path d="${hl}" fill="none" stroke="#fff" stroke-width="2.5" opacity=".3" stroke-linecap="round"/></g>`;
}
const S6_JAM=[[78,98,119,138.5,155.5,176,194],[80.5,96,117,137.5,157,178,194],[80,98,117.5,138.5,155.5,174,198],[79,97,115,138.5,155,176,196]];
const S6_JY=[[11,11.5,11.5,11,10,13,12.5],[27,27.5,28,28,28.5,26,27],[42.5,43,44.5,43.5,43.5,43.5,43.5],[59,58.5,58.5,59,58,60,60]];
const S6_ROT=[[0,0,0,0,-4,2,0],[0,0,0,0,0,-3,0],[0,0,3,0,0,0,0],[2,0,0,0,0,0,0]];
const S6_BLUE=[[0,4],[1,5],[2,6],[3]];
const S6_ROWY=[12.5,28,45,60.5];
function s6_blk(x,y,blue,op=1,rot=0,w=17,h=11.5){return `<rect transform="rotate(${rot} ${r1(x+w/2)} ${r1(y+h/2)})" x="${r1(x)}" y="${r1(y)}" width="${w}" height="${h}" rx="1.8" fill="${blue?'#2e4b88':'#efede2'}" stroke="${blue?'#22386a':'#2a281e'}" stroke-width=".8" opacity="${r1(op)}"/>`}
function s6_jam(y,t,op,fadeRows){let g='';
  for(let r=0;r<4;r++)for(let c=0;c<7;c++){
    const jx=Math.sin(t*23+r*3.1+c*1.7)*.6,jy=Math.cos(t*19+r*2.3+c*2.9)*.5;
    let o=op;if(fadeRows&&(r>=2||(r==1&&c>=5)))o*=1-E(t,59.4+(6-c)*.05+(3-r)*.1,59.7+(6-c)*.05+(3-r)*.1);
    if(o>0.01)g+=s6_blk(S6_JAM[r][c]+2+jx,y+S6_JY[r][c]+1.5+jy,S6_BLUE[r].includes(c),o,S6_ROT[r][c])}
  return g}
function s6_pill(cx,y,w,h,s,col,fill,op){if(op<=0)return '';return `<g opacity="${r1(op)}">`+R(cx-w/2,y,w,h,{rx:h/2,fill,stroke:col,sw:1})+T(cx,y+h/2+4.5,s,{size:12.8,w:400,fill:col,a:'middle',font:FONT.body})+'</g>'}
SCENES.push(function s6(t){
  if(t<57.35||t>=63.4)return '';
  let g=`<defs><linearGradient id="s6grad" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#e9e7dc"/><stop offset=".5" stop-color="#dedcd0"/><stop offset="1" stop-color="#cfcbbc"/></linearGradient><filter id="s6blur"><feGaussianBlur stdDeviation="3"/></filter></defs>`;
  const y=278.5-66*(1-Math.pow(1-cl((t-59.02)/.45),3));
  g+=s6_pipe(y,1);
  /* neck glow */
  const gl=E(t,58.3,58.55)*(1-E(t,59.7,59.95));
  if(gl>0)g+=`<ellipse cx="270" cy="${r1(y+44)}" rx="20" ry="9" fill="${C.red}" opacity="${r1(.28*gl*(.8+.2*Math.sin(t*6)))}" filter="url(#s6blur)"/>`;
  /* blocks */
  if(t<60.45)g+=s6_jam(y,t,1,true);
  else{const K=[[0,0],[0,1],[0,2],[0,3],[0,4],[0,5],[1,1],[0,6],[1,0],[1,2],[1,3],[1,4]];
    const tgt=[];for(let r=0;r<3;r++)for(let c=0;c<5;c++)if(r<2||c>=3)tgt.push([387+c*16,y+22.5+r*12.4]);
    const tblue=[0,4,6];
    /* order through the neck: rightmost leaves first */
    const ord=K.map((k,i)=>i).sort((p,q)=>(S6_JAM[K[q][0]][K[q][1]]-S6_JAM[K[p][0]][K[p][1]])||(K[p][0]-K[q][0]));
    for(let n=0;n<12;n++){const i=ord[n],[r,c]=K[i],sx=S6_JAM[r][c],sy=y+S6_ROWY[r];const [tx,ty]=tgt[i];
      const m=11-n;/* m: 0 = last to leave (leftmost) */
      const lx=m<3?83+m*17:136+(m-3)*2.6+((m*5)%3)*4,ly=y+31+(m<3?0:((m*7)%5-2)*3.2);
      const lr=m<3?0:((m*11)%7-3)*5;
      const k1=io(cl((t-60.42)/.18));
      let x=sx+(lx-sx)*k1,yy=sy+(ly-sy)*k1,rot=lr*k1;
      const p=cl((t-60.5-n*.02)/.45);
      if(p>0){const nx=262;let q;
        if(p<.35){q=io(p/.35);x=lx+(nx-lx)*q;yy=ly+(y+38-ly)*q;rot=lr*(1-q)+((n*13)%9-4)*3*q}
        else{q=1-Math.pow(1-(p-.35)/.65,2);const sc=((n*7)%5-2)*4*(1-q)*Math.sin(q*Math.PI);
          x=nx+(tx-nx)*q;yy=y+38+(ty-y-38)*q+sc;rot=((n*13)%9-4)*3*(1-q)}}
      const sz=cl((t-61.15)/.15);g+=s6_blk(x,yy,tblue.includes(i),1,rot,17-sz,11.5+.5*sz)}}
  g+=T(65,y+0.5,'− less data',{size:19.5,w:700,fill:C.green,font:FONT.body,op:E(t,59.1,59.45)});
  g+=s6_pill(270,y-30,97,25.5,'bottleneck',C.red,'#f3e2d6',E(t,58.25,58.5)*(1-E(t,59.75,59.95)));
  g+=s6_pill(428,y-20.5,75,24,'√ flows',C.green,'#e9ede0',E(t,60.8,61.0));
  /* second pipe */
  const o2=E(t,61.3,61.7);
  if(o2>0){const y2=402.5+55*Math.pow(1-cl((t-61.32)/.55),3);
    g+=`<g opacity="${r1(o2)}">`+s6_pipe(y2,1)+s6_jam(y2,t,1,false)+T(65,y2+0.5,'+ more executors',{size:19.5,w:700,fill:C.red,font:FONT.body})+'</g>';
    for(let i=0;i<4;i++){const o=E(t,61.95+i*.15,62.25+i*.15);if(o>0)g+=`<g opacity="${r1(o)}">`+R(341+i*31,y2+93,25,17.5,{rx:2.5,fill:'#efede2',stroke:'#2a281e',sw:.8})+`<circle cx="${r1(346+i*31)}" cy="${r1(y2+98.5)}" r="1.8" fill="${C.green}"/></g>`}
    g+=s6_pill(426,y2-22,126,26,'× still stuck',C.red,'#f6e8dc',E(t,62.5,62.8));}
  if(t>62.95)g=`<g opacity="${(1-cl((t-62.95)/.4)).toFixed(3)}">${g}</g>`;return g;
});
