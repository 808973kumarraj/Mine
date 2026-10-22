/* S7 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s7(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S7 63–70: filter + avoid */
  if(t>62.8&&t<70.7){const o=W(t,63,70.5);let g='';const second=t>=66.4;
    g+=T(63,142,second?'AND':'BEFORE THE SHUFFLE',{size:7,fill:C.muted,ls:1});
    if(!second){g+=T(63,170,`<tspan opacity="${r1(E(t,64.6,64.9))}">filter</tspan><tspan opacity="${r1(E(t,64.3,64.6))}"> <tspan fill="${C.green}">rows</tspan></tspan><tspan opacity="${r1(E(t,65,65.3))}"> + <tspan fill="${C.green}">columns</tspan></tspan>`,{size:19,w:700,raw:true});
      const cols=['order_id','country','amount','user_id','notes','meta'],sh=1-.35*E(t,65.4,66);
      g+=`<g transform="translate(200 360) scale(${r1(sh*100)/100}) translate(-270 -360)" opacity="${r1(1-E(t,66,66.4))}">`;
      for(let r=0;r<7;r++)for(let c=0;c<6;c++){let op=1;if(r>0&&r%2===0)op-=.9*E(t,64.4,64.9);if(c>2)op=Math.min(op,1-.9*E(t,65.1,65.6));
        const cx=110+c*50+r*16,cy=300+r*26-c*11;
        g+=`<g opacity="${r1(op)}">`+P([[cx,cy],[cx+46,cy-12],[cx+60,cy-5],[cx+14,cy+7]],r===0?'#dfe5f2':C.card,C.edge,.6)+P([[cx,cy],[cx+14,cy+7],[cx+14,cy+11],[cx,cy+4]],C.side,C.edge,.5)+P([[cx+14,cy+7],[cx+60,cy-5],[cx+60,cy-1],[cx+14,cy+11]],C.front,C.edge,.5)+(r===0?T(cx+16,cy-1,cols[c],{size:6,fill:C.blue}):'')+'</g>'}
      g+='</g>';
      const sp=bk(t,65.6,.5);g+=`<g opacity="${r1(cl((t-65.6)/.2)*(1-E(t,66.1,66.4)))}" transform="translate(0 ${r1((1-sp)*-20)})">`+box(390,296,80,72,22,{})+shuffleIcon(430,326,1,C.ink)+T(430,360,'SHUFFLE',{size:5.5,fill:C.muted,a:'middle',ls:.8})+'</g>'}
    else{g+=T(63,170,'avoid unnecessary shuffles',{size:19,w:700,fill:C.red,op:E(t,66.4,66.7)});
      const fl=E(t,69,69.8),cy=216+fl*90,h=176*(1-fl*.75);
      g+=`<g opacity="${r1(E(t,66.6,66.9))}">`+R(140,cy+5,264,h,{rx:4,fill:'#000',op:.1})+R(136,cy,264,h,{rx:4,stroke:C.ink,sw:1.3});
      g+=`<g transform="translate(268 ${r1(cy+h*.42)}) scale(1 ${r1(1-fl*.75)})">`+shuffleIcon(0,0,1.5,C.ink)+T(0,44,'extra shuffle',{size:13,w:700,a:'middle'})+'</g>';
      g+=`<line x1="150" y1="${r1(cy+h-12)}" x2="386" y2="${r1(cy+12)}" stroke="${C.red}" stroke-width="2.4" stroke-dasharray="300" stroke-dashoffset="${r1(300*(1-E(t,67.3,68.1)))}"/></g>`;
      g+=pill(270,cy+h+40,'✓ less data moved · faster job',C.green,{op:E(t,69.7,70)})}
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
