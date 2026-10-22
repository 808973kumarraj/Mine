/* S1 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s1(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S1 0–7.9: 100 executors */
  if(t<8.2){const o=W(t,-1,7.9);let g='';
    const card=E(t,4.9,5.3),slow=E(t,5.6,6.4),gy=card*50;
    g+=T(270,190,String(Math.round(100*E(t,0,.75))),{size:44,w:800,a:'middle',op:1-card});
    g+=T(270,210,'SPARK EXECUTORS',{size:7.5,fill:C.muted,a:'middle',ls:1.4,op:1-card});
    for(let j=0;j<10;j++)for(let i=0;i<10;i++){
      const d=(i+j)*.035,p=bk(t,.05+d,.5),cx=270+(i-j)*21,cy=268+(i+j)*12.2+gy-(1-p)*40;
      const red=slow>((i*37+j*11)%100)/100*.9+.05&&(i*7+j)%3;
      g+=cube(cx,cy,19,9,{op:cl((t-.05-d)/.25),dot:red?C.red:C.green})}
    g+=pill(270,520+gy*.4,slow>.4?'● SLOWING DOWN':'● ALL RUNNING',slow>.4?C.red:C.green,{op:E(t,2.3,2.6)});
    g+=term(150,228,240,code('df.groupBy("country")'),{op:E(t,3.9,4.15)*(1-E(t,4.9,5.1)),dy:-10*(1-bk(t,3.9))});
    if(t>4.9){const cy=166-14*(1-bk(t,4.9)),pc=Math.min(63,44*E(t,5,5.4)+12*E(t,5.4,5.9)+7*E(t,5.9,7.5));
      const clk=t<5.9?(t-4.9)*23:23+(t-5.9)*65,mm=String(Math.floor(clk/60)).padStart(2,'0')+':'+String(Math.floor(clk%60)).padStart(2,'0');
      g+=`<g opacity="${r1(card)}">`+R(84,cy+4,372,128,{rx:4,fill:'#000',op:.08})+R(80,cy,372,128,{rx:4,stroke:C.ink,sw:1.4})
        +T(98,cy+24,'SPARK JOB · STAGE 2 OF 3',{size:7,fill:C.muted,ls:1})
        +T(98,cy+58,'groupBy · count',{size:18,w:700})+T(434,cy+58,mm,{size:21,w:700,a:'end',fill:t>6.6?C.red:C.teal})
        +R(98,cy+76,336,16,{rx:2,fill:'none',stroke:C.ink,sw:1})
        +`<rect x="99" y="${r1(cy+77)}" width="${r1(334*pc/100)}" height="14" fill="url(#stripe)"/>`
        +T(98,cy+108,t>6.6?'tasks waiting on data…':'running…',{size:7.5,fill:C.muted})+T(434,cy+108,Math.round(pc)+'%',{size:7.5,fill:C.muted,a:'end'})+'</g>'}
    s+=`<g opacity="${r1(o)}">${g}</g>`}

  return s;
});
