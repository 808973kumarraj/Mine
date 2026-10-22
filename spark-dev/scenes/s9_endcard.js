/* S9 — returns an SVG string for time t (seconds of the source video). Only this file is yours to edit. */
SCENES.push(function s9(t){
  let s='';const blink=Math.floor(t*2.2)%2===0;
  /* S9 77.5–: end card */
  if(t>77.3){let g='';
    g+=T(270,300,'DATA / 03',{size:8,fill:C.muted,a:'middle',ls:2});
    g+=T(270,342-8*(1-E(t,77.5,78.1)),'Spark Performance',{size:26,w:800,a:'middle'});
    g+=T(270,366,'Episode 3 · Shuffling',{size:9,fill:C.muted,a:'middle'});
    KEYS.forEach((k,i)=>{const x=172+i*52;g+=`<g opacity="${r1(E(t,77.8+i*.12,78.1+i*.12))}">`+R(x,394,40,20,{rx:4,fill:i?C.paper:C.blue,stroke:C.blue,sw:1.2})+T(x+20,408,k,{size:8,w:700,fill:i?C.blue:'#fff',a:'middle'})+'</g>'});
    g+=T(270,470,'BinaryHabit<tspan fill="'+C.red+'">.</tspan>',{size:17,w:800,a:'middle',raw:true,op:E(t,78.4,78.8)});
    s+=`<g opacity="${r1(E(t,77.5,77.9))}">${g}</g>`}

  return s;
});
