/* S9 — end card, 77.5–80.5 s (hard cut in from full-screen presenter). */
SCENES.push(function s9(t){
  if(t<77.5||t>80.6) return '';
  const SANS="'IBM Plex Sans', 'Helvetica Neue', Arial, sans-serif";
  const MONO="'IBM Plex Mono', 'Space Mono', ui-monospace, monospace";
  let g='';
  // hide header cursor (original shows none on the end card)
  g+=`<rect x="376" y="95.5" width="13" height="16" fill="${C.paper}"/>`;
  // warm vignette + stronger 12px grid as in the original end card
  let gl='';for(let v=6;v<=540;v+=12)gl+=`M${v} 0V960`;for(let v=6;v<=960;v+=12)gl+=`M0 ${v}H540`;
  g+=`<defs><radialGradient id="s9vg" cx="270" cy="330" r="420" gradientUnits="userSpaceOnUse"><stop offset="0" stop-color="#fffcf0" stop-opacity=".35"/><stop offset=".6" stop-color="#fffcf0" stop-opacity="0"/><stop offset="1" stop-color="#6b6550" stop-opacity=".06"/></radialGradient></defs>`;
  g+=`<path d="${gl}" stroke="#cfccbf" stroke-width=".6" opacity=".7" fill="none"/>`;
  g+=`<rect width="540" height="960" fill="url(#s9vg)"/>`;
  const a0=E(t,77.48,77.76);
  g+=`<g opacity="${r1(a0)}">`;
  g+=T(270,204,'DATA / 03',{size:11,fill:'#5f5c54',a:'middle',ls:3.2,font:FONT.header});
  g+=T(270,258,'Spark Performance',{size:32.6,w:700,a:'middle',font:FONT.title});
  g+=T(270,292,'Episode 3 · Shuffling',{size:16.5,w:400,fill:'#8a877d',a:'middle',font:SANS});
  g+='</g>';
  g+=`<defs><filter id="s9sh" x="-30%" y="-30%" width="160%" height="200%"><feDropShadow dx="0" dy="6" stdDeviation="5" flood-color="#5a5544" flood-opacity=".38"/></filter></defs>`;
  KEYS.forEach((k,i)=>{
    const st=[77.55,77.68,77.81,77.94][i],pp=E(t,st,st+.3),p=E(t,st,st+.5),q=cl((t-st)/.4); if(p<=0) return;
    const x=138+i*70.5, y=340.5-22*(1-pp)+2.5*Math.sin(Math.PI*cl((q-.45)/.55));
    g+=`<g opacity="${r1(p)}" filter="url(#s9sh)">`+R(x,y,52,28,{rx:4,fill:'#f2efe5',stroke:'#1d1d1b',sw:1})+(i?'':R(x,y,52,28,{rx:4,fill:'#2d4b8c',stroke:'#1d1d1b',sw:1,op:r1(.45+.55*E(t,77.85,78.15))}))
      +T(x+26,y+18.5,k,{size:12.5,w:500,fill:i?'#1d1d1b':'#fff',a:'middle',font:MONO})+'</g>';
  });
  const b=E(t,78.17,78.7);
  g+=`<g opacity="${r1(b)}">`+T(264.5,471,'BinaryHabit',{size:22.6,w:700,a:'middle',font:FONT.title})+`<circle cx="348.5" cy="468.5" r="3" fill="#c98f5c"/></g>`;
  return g;
});
