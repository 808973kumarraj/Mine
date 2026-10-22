/* S0_header — persistent header measured from the original (1080x1920 -> x0.5).
   Silkscreen caps 8.5px tall, ink x 33..371, baseline 109.3; rule y117.5 x33..507;
   navy progress bar (2px) grows over the full 80.5 s; amber block cursor blinks every 1.0605 s (0.55 s on). */
SCENES.push(function s0_header(t){
  let s='';
  const H='BINARY HABIT / SPARK PERF / 03',ADV=10.9;
  const n=Math.min(H.length,Math.floor(t*44.5));
  s+=`<line x1="33" y1="117.5" x2="507" y2="117.5" stroke="#c7c4b5" stroke-width="1"/>`;
  s+=`<rect x="33" y="116.5" width="${r1(474*cl(t/80.5))}" height="2" fill="#3d4b66"/>`;
  s+=T(31.4,109.3,'&gt; '+esc(H.slice(0,n)),{size:13.5,font:FONT.header,fill:'#4f4c44',ls:1.87,raw:true});
  if((t%1.0605)<0.55){const cx=36+ADV*Math.min(31.4,n+(n>=H.length?1.4:0));
    s+=`<rect x="${r1(cx)}" y="97" width="8" height="13" fill="#c6884a"/>`}
  return s;
});
