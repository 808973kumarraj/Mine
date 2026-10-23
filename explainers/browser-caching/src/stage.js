// Stage markup. Everything sits on a 1600x900 design canvas that the page scales to fit,
// so positions below are design pixels. Each <section class="scene"> is one layer that the
// timeline fades in and out; ids are what src/timeline.js animates.

const P = (l, t, w, h) =>
  `left:${l}px;top:${t}px` + (w != null ? `;width:${w}px` : '') + (h != null ? `;height:${h}px` : '');
const acc = (w) => `<span class="acc">${w}</span>`;

const INK = '#161513';
const SURFACE = '#faf6ec';

const ICON = {
  file: `<svg class="i-file" viewBox="0 0 16 20" width="16" height="20" aria-hidden="true"><path d="M2 1.5h8l4.5 4.5v12.5H2z" fill="${SURFACE}" stroke="${INK}" stroke-width="2" stroke-linejoin="round"/><path d="M10 1.5V6h4.5" fill="none" stroke="${INK}" stroke-width="2" stroke-linejoin="round"/></svg>`,
  lens: `<svg viewBox="0 0 84 84" width="84" height="84" aria-hidden="true"><circle cx="34" cy="34" r="22" fill="rgba(250,246,236,.6)" stroke="${INK}" stroke-width="5"/><line x1="51" y1="51" x2="75" y2="75" stroke="${INK}" stroke-width="9" stroke-linecap="round"/></svg>`,
  check: `<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true"><path d="M5 12.5l4.5 4.5L19 7.5" fill="none" stroke="${INK}" stroke-width="3.4" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
  cross: `<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18" fill="none" stroke="${INK}" stroke-width="3.4" stroke-linecap="round"/></svg>`,
  noentry: `<svg viewBox="0 0 40 40" width="36" height="36" aria-hidden="true"><circle cx="20" cy="20" r="16" fill="${SURFACE}" stroke="#e07a45" stroke-width="4.5"/><line x1="9" y1="31" x2="31" y2="9" stroke="#e07a45" stroke-width="4.5" stroke-linecap="round"/></svg>`,
  tag: `<svg viewBox="0 0 26 22" width="26" height="22" aria-hidden="true"><path d="M2 3.5A1.5 1.5 0 0 1 3.5 2H13l10 9-10 9H3.5A1.5 1.5 0 0 1 2 18.5z" fill="#f0c860" stroke="${INK}" stroke-width="2" stroke-linejoin="round"/><circle cx="7.5" cy="11" r="2" fill="${INK}"/></svg>`,
  eq: `<svg viewBox="0 0 30 30" width="28" height="28" aria-hidden="true"><path d="M6 11h18M6 19h18" fill="none" stroke="${INK}" stroke-width="3.2" stroke-linecap="round"/></svg>`,
  neq: `<svg viewBox="0 0 30 30" width="28" height="28" aria-hidden="true"><path d="M6 11h18M6 19h18M19 5L11 25" fill="none" stroke="${INK}" stroke-width="3.2" stroke-linecap="round"/></svg>`,
  right: `<svg viewBox="0 0 24 16" width="24" height="16" aria-hidden="true"><path d="M2 8h18M14 2.5L20.5 8 14 13.5" fill="none" stroke="${INK}" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
  left: `<svg viewBox="0 0 24 16" width="24" height="16" aria-hidden="true"><path d="M22 8H4M10 2.5L3.5 8 10 13.5" fill="none" stroke="${INK}" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
  pointer: `<svg viewBox="0 0 28 36" width="38" height="49" aria-hidden="true"><path d="M3 2l21 19-9.5 1.5L20 33l-5 2.2-5.5-10.6L3 31z" fill="${SURFACE}" stroke="${INK}" stroke-width="2.4" stroke-linejoin="round"/></svg>`,
  play: `<svg viewBox="0 0 40 40" width="64" height="64" aria-hidden="true"><path d="M13 8.5v23L32 20z" fill="${SURFACE}" stroke="${SURFACE}" stroke-width="2" stroke-linejoin="round"/></svg>`,
  clock: (hand) => `<svg viewBox="0 0 56 56" width="56" height="56" aria-hidden="true"><circle cx="28" cy="28" r="24" fill="${SURFACE}" stroke="${INK}" stroke-width="3.5"/><line x1="28" y1="28" x2="38" y2="28" stroke="${INK}" stroke-width="3.5" stroke-linecap="round"/><line id="${hand}" x1="28" y1="28" x2="28" y2="10" stroke="#e07a45" stroke-width="3.5" stroke-linecap="round"/><circle cx="28" cy="28" r="3.5" fill="${INK}"/></svg>`,
};

// An arrowhead at (x, y) pointing along (dx, dy).
function head(x, y, dx, dy, size = 13) {
  const len = Math.hypot(dx, dy);
  const ux = dx / len, uy = dy / len;
  const a = Math.PI / 6;
  const p = (s) => {
    const c = Math.cos(s * a), sn = Math.sin(s * a);
    const rx = -(ux * c - uy * sn), ry = -(ux * sn + uy * c);
    return `${(x + rx * size).toFixed(1)} ${(y + ry * size).toFixed(1)}`;
  };
  return `M ${p(1)} L ${x} ${y} L ${p(-1)}`;
}

const unit = () => `<div class="unit"><i class="led"></i><i class="led off"></i><span class="bars"></span></div>`;

function chrome(chapters) {
  const chips = chapters
    .map((c) => `<div class="chip up chapchip" id="cc-${c.id}">${c.title}</div>`)
    .join('');
  return `${chips}
  <div class="rail" id="rail" style="${P(1152, 40, 400)}"></div>
  <div class="chip up examples" id="examples">Example values</div>`;
}

function intro1() {
  return `<section class="scene" id="sc-intro1">
  <h2 class="headline" id="h-i1">The browser checks its cache ${acc('first')}.</h2>
  <div class="card mini-browser" id="i1-browser" style="${P(120, 300, 470, 300)}">
    <div class="titlebar"><span class="dots"><i></i><i></i><i></i></span><span class="addr"><span>example.com/</span></span></div>
    <div class="page tall"><div class="plain"><div class="pp-h1">Welcome</div><p>This page has no styles yet.</p><a>About us</a></div></div>
    <div class="chip needs" id="i1-needs">${ICON.file} needs style.css</div>
  </div>
  <svg class="ov" id="i1-arrows" viewBox="0 0 1600 900" width="1600" height="900" aria-hidden="true">
    <path id="i1-arrow1" class="arr" d="M 608 450 L 772 450"/>
    <path id="i1-head1" class="arrh" d="${head(780, 450, 1, 0)}"/>
    <path id="i1-arrow2" class="arr dash" d="M 1124 450 L 1294 450"/>
    <path id="i1-head2" class="arrh dashh" d="${head(1302, 450, 1, 0)}"/>
  </svg>
  <div class="lbl" id="i1-lbl1" style="${P(595, 414, 190)}">1 · check here</div>
  <div class="card cachebox" id="i1-cache" style="${P(796, 356, 304, 188)}">
    <div class="chip up dlabel">HTTP cache</div>
    <div class="minislot">copies of files</div>
    <div class="lens small" id="i1-lens">${ICON.lens}</div>
  </div>
  <div class="lbl muted" id="i1-lbl2" style="${P(1118, 414, 190)}">2 · only if needed</div>
  <div class="card serverbox" id="i1-server" style="${P(1322, 340, 230, 220)}">
    <div class="chip up slabel">Server</div>
    <div class="rack mini">${unit()}${unit()}${unit()}</div>
  </div>
  <div class="card question" id="i2-q" style="${P(300, 352, 1000, 200)}">
    <div class="q-text">Do I have a ${acc('fresh')} copy?</div>
    <div class="q-sub" id="i2-sub">The browser asks this for every file, before it calls the server.</div>
  </div>
</section>`;
}

function intro3() {
  const card = (n, l, title, chip, cls, slot, when) => `
  <div class="card reqcard ${cls}" id="i3-c${n}" style="${P(l, 236, 480, 380)}">
    <div class="chip rq-when">${when}</div>
    <div class="rq-num">${n}</div>
    <div class="rq-title">${title}</div>
    <div class="chip rq-chip">${chip}</div>
    <div class="minislot rq-slot">${slot}</div>
  </div>`;
  return `<section class="scene" id="sc-intro3">
  <h2 class="headline" id="h-i3">One file. ${acc('Three')} requests.</h2>
  <div class="chip filechip" id="i3-file">${ICON.file} style.css</div>
  ${card(1, 48, 'First visit', 'no copy yet', 'k-miss', 'empty', 'at 0:00')}
  ${card(2, 560, 'Cache hit', 'fresh copy', 'k-hit', `<span class="mini-copy fresh">${ICON.file} style.css</span>`, '10 min later')}
  ${card(3, 1072, 'Expired', 'stale copy', 'k-stale', `<span class="mini-copy stale">${ICON.file} style.css</span>`, '2 hours later')}
</section>`;
}

function mainScene() {
  const h = (id, html) => `<h2 class="headline" id="h-${id}">${html}</h2>`;
  const row = (n, top, status, size, note, cls, dim) => `
    <div class="dt-grid dt-row" id="row${n}" style="top:${top}px">
      <span class="flash" id="row${n}-flash"></span>
      <span>style.css</span><span class="${dim ? 'dimc' : ''}">${status}</span><span>stylesheet</span><span class="${dim ? 'dimc' : ''}">${size}</span>
      <span class="notecell"><span class="chip up note ${cls}" id="note${n}">${note}</span></span>
    </div>`;
  // Freshness timeline: 0 to 3 hours across 556 px.
  const X = (hours) => +(48 + hours * 180).toFixed(2);
  const tick = (hrs, label) =>
    `<line x1="${X(hrs)}" y1="124" x2="${X(hrs)}" y2="134" stroke="${INK}" stroke-width="2"/><text x="${X(hrs)}" y="156" class="tk">${label}</text>`;
  const marker = (n, hrs) =>
    `<g id="mk${n}"><circle cx="${X(hrs)}" cy="66" r="14" fill="${INK}"/><text x="${X(hrs)}" y="71.5" class="mk">${n}</text></g>`;

  return `<section class="scene" id="sc-main">
  ${h('r1a', `Cache ${acc('miss')}. Nothing stored yet.`)}
  ${h('r1c', `The server sends the ${acc('whole')} file.`)}
  ${h('r1f', `The browser ${acc('saves')} a copy.`)}
  ${h('r2a', `Is the copy still ${acc('fresh')}?`)}
  ${h('r2b', `600 s old. Still ${acc('fresh')}.`)}
  ${h('r2c', `Cache ${acc('hit')}. No request sent.`)}
  ${h('r2e', `DevTools still ${acc('lists')} it.`)}
  ${h('r3a', `Two hours ${acc('later')}.`)}
  ${h('r3b', `${acc('Stale')}, but still stored.`)}
  ${h('r3c', `Ask the ${acc('server')} first.`)}
  ${h('r3e', `${acc('Not')} modified. Keep the copy.`)}
  ${h('r3f', `Headers only. ${acc('Tiny')}.`)}
  ${h('r3g', `Fresh ${acc('again')} for an hour.`)}
  ${h('r3i', `What if it ${acc('changed')}?`)}

  <div class="card browser" id="browser" style="${P(48, 160, 480, 388)}">
    <div class="titlebar"><span class="dots"><i></i><i></i><i></i></span>
      <span class="addr"><span id="addr-root">example.com/</span><span id="addr-about">example.com/about</span></span></div>
    <div class="page">
      <div class="plain"><div class="pp-h1">Welcome</div><p>This page has no styles yet.</p><a>About us</a></div>
      <div class="styled" id="page-styled"><div class="nav"><i class="logo"></i><span>Home</span><span>About</span></div><div class="sh1">Welcome</div><span class="sbtn">Read more</span></div>
    </div>
    <div class="chip needs" id="needs">${ICON.file} needs style.css</div>
    <div class="drawer">
      <div class="chip up dlabel">HTTP cache</div>
      <div class="chip up kept" id="kept">still stored</div>
      <div class="slot" id="slot-empty">empty</div>
      <div class="copy" id="copy">
        <div class="c-row1">${ICON.file}<span>style.css</span></div>
        <div class="c-state" id="copy-state"><span class="chip up st-fresh" id="state-fresh">fresh</span><span class="chip up st-stale" id="state-stale">stale</span></div>
        <div class="c-row2"><span class="c-etag"><span class="hlbg" id="copy-etag-hl"></span><span class="t">ETag "a3f9"</span></span><span>max-age=3600</span></div>
        <div class="c-row3"><span class="age-lbl">age</span><span class="age-num" id="age">0 s</span><span class="meter"><i id="meter"></i></span></div>
      </div>
      <div class="lens" id="lens">${ICON.lens}</div>
      <div class="stamp miss" id="st-miss">MISS</div>
    </div>
  </div>
  <div class="ghost" id="ghost" style="${P(188, 449, 200, 48)}">${ICON.file}<span>style.css</span></div>
  <div class="ping" id="copy-ping" style="${P(59, 403, 458, 140)}"></div>

  <svg class="lanes" id="lanes" style="${P(528, 160, 652, 388)}" viewBox="0 0 652 388" width="652" height="388" aria-hidden="true">
    <g id="lane-req"><path class="lane" d="M 14 40 H 628"/><path class="laneh" d="${head(638, 40, 1, 0, 12)}"/></g>
    <g id="lane-res"><path class="lane" d="M 638 352 H 24"/><path class="laneh" d="${head(14, 352, -1, 0, 12)}"/></g>
  </svg>
  <div class="lane-lbl" id="lbl-req" style="${P(548, 214)}">request</div>
  <div class="lane-lbl" id="lbl-res" style="${P(552, 524)}">response</div>

  <div class="card server" id="server" style="${P(1180, 160, 372, 388)}">
    <div class="chip up slabel">Server</div><span class="host">example.com</span>
    <div class="rack">${unit()}${unit()}${unit()}</div>
    <div class="srvfile">${ICON.file}<span>style.css</span><span class="chip etag" id="srv-etag">ETag "a3f9"</span></div>
    <div class="srvcmp" id="srv-cmp"><span class="chip tagq">"a3f9"</span>${ICON.eq}<span class="chip">"a3f9"</span><span class="chip up ok">${ICON.check}match</span></div>
  </div>
  <div class="ping" id="srv-ping" style="${P(1180, 160, 372, 388)}"></div>

  <div class="card msg req" id="req1" style="${P(540, 157, 290, 86)}">
    <div class="m-row1"><span class="chip verb">GET</span><span class="path">/style.css</span></div>
    <div class="m-row2"><span class="t">Host: example.com</span></div>
  </div>
  <div class="card msg res" id="res1" style="${P(634, 222, 440, 284)}">
    <div class="m-status"><span class="proto">HTTP/1.1</span><span class="chip s200">200 OK</span></div>
    <div class="m-hdr" id="res1-h1"><span class="hl" id="res1-hl1"></span><span class="t">Cache-Control: max-age=3600</span></div>
    <div class="m-hdr" id="res1-h2"><span class="hl" id="res1-hl2"></span><span class="t">ETag: "a3f9"</span></div>
    <div class="m-body" id="res1-body"><div class="css">body { font: 16px/1.5 serif; }<br>.nav { display: flex; gap: 1rem; }<br>h1 { font-size: 2rem; }</div><span class="chip size">48.2 kB · the whole file</span></div>
  </div>
  <div class="co-wrap" id="co-maxage" style="${P(594, 164, 520, 48)}"><div class="callout">fresh for 3600 s = 1 hour</div></div>
  <div class="co-wrap" id="co-etag" style="${P(594, 164, 520, 48)}"><div class="callout">${ICON.tag}<span><span class="mono">"a3f9"</span> names this exact version</span></div></div>

  <div class="skip" id="skip10" style="${P(654, 254, 400, 140)}"><div class="card skip-pill">${ICON.clock('hand10')}<div><div class="skip-big">+10 min</div><div class="skip-sub">since the first visit</div></div></div></div>
  <div class="cmp-wrap" id="cmp-fresh" style="${P(654, 206, 400, 270)}"><div class="card cmp">
    <div class="cmp-row"><div class="cv"><div class="cl">age</div><div class="cn">600 s</div></div><div class="op">&lt;</div><div class="cv"><div class="cl">max-age</div><div class="cn">3600 s</div></div></div>
    <div class="cmp-foot"><span class="stamp fresh" id="cmp-fresh-stamp">FRESH</span></div></div></div>
  <div class="nonet-wrap" id="nonet" style="${P(654, 172, 400, 56)}"><div class="nonet">${ICON.noentry}<span>no request sent</span></div></div>
  <div class="hit-wrap" id="st-hit" style="${P(654, 250, 400, 100)}"><span class="stamp hit big">CACHE HIT</span></div>
  <div class="hit-chips" id="hit-chips" style="${P(594, 372, 520, 40)}"><span class="chip" id="hit-c1">0 round trips</span><span class="chip" id="hit-c2">0 bytes downloaded</span></div>

  <div class="skip" id="skip2h" style="${P(654, 254, 400, 140)}"><div class="card skip-pill">${ICON.clock('hand2h')}<div><div class="skip-big">+2 h</div><div class="skip-sub">since the first visit</div></div></div></div>
  <div class="cmp-wrap" id="cmp-stale" style="${P(654, 206, 400, 270)}"><div class="card cmp">
    <div class="cmp-row"><div class="cv"><div class="cl">age</div><div class="cn">7200 s</div></div><div class="op">&gt;</div><div class="cv"><div class="cl">max-age</div><div class="cn">3600 s</div></div></div>
    <div class="cmp-foot"><span class="stamp stale" id="cmp-stale-stamp">STALE</span></div></div></div>

  <div class="card msg req" id="req3" style="${P(540, 157, 290, 86)}">
    <div class="m-row1"><span class="chip verb">GET</span><span class="path">/style.css</span></div>
    <div class="m-row2"><span class="hl" id="req3-hl"></span><span class="t">If-None-Match: "a3f9"</span></div>
  </div>
  <div class="bubble" id="bubble" style="${P(878, 266, 290, 64)}">Changed since <span class="mono">"a3f9"</span>?</div>
  <div class="card msg res r304" id="res3" style="${P(654, 222, 400, 222)}">
    <div class="m-status"><span class="proto">HTTP/1.1</span><span class="chip s304">304 Not Modified</span></div>
    <div class="m-hdr"><span class="t">Cache-Control: max-age=3600</span></div>
    <div class="m-hdr"><span class="t">ETag: "a3f9"</span></div>
    <div class="nobody" id="res3-nobody">no body</div>
  </div>
  <div class="card sizes" id="sizes" style="${P(654, 456, 400, 84)}">
    <div class="sz-row"><span class="sz-l">200</span><span class="sz-track"><i class="b200" id="size-bar200"></i></span><span class="sz-v">48.2 kB</span></div>
    <div class="sz-row"><span class="sz-l">304</span><span class="sz-track"><i class="b304" id="size-bar304"></i></span><span class="sz-v">0.3 kB</span></div>
  </div>

  <div class="card inset" id="inset" style="${P(560, 160, 992, 394)}">
    <div class="chip up in-chip">New version on the server</div>
    <div class="in-col" id="in-a" style="${P(28, 104, 270)}">
      <div class="in-lbl">Browser sends</div>
      <div class="chip in-req">If-None-Match: "a3f9"</div>
      <div class="in-note">its stored version</div>
    </div>
    <div class="in-col" id="in-b" style="${P(350, 104, 290)}">
      <div class="in-lbl">Server has</div>
      <div class="chip in-new">ETag "b71c"</div>
      <div class="in-cmp"><span class="chip">"a3f9"</span>${ICON.neq}<span class="chip">"b71c"</span></div>
      <div class="chip up bad">${ICON.cross}no match</div>
    </div>
    <div class="in-col" id="in-c" style="${P(690, 104, 270)}">
      <div class="in-lbl">Server replies</div>
      <div class="in-res">
        <span class="chip s200">200 OK</span>
        <div class="in-hdr">ETag: "b71c"</div>
        <div class="in-body">new file · full size</div>
      </div>
    </div>
    <svg class="in-arrows" viewBox="0 0 992 394" width="992" height="394" aria-hidden="true">
      <path id="in-ar1" class="arr" d="M 300 154 H 340"/><path id="in-ah1" class="arrh" d="${head(346, 154, 1, 0, 11)}"/>
      <path id="in-ar2" class="arr" d="M 642 154 H 680"/><path id="in-ah2" class="arrh" d="${head(686, 154, 1, 0, 11)}"/>
    </svg>
  </div>

  <div class="card devtools" id="devtools" style="${P(48, 572, 852, 198)}">
    <div class="dt-bar"><b>DevTools</b><span>Elements</span><span>Console</span><span class="on">Network</span></div>
    <div class="dt-grid dt-head"><span>Name</span><span>Status</span><span>Type</span><span>Size</span><span></span></div>
    <div class="dt-empty" id="dt-empty">Recording network activity...</div>
    ${row(1, 76, '200', '48.2 kB', 'full download', 'n-miss', false)}
    ${row(2, 114, '200', '(disk cache)', 'cache hit', 'n-hit', true)}
    ${row(3, 152, '304', '0.3 kB', 'revalidated', 'n-reval', false)}
    <span class="cellring" id="row2-ring"></span>
  </div>

  <div class="card tline" id="tline" style="${P(932, 572, 620, 198)}">
    <div class="tl-head"><span class="chip up">Freshness</span><span class="lg"><i class="sw s-fresh"></i>fresh</span><span class="lg"><i class="sw s-stale"></i>stale</span><span class="tl-ma">max-age=3600</span></div>
    <svg class="tl-svg" viewBox="0 0 614 192" width="614" height="192" aria-hidden="true">
      <rect x="48" y="92" width="540" height="30" rx="6" fill="#efe7d4" stroke="rgba(22,21,19,.3)" stroke-width="1.5"/>
      <rect id="band-stale" x="${X(1)}" y="92" width="0" height="30" fill="#f0c860" stroke="${INK}" stroke-width="2"/>
      <rect id="band-fresh1" x="${X(0)}" y="92" width="${(X(1) - X(0)).toFixed(2)}" height="30" rx="6" fill="#a9c79b" stroke="${INK}" stroke-width="2"/>
      <rect id="band-fresh2" x="${X(2)}" y="92" width="${(X(3) - X(2)).toFixed(2)}" height="30" rx="6" fill="#a9c79b" stroke="${INK}" stroke-width="2"/>
      <text id="band-fresh1-lbl" x="${(X(1) - 10).toFixed(1)}" y="112.5" class="bl end">fresh for 1 h</text>
      <text id="band-stale-lbl" x="${((X(1) + X(2)) / 2).toFixed(1)}" y="112.5" class="bl">stale</text>
      <text id="band-fresh2-lbl" x="${((X(2) + X(3)) / 2).toFixed(1)}" y="112.5" class="bl">fresh again</text>
      <line x1="${X(0)}" y1="129" x2="${X(3)}" y2="129" stroke="${INK}" stroke-width="2.5"/>
      ${tick(0, '0')}${tick(1, '1 h')}${tick(2, '2 h')}${tick(3, '3 h')}
      ${marker(1, 0)}${marker(2, 1 / 6)}${marker(3, 2)}
      <g id="cursor"><line x1="0" y1="86" x2="0" y2="129" stroke="${INK}" stroke-width="3"/><path d="M -8 80 L 8 80 L 0 90 Z" fill="${INK}"/></g>
    </svg>
  </div>
</section>`;
}

function recap() {
  const col = (n, l, title, sub, flow, dt, cls) => `
  <div class="card rcol ${cls}" id="rc${n}" style="${P(l, 168, 480, 330)}">
    <div class="rc-top"><span class="rc-num">${n}</span><div><div class="rc-title">${title}</div><div class="rc-sub">${sub}</div></div></div>
    <div class="rc-flow">${flow}</div>
    <div class="rc-dt">${dt}</div>
  </div>
  <div class="rc-ring" id="rc${n}-ring" style="${P(l - 10, 158, 500, 350)}"></div>`;
  const out = (t) => `<div class="fl">${ICON.right}<span>${t}</span></div>`;
  const back = (t) => `<div class="fl">${ICON.left}<span>${t}</span></div>`;
  return `<section class="scene" id="sc-recap">
  <h2 class="headline" id="h-c1">Three requests, three ${acc('outcomes')}.</h2>
  <h2 class="headline" id="h-c2">Your header picks the ${acc('path')}.</h2>
  ${col(1, 48, 'No copy', 'first visit', out('GET /style.css') + back('200 OK + the whole file'), '<span>200</span><span>48.2 kB</span>', 'k-miss')}
  ${col(2, 560, 'Fresh copy', 'cache hit', `<div class="fl">${ICON.noentry}<span>no request sent</span></div><div class="fl">${ICON.check}<span>the stored copy is used</span></div>`, '<span>200</span><span class="dimc">(disk cache)</span>', 'k-hit')}
  ${col(3, 1072, 'Stale copy', 'expired', out('If-None-Match: "a3f9"') + back('304, headers only'), '<span>304</span><span>0.3 kB</span>', 'k-stale')}
  <svg class="ov" viewBox="0 0 1600 900" width="1600" height="900" aria-hidden="true">
    <path id="ar-nostore" class="arr" d="M 288 600 V 518"/><path id="ah-nostore" class="arrh" d="${head(288, 510, 0, -1)}"/>
    <path id="ar-max2" class="arr" d="M 800 600 V 518"/><path id="ah-max2" class="arrh" d="${head(800, 510, 0, -1)}"/>
    <path id="ar-max3" class="arr" d="M 952 640 C 1060 640, 1150 610, 1176 520"/><path id="ah-max3" class="arrh" d="${head(1178, 512, 0.29, -0.96)}"/>
    <path id="ar-nocache" class="arr" d="M 1312 600 V 518"/><path id="ah-nocache" class="arrh" d="${head(1312, 510, 0, -1)}"/>
  </svg>
  <div class="lb" id="lb-max2" style="${P(812, 538)}">first hour</div>
  <div class="lb" id="lb-max3" style="${P(985, 570)}">after that</div>
  <div class="hchip" id="ch-nostore" style="${P(128, 604, 320)}"><span class="chip big">no-store</span><span class="hsub">saves nothing</span></div>
  <div class="hchip" id="ch-maxage" style="${P(640, 604, 320)}"><span class="chip big">max-age=3600</span><span class="hsub">fresh for 1 hour</span></div>
  <div class="hchip" id="ch-nocache" style="${P(1152, 604, 320)}"><span class="chip big">no-cache</span><span class="hsub">stores, but always checks</span></div>
</section>`;
}

function tip() {
  return `<section class="scene" id="sc-tip">
  <h2 class="headline" id="h-c5">${acc('Replay')} a first visit.</h2>
  <div class="card tipdt" id="tip-dt" style="${P(160, 196, 1280, 300)}">
    <div class="dt-bar"><b>DevTools</b><span>Elements</span><span>Console</span><span class="on">Network</span></div>
    <div class="tip-tools">
      <span class="rec"></span><span class="sep"></span>
      <span class="filter">Filter</span><span class="sep"></span>
      <span class="cb"><span class="box"></span>Preserve log</span>
      <span class="cb target"><span class="box" id="tip-box"><svg viewBox="0 0 24 24" width="24" height="24" aria-hidden="true"><path id="tip-check" d="M5 12.5l4.5 4.5L19 7.5" fill="none" stroke="${INK}" stroke-width="3.4" stroke-linecap="round" stroke-linejoin="round"/></svg></span>Disable cache</span>
      <span class="sep"></span><span class="throttle">No throttling</span>
    </div>
    <div class="dt-grid dt-head wide"><span>Name</span><span>Status</span><span>Type</span><span>Size</span><span></span></div>
    <div class="dt-grid dt-row wide" id="tip-row" style="top:174px"><span>style.css</span><span>200</span><span>stylesheet</span><span>48.2 kB</span><span class="notecell"><span class="chip up note n-miss">full download</span></span></div>
  </div>
  <div class="pointer" id="pointer" style="${P(696, 268)}">${ICON.pointer}</div>
  <div class="tip-note" id="tip-note" style="${P(160, 546, 1280)}">DevTools disables the browser cache, so every request acts like a first visit.</div>
</section>`;
}

function final() {
  const tile = (n, l, chip, big, sub, cls) => `
  <div class="card fin ${cls}" id="fin${n}" style="${P(l, 196, 480, 330)}">
    <span class="chip up fin-chip">${chip}</span>
    <div class="fin-big">${big}</div>
    <div class="fin-sub">${sub}</div>
  </div>`;
  return `<section class="scene" id="sc-final">
  <h2 class="headline" id="h-c6">What each request ${acc('costs')}.</h2>
  ${tile(1, 48, 'Fresh hit', 'No network', '0 requests sent', 'k-hit')}
  ${tile(2, 560, '304 Not Modified', 'One small round trip', 'headers only, no body', 'k-stale')}
  ${tile(3, 1072, '200 OK', 'The whole file', 'the full body, 48.2 kB here', 'k-miss')}
  <div class="fin-foot" id="fin-foot" style="${P(48, 574, 1504)}">In DevTools, the Status and Size columns show which one you got.</div>
</section>`;
}

function poster(totalLabel) {
  return `<div class="poster" id="poster">
  <div class="p-chips"><span class="chip up">Interactive explainer</span><span class="chip">${totalLabel}</span><span class="chip up">Captions on</span></div>
  <h1 class="p-title">How browser ${acc('caching')} works</h1>
  <p class="p-sub">Follow one file through three requests: a first visit, a cache hit, and an expired copy.</p>
  <div class="p-hint">Space plays and pauses. Arrow keys step through each request.</div>
  <button class="p-play" id="poster-play" type="button" aria-label="Play the explainer">${ICON.play}</button>
</div>`;
}

function stageHTML({ chapters, totalLabel }) {
  return `<div class="bg-grid"></div>
${chrome(chapters)}
${intro1()}
${intro3()}
${mainScene()}
${recap()}
${tip()}
${final()}
<div class="caption" id="caption"><span id="captext"></span></div>
${poster(totalLabel)}`;
}

module.exports = { stageHTML };
