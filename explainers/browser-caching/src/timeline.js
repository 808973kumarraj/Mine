/* Scene timeline. Every tween sits at a beat's start T(id), its end E(id), or a fraction of
   its spoken part F(id, f). No callbacks: values that follow the clock (the copy's age, the
   freshness cursor) are computed from time in derive(), so scrubbing in either direction is safe. */
(function () {
  const B = {};
  BEATS.forEach((b) => { B[b.id] = b; });
  const T = (id) => B[id].start;
  const E = (id) => B[id].end;
  const F = (id, f) => B[id].start + B[id].dur * f;
  const RM = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const D = RM ? 0.35 : 1;
  const POP = RM ? 'power1.out' : 'back.out(1.7)';
  const tl = gsap.timeline({ paused: true, defaults: { ease: 'power2.out' } });

  const el = (id) => {
    const n = document.getElementById(id);
    if (!n) throw new Error('timeline: missing #' + id);
    return n;
  };
  const els = (ids) => [].concat(ids).map(el);

  // Motion helpers. First appearance uses fromTo; later changes to the same element use to.
  function land(id, at, o) {
    tl.fromTo(el(id), { autoAlpha: 0, scale: 0.72 }, Object.assign({ autoAlpha: 1, scale: 1, duration: 0.5 * D, ease: POP }, o), at);
  }
  function rise(ids, at, o) {
    tl.fromTo(els(ids), { autoAlpha: 0, y: 22 }, Object.assign({ autoAlpha: 1, y: 0, duration: 0.5 * D, stagger: 0.09 * D }, o), at);
  }
  function fade(ids, at, d, to) {
    tl.fromTo(els(ids), { autoAlpha: 0 }, { autoAlpha: to == null ? 1 : to, duration: (d || 0.4) * D, ease: 'none' }, at);
  }
  function show(ids, at, d) { tl.to(els(ids), { autoAlpha: 1, duration: (d || 0.35) * D, ease: 'none' }, at); }
  function exit(ids, at, d) { tl.to(els(ids), { autoAlpha: 0, duration: (d || 0.3) * D, ease: 'none' }, at); }
  function dim(ids, at, v) { tl.to(els(ids), { opacity: v, duration: 0.35 * D, ease: 'none' }, at); }
  function draw(id, at, d) {
    const p = el(id);
    const len = Math.ceil(p.getTotalLength()) + 2;
    tl.fromTo(p, { strokeDasharray: len, strokeDashoffset: len }, { strokeDashoffset: 0, duration: (d || 0.6) * D, ease: 'power1.inOut' }, at);
  }
  function grow(id, at, d, origin) {
    tl.fromTo(el(id), { scaleX: 0, transformOrigin: origin || '0% 50%' }, { scaleX: 1, duration: (d || 0.6) * D, ease: 'power2.inOut' }, at);
  }
  // The page gets its styles: a top-to-bottom wipe, so the plain and styled pages never overlap.
  function wipeIn(id, at, first) {
    const n = el(id);
    const from = { autoAlpha: 1, clipPath: 'inset(0% 0% 100% 0%)' };
    const to = { clipPath: 'inset(0% 0% 0% 0%)', duration: 0.55 * D, ease: 'power2.inOut' };
    if (first) tl.fromTo(n, from, to, at);
    else {
      tl.set(n, from, at);
      tl.to(n, to, at);
    }
  }
  function shrink(id, at) { tl.to(el(id), { scaleX: 0, duration: 0.3 * D, ease: 'power2.in' }, at); }
  function pulse(id, at, s) { tl.to(el(id), { scale: s || 1.12, duration: 0.18 * D, yoyo: true, repeat: 1, ease: 'power1.inOut' }, at); }
  function ping(id, at) {
    tl.fromTo(el(id), { autoAlpha: 0, scale: 1 }, { autoAlpha: 1, duration: 0.08, ease: 'none' }, at);
    tl.to(el(id), { autoAlpha: 0, scale: 1.05, duration: 0.6 * D, ease: 'power1.out' }, at + 0.08);
  }
  function flash(id, at) {
    tl.fromTo(el(id), { autoAlpha: 0 }, { autoAlpha: 0.75, duration: 0.15, ease: 'none' }, at);
    tl.to(el(id), { autoAlpha: 0, duration: 0.9 * D, ease: 'power1.in' }, at + 0.35);
  }
  function spin(id, t0, t1, turns) {
    tl.fromTo(el(id), { rotation: 0, svgOrigin: '28 28' }, { rotation: 360 * turns, duration: t1 - t0, ease: 'sine.inOut' }, t0);
  }
  // A scene fades in just before its first beat and out just before the next scene's first beat.
  function scene(id, first, next) {
    tl.fromTo(el(id), { autoAlpha: 0 }, { autoAlpha: 1, duration: 0.3 * D, ease: 'none' }, Math.max(0, T(first) - 0.15));
    if (next) tl.to(el(id), { autoAlpha: 0, duration: 0.3 * D, ease: 'none' }, T(next) - 0.3);
  }
  function head(id, from, until) {
    tl.fromTo(el(id), { autoAlpha: 0, y: 14 }, { autoAlpha: 1, y: 0, duration: 0.4 * D }, T(from) + 0.02);
    if (until) tl.to(el(id), { autoAlpha: 0, duration: 0.22 * D, ease: 'none' }, T(until) - 0.26);
  }
  const SVGC = { transformOrigin: '50% 50%' };

  // Chapter chips (chrome).
  CHAPTERS.forEach((c, i) => {
    const first = BEATS.find((b) => b.ch === c.id);
    const next = CHAPTERS[i + 1] && BEATS.find((b) => b.ch === CHAPTERS[i + 1].id);
    tl.fromTo(el('cc-' + c.id), { autoAlpha: 0, y: -8 }, { autoAlpha: 1, y: 0, duration: 0.3 * D }, Math.max(0, first.start - 0.15));
    if (next) tl.to(el('cc-' + c.id), { autoAlpha: 0, duration: 0.2 * D, ease: 'none' }, next.start - 0.22);
  });

  // ---------- Intro: cache first, the question ----------
  scene('sc-intro1', 'i1', 'i3');
  head('h-i1', 'i1', 'i2');
  rise('i1-browser', T('i1') + 0.05);
  land('i1-needs', T('i1') + 0.3);
  draw('i1-arrow1', F('i1', 0.3), 0.5);
  fade('i1-head1', F('i1', 0.3) + 0.42, 0.12);
  rise('i1-lbl1', F('i1', 0.32));
  rise('i1-cache', F('i1', 0.4));
  tl.fromTo(el('i1-lens'), { autoAlpha: 0, x: -60 }, { autoAlpha: 1, x: 40, duration: 1.2 * D, ease: 'power1.inOut' }, F('i1', 0.55));
  exit('i1-lens', F('i1', 0.55) + 1.35, 0.3);
  fade(['i1-arrow2', 'i1-head2'], F('i1', 0.72), 0.35);
  rise('i1-lbl2', F('i1', 0.74));
  rise('i1-server', F('i1', 0.76), { autoAlpha: 0.55 });

  dim(['i1-browser', 'i1-cache', 'i1-server', 'i1-arrows', 'i1-lbl1', 'i1-lbl2'], T('i2'), 0.14);
  land('i2-q', T('i2') + 0.08);
  rise('i2-sub', F('i2', 0.5));

  // ---------- Intro: three requests ----------
  scene('sc-intro3', 'i3', 'r1a');
  head('h-i3', 'i3');
  land('i3-file', T('i3') + 0.12);
  rise(['i3-c1', 'i3-c2', 'i3-c3'], T('i3') + 0.06, { stagger: 0.16 * D });

  // ---------- Main diagram: three requests ----------
  const REQ_DX = 338;                    // request card: from the browser edge to the server edge
  const RES1 = { x: -566, y: 109 };      // 200 response center -> stored copy center
  const RES3 = { x: -566, y: 140 };      // 304 response center -> stored copy center
  const GHOST_DY = -186;                 // stored copy -> page
  const segA = { t0: F('r2a', 0.15), t1: F('r2a', 0.85), a0: 0, a1: 600, c0: 0, c1: 600 };
  const segB = { t0: F('r3a', 0.15), t1: F('r3a', 0.82), a0: 600, a1: 7200, c0: 600, c1: 7200 };
  const segC = { t0: F('r3g', 0.3), t1: F('r3g', 0.55), a0: 7200, a1: 0, c0: 7200, c1: 7200 };

  scene('sc-main', 'r1a', 'c1');
  fade('examples', T('r1a'), 0.4);

  // Request 1: first visit
  head('h-r1a', 'r1a', 'r1c');
  rise('browser', T('r1a') + 0.02);
  rise('server', T('r1a') + 0.12);
  grow('lane-req', T('r1a') + 0.3, 0.6, '0% 50%');
  grow('lane-res', T('r1a') + 0.4, 0.6, '100% 50%');
  rise(['lbl-req', 'lbl-res'], T('r1a') + 0.5);
  rise(['devtools', 'tline'], T('r1a') + 0.3, { stagger: 0.12 * D });
  land('needs', F('r1a', 0.22));
  tl.fromTo(el('lens'), { autoAlpha: 0, x: -40 }, { autoAlpha: 1, x: 170, duration: 1.1 * D, ease: 'power1.inOut' }, F('r1a', 0.3));
  exit('lens', F('r1a', 0.3) + 1.15, 0.25);
  land('st-miss', F('r1a', 0.62), { rotation: -6 });

  tl.fromTo(el('req1'), { autoAlpha: 0, x: -24 }, { autoAlpha: 1, x: 0, duration: 0.3 * D }, T('r1b'));
  tl.to(el('req1'), { x: REQ_DX, duration: 1.1 * D, ease: 'power1.inOut' }, T('r1b') + 0.3);
  ping('srv-ping', T('r1b') + 1.4);

  head('h-r1c', 'r1c', 'r1f');
  exit('st-miss', T('r1c'));
  tl.to(el('req1'), { autoAlpha: 0, x: REQ_DX + 40, scale: 0.8, duration: 0.35 * D }, T('r1c'));
  tl.fromTo(el('res1'), { autoAlpha: 0, x: 120, scale: 0.94 }, { autoAlpha: 1, x: 0, scale: 1, duration: 0.55 * D }, T('r1c') + 0.15);
  rise(['res1-h1', 'res1-h2', 'res1-body'], T('r1c') + 0.75, { stagger: 0.25 * D });

  grow('res1-hl1', T('r1d') + 0.05, 0.45);
  land('co-maxage', F('r1d', 0.22));
  grow('band-fresh1', F('r1d', 0.45), 0.9);
  fade('band-fresh1-lbl', F('r1d', 0.45) + 0.8, 0.3);

  shrink('res1-hl1', T('r1e'));
  exit('co-maxage', T('r1e'));
  grow('res1-hl2', T('r1e') + 0.1, 0.45);
  land('co-etag', F('r1e', 0.22));
  pulse('srv-etag', F('r1e', 0.5));

  head('h-r1f', 'r1f', 'r2a');
  exit('co-etag', T('r1f'));
  shrink('res1-hl2', T('r1f'));
  wipeIn('page-styled', T('r1f') + 0.25, true);
  exit('needs', T('r1f') + 0.25);
  tl.to(el('res1'), { x: RES1.x, y: RES1.y, scale: 0.3, autoAlpha: 0, duration: 0.9 * D, ease: 'power2.inOut' }, T('r1f') + 0.35);
  exit('slot-empty', T('r1f') + 0.8);
  land('copy', T('r1f') + 1.0);
  land('mk1', F('r1f', 0.6), SVGC);
  fade('cursor', F('r1f', 0.6), 0.3);
  pulse('age', F('r1f', 0.8), 1.25);

  exit('dt-empty', T('r1g'));
  rise('row1', T('r1g') + 0.1);
  flash('row1-flash', T('r1g') + 0.1);
  land('note1', T('r1g') + 0.5);

  // Request 2: cache hit
  head('h-r2a', 'r2a', 'r2b');
  land('skip10', T('r2a') + 0.05);
  spin('hand10', segA.t0, segA.t1, 2);
  exit(['addr-root', 'page-styled'], T('r2a') + 0.08, 0.12);
  fade('addr-about', T('r2a') + 0.2, 0.15);
  show('needs', T('r2a') + 0.5);
  land('mk2', segA.t1, SVGC);
  exit('skip10', T('r2b') - 0.3);

  head('h-r2b', 'r2b', 'r2c');
  land('cmp-fresh', T('r2b') + 0.05);
  land('cmp-fresh-stamp', F('r2b', 0.55), { rotation: -4 });
  pulse('copy-state', F('r2b', 0.6));
  exit('cmp-fresh', T('r2c'));

  head('h-r2c', 'r2c', 'r2e');
  tl.fromTo(el('ghost'), { autoAlpha: 0, y: 0, scale: 0.6 }, { autoAlpha: 1, scale: 1, duration: 0.3 * D, ease: POP }, T('r2c') + 0.15);
  tl.to(el('ghost'), { y: GHOST_DY, duration: 0.65 * D, ease: 'power2.inOut' }, T('r2c') + 0.45);
  tl.to(el('ghost'), { autoAlpha: 0, duration: 0.2 * D, ease: 'none' }, T('r2c') + 1.1);
  wipeIn('page-styled', T('r2c') + 1.0);
  exit('needs', T('r2c') + 1.0);
  dim(['lanes', 'lbl-req', 'lbl-res', 'server'], F('r2c', 0.45), 0.3);
  land('nonet', F('r2c', 0.5));

  land('st-hit', T('r2d') + 0.05, { rotation: -4 });
  rise(['hit-c1', 'hit-c2'], F('r2d', 0.42));

  head('h-r2e', 'r2e', 'r3a');
  rise('row2', T('r2e') + 0.1);
  flash('row2-flash', T('r2e') + 0.1);
  land('note2', T('r2e') + 0.5);
  fade('row2-ring', F('r2e', 0.55), 0.3);

  // Request 3: expired
  head('h-r3a', 'r3a', 'r3b');
  exit(['st-hit', 'hit-c1', 'hit-c2', 'nonet', 'row2-ring'], T('r3a'));
  dim(['lanes', 'lbl-req', 'lbl-res', 'server'], T('r3a') + 0.1, 1);
  land('skip2h', T('r3a') + 0.08);
  spin('hand2h', segB.t0, segB.t1, 4);
  exit(['addr-about', 'page-styled'], T('r3a') + 0.08, 0.12);
  show('addr-root', T('r3a') + 0.2, 0.15);
  show('needs', T('r3a') + 0.5);
  land('mk3', segB.t1, SVGC);
  fade('band-stale-lbl', segB.t1, 0.3);
  exit('skip2h', T('r3b') - 0.3);

  head('h-r3b', 'r3b', 'r3c');
  land('cmp-stale', T('r3b') + 0.05);
  land('cmp-stale-stamp', F('r3b', 0.4), { rotation: -4 });
  pulse('copy-state', F('r3b', 0.45));
  land('kept', F('r3b', 0.62));
  exit('cmp-stale', T('r3c'));

  head('h-r3c', 'r3c', 'r3e');
  fade('copy-etag-hl', T('r3c') + 0.1, 0.25);
  tl.fromTo(el('req3'), { autoAlpha: 0, x: -24 }, { autoAlpha: 1, x: 0, duration: 0.3 * D }, T('r3c') + 0.4);
  grow('req3-hl', T('r3c') + 0.75, 0.4);
  tl.to(el('req3'), { x: REQ_DX, duration: 1.2 * D, ease: 'power1.inOut' }, F('r3c', 0.45));
  ping('srv-ping', F('r3c', 0.45) + 1.25);

  land('bubble', T('r3d') + 0.05);
  pulse('srv-etag', F('r3d', 0.55));

  head('h-r3e', 'r3e', 'r3f');
  exit(['bubble', 'copy-etag-hl'], T('r3e'));
  land('srv-cmp', T('r3e') + 0.05);
  tl.to(el('req3'), { autoAlpha: 0, x: REQ_DX + 40, scale: 0.8, duration: 0.35 * D }, T('r3e') + 0.1);
  tl.fromTo(el('res3'), { autoAlpha: 0, x: 120, scale: 0.94 }, { autoAlpha: 1, x: 0, scale: 1, duration: 0.55 * D }, F('r3e', 0.4));

  head('h-r3f', 'r3f', 'r3g');
  pulse('res3-nobody', T('r3f') + 0.1, 1.06);
  rise('sizes', F('r3f', 0.3));
  grow('size-bar200', F('r3f', 0.3) + 0.3, 0.6);
  grow('size-bar304', F('r3f', 0.3) + 0.6, 0.2);

  head('h-r3g', 'r3g', 'r3i');
  exit(['sizes', 'srv-cmp'], T('r3g'));
  tl.to(el('res3'), { x: RES3.x, y: RES3.y, scale: 0.3, autoAlpha: 0, duration: 0.8 * D, ease: 'power2.inOut' }, T('r3g') + 0.1);
  ping('copy-ping', segC.t0);
  exit('kept', segC.t1);
  wipeIn('page-styled', F('r3g', 0.55));
  exit('needs', F('r3g', 0.55));
  grow('band-fresh2', F('r3g', 0.6), 0.8);
  fade('band-fresh2-lbl', F('r3g', 0.6) + 0.7, 0.3);

  rise('row3', T('r3h') + 0.1);
  flash('row3-flash', T('r3h') + 0.1);
  land('note3', T('r3h') + 0.5);

  head('h-r3i', 'r3i');
  exit(['lanes', 'lbl-req', 'lbl-res'], T('r3i'));
  land('inset', T('r3i') + 0.02, { duration: 0.35 * D });
  rise('in-a', T('r3i') + 0.12);
  draw('in-ar1', F('r3i', 0.16), 0.35);
  fade('in-ah1', F('r3i', 0.16) + 0.3, 0.1);
  rise('in-b', F('r3i', 0.2));
  draw('in-ar2', F('r3i', 0.48), 0.35);
  fade('in-ah2', F('r3i', 0.48) + 0.3, 0.1);
  rise('in-c', F('r3i', 0.52));

  // ---------- Recap ----------
  scene('sc-recap', 'c1', 'c5');
  head('h-c1', 'c1', 'c2');
  rise(['rc1', 'rc2', 'rc3'], T('c1') + 0.1, { stagger: 0.2 * D });

  head('h-c2', 'c2');
  land('ch-maxage', T('c2') + 0.1);
  draw('ar-max2', F('c2', 0.4), 0.45);
  fade('ah-max2', F('c2', 0.4) + 0.4, 0.12);
  fade(['lb-max2', 'rc2-ring'], F('c2', 0.42), 0.3);
  exit('rc2-ring', F('c2', 0.7));
  draw('ar-max3', F('c2', 0.72), 0.6);
  fade('ah-max3', F('c2', 0.72) + 0.55, 0.12);
  fade(['lb-max3', 'rc3-ring'], F('c2', 0.76), 0.3);

  dim(['ch-maxage', 'ar-max2', 'ah-max2', 'lb-max2', 'ar-max3', 'ah-max3', 'lb-max3'], T('c3'), 0.28);
  land('ch-nocache', T('c3') + 0.1);
  draw('ar-nocache', F('c3', 0.35), 0.45);
  fade('ah-nocache', F('c3', 0.35) + 0.4, 0.12);
  pulse('rc3-ring', F('c3', 0.4), 1.02);

  dim(['ch-nocache', 'ar-nocache', 'ah-nocache'], T('c4'), 0.28);
  exit('rc3-ring', T('c4'));
  land('ch-nostore', T('c4') + 0.1);
  draw('ar-nostore', F('c4', 0.35), 0.45);
  fade('ah-nostore', F('c4', 0.35) + 0.4, 0.12);
  fade('rc1-ring', F('c4', 0.4), 0.3);

  // ---------- DevTools tip ----------
  scene('sc-tip', 'c5', 'c6');
  head('h-c5', 'c5');
  rise('tip-dt', T('c5') + 0.05);
  tl.fromTo(el('pointer'), { autoAlpha: 0, x: 240, y: 150 }, { autoAlpha: 1, x: 0, y: 0, duration: 0.8 * D, ease: 'power2.inOut' }, F('c5', 0.15));
  pulse('pointer', F('c5', 0.15) + 0.85, 0.86);
  tl.to(el('pointer'), { x: 46, y: 36, duration: 0.4 * D, ease: 'power2.out' }, F('c5', 0.15) + 1.3);
  draw('tip-check', F('c5', 0.15) + 0.95, 0.25);
  tl.to(el('tip-box'), { backgroundColor: '#f0a87a', duration: 0.2, ease: 'none' }, F('c5', 0.15) + 0.95);
  rise('tip-row', F('c5', 0.55));
  rise('tip-note', F('c5', 0.62));

  // ---------- Final ----------
  scene('sc-final', 'c6');
  head('h-c6', 'c6');
  rise('fin1', T('c6') + 0.05);
  rise('fin2', F('c6', 0.32));
  rise('fin3', F('c6', 0.68));
  fade('fin-foot', F('c6', 0.9), 0.4);

  tl.set({}, {}, TOTAL);

  // ---------- Values that follow the clock ----------
  const segs = [segA, segB, segC];
  const ease = (p) => -(Math.cos(Math.PI * p) - 1) / 2; // matches GSAP sine.inOut
  function valAt(t, k) {
    let v = segs[0][k + '0'];
    for (const s of segs) {
      if (t >= s.t1) v = s[k + '1'];
      else if (t > s.t0) { v = s[k + '0'] + (s[k + '1'] - s[k + '0']) * ease((t - s.t0) / (s.t1 - s.t0)); break; }
      else break;
    }
    return v;
  }
  const X = (hours) => 48 + hours * 180;
  const ageEl = el('age'), meter = el('meter'), copyEl = el('copy');
  const stFresh = el('state-fresh'), stStale = el('state-stale'), cursor = el('cursor'), staleBand = el('band-stale');
  function derive(t) {
    const age = valAt(t, 'a');
    const clock = valAt(t, 'c');
    const fresh = age < 3600;
    ageEl.textContent = Math.round(age) + ' s';
    meter.style.transform = 'scaleX(' + Math.max(0, 1 - age / 3600).toFixed(4) + ')';
    copyEl.style.backgroundColor = fresh ? '' : 'var(--stale-tint)';
    stFresh.style.display = fresh ? '' : 'none';
    stStale.style.display = fresh ? 'none' : '';
    cursor.setAttribute('transform', 'translate(' + X(clock / 3600).toFixed(2) + ' 0)');
    const end = Math.min(Math.max(clock, 3600), 7200);
    staleBand.setAttribute('width', (X(end / 3600) - X(1)).toFixed(2));
  }

  window.__tl = tl;
  window.__derive = derive;
})();
