/* Player: play/pause, scrubber with chapter marks, captions, speed, full screen, and the step bar
   (Prev/Next step, request tabs with one dot per step, pause after each step). */
(function () {
  const tl = window.__tl;
  const derive = window.__derive;
  const $ = (id) => document.getElementById(id);
  const stage = $('stage'), canvas = $('canvas'), poster = $('poster'), deck = $('deck');
  const btnPlay = $('btn-play'), timeEl = $('time'), scrub = $('scrub'), marks = $('marks');
  const btnCC = $('btn-cc'), speed = $('speed'), btnFS = $('btn-fs');
  const btnPrev = $('btn-prev'), btnNext = $('btn-next'), tabs = $('tabs');
  const stepLabel = $('steplabel'), stepMode = $('stepmode');
  const caption = $('caption'), capText = $('captext'), live = $('live'), rail = $('rail');

  // Fit the 1600x900 design canvas to the stage box.
  const fit = () => canvas.style.setProperty('--k', String(stage.clientWidth / 1600));
  if ('ResizeObserver' in window) new ResizeObserver(fit).observe(stage);
  else window.addEventListener('resize', fit);
  fit();

  const clamp = (v, a, b) => Math.min(Math.max(v, a), b);
  const fmt = (s) => {
    s = Math.max(0, Math.floor(s + 0.001));
    return Math.floor(s / 60) + ':' + String(s % 60).padStart(2, '0');
  };

  // Chapter spans: the first starts at 0, each ends where the next begins.
  const byId = {};
  CHAPTERS.forEach((c, i) => {
    c.beats = BEATS.filter((b) => b.ch === c.id);
    c.t0 = i === 0 ? 0 : c.beats[0].start;
    byId[c.id] = c;
  });
  CHAPTERS.forEach((c, i) => { c.t1 = i < CHAPTERS.length - 1 ? CHAPTERS[i + 1].t0 : TOTAL; });

  // Progress rail inside the stage.
  CHAPTERS.forEach((c) => {
    const seg = document.createElement('span');
    seg.className = 'seg';
    seg.style.flex = (c.t1 - c.t0).toFixed(2) + ' 1 0';
    c.fill = document.createElement('i');
    seg.appendChild(c.fill);
    rail.appendChild(seg);
  });

  // Chapter marks on the scrubber.
  CHAPTERS.forEach((c, i) => {
    if (i === 0) return;
    const m = document.createElement('button');
    m.type = 'button';
    m.className = 'mark';
    m.style.left = ((c.t0 / TOTAL) * 100).toFixed(2) + '%';
    m.title = c.title;
    m.setAttribute('aria-label', 'Jump to ' + c.title);
    m.addEventListener('click', () => goChapter(c));
    marks.appendChild(m);
  });

  // Step bar: one tab per chapter, one dot per step.
  const dotEls = [];
  const tabEls = CHAPTERS.map((c) => {
    const root = document.createElement('div');
    root.className = 'tab';
    const name = document.createElement('button');
    name.type = 'button';
    name.className = 'tab-name';
    name.textContent = c.short;
    name.title = 'Play ' + c.title;
    name.addEventListener('click', () => goChapter(c));
    const dots = document.createElement('div');
    dots.className = 'tab-dots';
    c.beats.forEach((b, j) => {
      const i = BEATS.indexOf(b);
      const d = document.createElement('button');
      d.type = 'button';
      d.className = 'dot';
      d.title = b.lab;
      d.setAttribute('aria-label', c.title + ', step ' + (j + 1) + ': ' + b.lab);
      d.addEventListener('click', () => goStep(i));
      dots.appendChild(d);
      dotEls[i] = d;
    });
    root.append(name, dots);
    tabs.appendChild(root);
    return { c, root, name };
  });

  function beatIndexAt(t) {
    let k = -1;
    for (let i = 0; i < BEATS.length; i++) {
      if (BEATS[i].start <= t + 0.001) k = i;
      else break;
    }
    return k;
  }

  let lastBeat = -2;
  let lastCap = null;
  function updateSteps(k) {
    const b = k >= 0 ? BEATS[k] : null;
    tabEls.forEach(({ c, root, name }) => {
      const on = !!b && b.ch === c.id;
      root.classList.toggle('on', on);
      if (on) name.setAttribute('aria-current', 'step');
      else name.removeAttribute('aria-current');
    });
    dotEls.forEach((d, i) => {
      d.classList.toggle('on', i === k);
      d.classList.toggle('done', i < k);
    });
    if (!b) {
      stepLabel.textContent = 'Press play, or use Next step to go one step at a time.';
      return;
    }
    const c = byId[b.ch];
    stepLabel.textContent = c.title + ' · Step ' + (c.beats.indexOf(b) + 1) + ' of ' + c.beats.length + ' · ' + b.lab;
  }

  let scrubbing = false;
  function render() {
    const t = tl.time();
    derive(t);
    let cap = '';
    for (const c of CAPS) {
      if (c.t0 <= t + 0.001) cap = c.text;
      else break;
    }
    if (cap !== lastCap) {
      capText.textContent = cap;
      lastCap = cap;
    }
    timeEl.textContent = fmt(t) + ' / ' + fmt(TOTAL);
    if (!scrubbing) scrub.value = String(Math.round((t / TOTAL) * 1000));
    scrub.style.setProperty('--p', ((t / TOTAL) * 100).toFixed(2) + '%');
    CHAPTERS.forEach((c) => {
      c.fill.style.transform = 'scaleX(' + clamp((t - c.t0) / (c.t1 - c.t0), 0, 1).toFixed(4) + ')';
    });
    const k = beatIndexAt(t);
    if (k !== lastBeat) {
      updateSteps(k);
      if (k >= 0 && lastBeat !== -2) live.textContent = BEATS[k].text;
      lastBeat = k;
    }
  }

  // Playback state. stopAt is the time to pause at when stepping.
  let playing = false;
  let stopAt = null;
  let rate = 1;
  const ICONS = { play: btnPlay.querySelector('.i-play'), pause: btnPlay.querySelector('.i-pause'), replay: btnPlay.querySelector('.i-replay') };
  function setPlaying(p) {
    playing = p;
    const atEnd = !p && tl.time() >= TOTAL - 0.02;
    ICONS.play.toggleAttribute('hidden', p || atEnd);
    ICONS.pause.toggleAttribute('hidden', !p);
    ICONS.replay.toggleAttribute('hidden', p || !atEnd);
    btnPlay.setAttribute('aria-label', p ? 'Pause' : atEnd ? 'Replay' : 'Play');
  }
  function setReady(on) { btnNext.classList.toggle('ready', on); }
  function hidePoster() { if (!poster.hidden) poster.hidden = true; }

  function seek(t) {
    tl.pause();
    tl.time(clamp(t, 0, TOTAL), false);
    render();
  }
  function stepEnd(t) {
    const k = beatIndexAt(t);
    if (k < 0) return BEATS[0].end;
    if (t < BEATS[k].end - 0.05) return BEATS[k].end;
    return k + 1 < BEATS.length ? BEATS[k + 1].end : TOTAL;
  }
  function play(until) {
    hidePoster();
    if (tl.time() >= TOTAL - 0.02) tl.time(0, false);
    stopAt = until != null ? until : stepMode.checked ? stepEnd(tl.time()) : null;
    tl.timeScale(rate);
    tl.play();
    setPlaying(true);
    setReady(false);
    requestAnimationFrame(loop);
  }
  function pause() {
    tl.pause();
    stopAt = null;
    setPlaying(false);
    render();
  }
  function toggle() { if (playing) pause(); else play(); }
  function loop() {
    if (!playing) return;
    const t = tl.time();
    if (stopAt != null && t >= stopAt - 0.001) {
      tl.pause();
      tl.time(stopAt, false);
      stopAt = null;
      setPlaying(false);
      setReady(true);
      render();
      return;
    }
    if (t >= TOTAL - 0.001) {
      tl.pause();
      setPlaying(false);
      render();
      return;
    }
    render();
    requestAnimationFrame(loop);
  }

  function goStep(i) {
    i = clamp(i, 0, BEATS.length - 1);
    seek(BEATS[i].start);
    play(BEATS[i].end);
  }
  function next() {
    const k = beatIndexAt(tl.time());
    if (k + 1 > BEATS.length - 1) return;
    goStep(k + 1);
  }
  function prev() {
    const t = tl.time();
    const k = beatIndexAt(t);
    if (k < 0) return goStep(0);
    goStep(t - BEATS[k].start > 1 ? k : k - 1);
  }
  function goChapter(c) {
    seek(c.beats[0].start);
    play();
  }

  btnPlay.addEventListener('click', toggle);
  $('poster-play').addEventListener('click', (e) => { e.stopPropagation(); play(); });
  poster.addEventListener('click', () => play());
  // Clicking the picture plays or pauses, like a video.
  stage.addEventListener('click', (e) => {
    if (e.target.closest('#poster')) return;
    toggle();
  });
  btnNext.addEventListener('click', next);
  btnPrev.addEventListener('click', prev);
  stepMode.addEventListener('change', () => {
    if (playing) stopAt = stepMode.checked ? stepEnd(tl.time()) : null;
  });

  let resume = false;
  scrub.addEventListener('pointerdown', () => {
    scrubbing = true;
    resume = playing;
    if (playing) { tl.pause(); setPlaying(false); }
  });
  window.addEventListener('pointerup', () => {
    if (!scrubbing) return;
    scrubbing = false;
    if (resume) play();
    resume = false;
  });
  scrub.addEventListener('input', () => {
    hidePoster();
    if (playing) { tl.pause(); setPlaying(false); }
    tl.time((Number(scrub.value) / 1000) * TOTAL, false);
    setReady(false);
    render();
  });

  function toggleCC() {
    const on = caption.classList.toggle('off') === false;
    btnCC.setAttribute('aria-pressed', String(on));
  }
  btnCC.addEventListener('click', toggleCC);
  speed.addEventListener('change', () => {
    rate = Number(speed.value) || 1;
    tl.timeScale(rate);
  });

  if (document.fullscreenEnabled && deck.requestFullscreen) {
    btnFS.addEventListener('click', () => {
      const p = document.fullscreenElement ? document.exitFullscreen() : deck.requestFullscreen();
      if (p && p.catch) p.catch(() => {});
    });
  } else {
    btnFS.hidden = true;
  }

  document.addEventListener('keydown', (e) => {
    if (e.altKey || e.ctrlKey || e.metaKey) return;
    const tag = (e.target && e.target.tagName ? e.target.tagName : '').toLowerCase();
    const native = ['button', 'input', 'select', 'textarea', 'a', 'summary'].includes(tag);
    if (e.key === ' ' || e.key === 'k') {
      if (native) return;
      e.preventDefault();
      toggle();
    } else if (e.key === 'ArrowRight' || e.key === 'ArrowLeft') {
      if (tag === 'select' || (tag === 'input' && e.target.type === 'range')) return;
      e.preventDefault();
      if (e.key === 'ArrowRight') next();
      else prev();
    } else if ((e.key === 'c' || e.key === 'C') && !['input', 'select', 'textarea'].includes(tag)) {
      toggleCC();
    }
  });

  seek(0);
  setPlaying(false);

  // Hook for the headless critique script.
  window.__explainer = { seek, play, pause, next, prev, total: TOTAL, beats: BEATS };
})();
