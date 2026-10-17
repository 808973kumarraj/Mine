// Animated walkthrough of the Delta Lake transaction log.
(function () {
  const STEP_SECONDS = 4.5;

  const STEPS = [
    { label: 'Create', title: 'Create the table', code: "df.write.format('delta').save('/sales')",
      body: 'Spark writes ordinary Parquet files, then writes commit 000.json. That commit records the schema (metaData), the protocol version and an "add" action for every data file.',
      key: 'A Delta table is just Parquet files plus a log that says which files belong to the table.' },
    { label: 'Append', title: 'Append new rows', code: "df.write.mode('append')...",
      body: 'New rows land in a new Parquet file. Only when commit 001.json is written do readers see them, so a crashed job leaves no half-written data.',
      key: 'Atomic commits: a change is all-or-nothing, decided by one log file appearing.' },
    { label: 'Update', title: 'Update or delete rows', code: 'UPDATE sales SET qty = 0 WHERE id = 7',
      body: 'Parquet files are never edited. Delta rewrites the affected file as part-003 and commits "remove part-001" + "add part-003". The old file stays on disk, marked as a tombstone.',
      key: 'Copy-on-write: changes add new files and logically remove old ones.' },
    { label: 'Concurrency', title: 'Two writers at once', code: 'Writer A + Writer B → commit 003',
      body: 'Both try to create 003.json, but only one can (put-if-absent). The loser re-reads the new commits, checks they don’t conflict with its own change, and retries as 004.json.',
      key: 'Optimistic concurrency gives serializable writes without locking the table.' },
    { label: 'Checkpoint', title: 'Checkpoints keep reads fast', code: 'every 10 commits (default)',
      body: 'Replaying thousands of JSON commits would be slow, so Delta periodically writes a Parquet checkpoint holding the full table state. Readers start from the latest checkpoint and replay only the commits after it.',
      key: 'State = latest checkpoint + the few JSON commits since.' },
    { label: 'Time travel', title: 'Query an older version', code: 'SELECT * FROM sales VERSION AS OF 1',
      body: 'Because old files are kept and the log records history, a reader can replay the log only up to version 1 and read exactly the files that were live then, including the tombstoned part-001.',
      key: 'Every version is reproducible: audits, rollbacks, reproducible ML training.' },
    { label: 'Vacuum', title: 'VACUUM cleans up', code: 'VACUUM sales  -- retention 7 days default',
      body: 'Tombstoned files older than the retention window are physically deleted. Storage is reclaimed, but time travel to versions that need those files is no longer possible.',
      key: 'History lives as long as the files it points to.' }
  ];
  const COMMITS = [
    { name: '000.json', at: 0, who: 'create', actions: 'protocol · metaData (schema)\n+ add part-000\n+ add part-001' },
    { name: '001.json', at: 1, who: 'append', actions: '+ add part-002' },
    { name: '002.json', at: 2, who: 'update', actions: '− remove part-001\n+ add part-003' },
    { name: '003.json', at: 3, who: 'writer A', actions: '+ add part-004' },
    { name: '004.json', at: 3, who: 'writer B (retry)', actions: '+ add part-005' },
    { name: '004.checkpoint.parquet', at: 4, who: 'checkpoint', actions: 'full state @ v4: part-000, 002, 003, 004, 005', cp: true }
  ];
  const FILES = [
    { name: 'part-000', add: 0 }, { name: 'part-001', add: 0, rm: 2, del: 6 },
    { name: 'part-002', add: 1 }, { name: 'part-003', add: 2 },
    { name: 'part-004', add: 3 }, { name: 'part-005', add: 3 }
  ];
  const VERS = [0, 1, 2, 4, 4, 4, 4];
  const TIME_TRAVEL = 5;

  const $ = (id) => document.getElementById(id);
  const el = (tag, cls, text) => {
    const n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text !== undefined) n.textContent = text;
    return n;
  };
  const live = (f, v) => f.add <= v && !(f.rm !== undefined && f.rm <= v);

  let step = 0;
  let playing = true;
  let timer;

  function schedule() {
    clearTimeout(timer);
    if (playing) timer = setTimeout(() => go(step + 1), STEP_SECONDS * 1000);
  }

  function go(s, stop) {
    if (stop) playing = false;
    step = (s + STEPS.length) % STEPS.length;
    render();
  }

  function renderStory(st) {
    const box = el('div', 'story-body fresh');
    box.append(el('h2', '', st.title), el('div', 'code', st.code), el('p', '', st.body));
    const key = el('div', 'key');
    key.append(el('b', '', 'Key idea · '), st.key);
    box.append(key);
    $('story').replaceWith(box);
    box.id = 'story';
  }

  function renderCommits(tt) {
    const list = $('commits');
    list.replaceChildren();
    COMMITS.filter((c) => c.at <= step).forEach((c) => {
      const fresh = c.at === step;
      const card = el('div', 'commit anim' + (c.cp ? ' cp' : '') + (fresh ? ' new fresh' : ''));
      if (tt && c.at > 1) card.style.opacity = 0.3;
      const top = el('div', 'commit-top');
      top.append(el('div', 'commit-name', c.name), el('div', 'commit-who', c.who));
      card.append(top, el('div', 'commit-actions', c.actions));
      list.append(card);
    });
    if (step === 3) {
      const note = el('div', 'conflict fresh');
      note.innerHTML = 'Writer B also tried to write <code>003.json</code>, found it taken, re-checked for conflicts and committed as <code>004.json</code>.';
      list.append(note);
    }
  }

  function renderFiles(tt) {
    const grid = $('files');
    grid.replaceChildren();
    FILES.filter((f) => f.add <= step).forEach((f) => {
      const gone = f.del !== undefined && f.del <= step;
      const tomb = !gone && f.rm !== undefined && f.rm <= step;
      const fresh = f.add === step || f.del === step;
      const inV1 = tt && live(f, 1);
      let tag = 'live', tagColor = '#5FD4BF', border = '#2A3846', nameColor = '#E6EDF3', op = 1;
      if (tomb) { tag = 'tombstoned · still on disk'; tagColor = '#E5735A'; nameColor = '#9AA7B4'; }
      if (gone) { tag = 'deleted by VACUUM'; tagColor = '#E5735A'; nameColor = '#5B6977'; border = '#3A2420'; op = 0.6; }
      if (f.add === step && !gone) border = '#2FB7A0';
      if (tt) {
        if (inV1) { border = '#F2A33A'; tag = 'read at v1'; tagColor = '#F2A33A'; nameColor = '#E6EDF3'; } else op = 0.3;
      }
      const card = el('div', 'file anim' + (fresh ? ' fresh' : ''));
      card.style.border = '1px ' + (gone ? 'dashed' : 'solid') + ' ' + border;
      card.style.opacity = op;
      const name = el('div', 'file-name', f.name);
      name.style.color = nameColor;
      name.style.textDecoration = (tomb || gone) && !inV1 ? 'line-through' : 'none';
      const t = el('div', 'file-tag', tag);
      t.style.color = tagColor;
      card.append(name, t);
      grid.append(card);
    });
  }

  function renderSnapshot(tt) {
    const rv = tt ? 1 : VERS[step];
    $('reader-label').textContent = tt ? 'Time-travel reader sees v1 →' : 'A reader of the latest version (v' + VERS[step] + ') reads →';
    const snap = $('snapshot');
    snap.replaceChildren();
    FILES.filter((f) => live(f, rv) && f.add <= step).forEach((f) => {
      const chip = el('div', 'chip fresh', f.name);
      chip.style.background = tt ? '#F2A33A' : '#5FD4BF';
      snap.append(chip);
    });
  }

  function renderSteps() {
    const wrap = $('steps');
    if (!wrap.children.length) {
      STEPS.forEach((t, i) => {
        const b = el('button', 'step');
        b.type = 'button';
        b.append(el('span', 'num', String(i + 1).padStart(2, '0')), el('span', 'label', t.label));
        b.addEventListener('click', () => go(i, true));
        wrap.append(b);
      });
    }
    [...wrap.children].forEach((b, i) => {
      b.classList.toggle('active', i === step);
      b.classList.toggle('done', i < step);
      b.setAttribute('aria-current', i === step ? 'step' : 'false');
    });
    $('progress').style.width = ((step / (STEPS.length - 1)) * 100).toFixed(1) + '%';
  }

  function render() {
    const tt = step === TIME_TRAVEL;
    $('version').textContent = 'v' + VERS[step];
    $('step-no').textContent = 'STEP ' + (step + 1) + ' / ' + STEPS.length;
    $('play-label').textContent = playing ? 'Pause' : 'Play';
    $('icon-pause').hidden = !playing;
    $('icon-play').hidden = playing;
    renderStory(STEPS[step]);
    renderCommits(tt);
    renderFiles(tt);
    renderSnapshot(tt);
    renderSteps();
    schedule();
  }

  $('prev').addEventListener('click', () => go(step - 1, true));
  $('next').addEventListener('click', () => go(step + 1, true));
  $('toggle').addEventListener('click', () => { playing = !playing; render(); });

  render();
})();
