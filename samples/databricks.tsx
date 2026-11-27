import { makeScene2D, Rect, Txt, Line, Circle, Node } from '@motion-canvas/2d';
import { all, createRef, sequence, waitFor, chain, loop, Reference, ThreadGenerator, easeOutBack } from '@motion-canvas/core';

const FG = '#ffffff', MUTED = '#9aa0a6';
const C = { ctrl: '#3b6fd8', comp: '#e0702a', store: '#2f9e6f', user: '#8a63d2' };

function Box(r: Reference<Rect>, x: number, y: number, w: number, h: number, color: string, title: string, sub: string) {
  return (
    <Rect ref={r} x={x} y={y} width={w} height={h} radius={20} fill={color + '33'} stroke={color} lineWidth={4} opacity={0} scale={0.9}>
      <Txt text={title} fill={FG} fontSize={40} fontWeight={700} y={-h / 2 + 45} fontFamily="system-ui, sans-serif" />
      <Txt text={sub} fill={MUTED} fontSize={26} y={-h / 2 + 90} fontFamily="system-ui, sans-serif" textAlign="center" />
    </Rect>
  );
}
const show = (r: Reference<Rect>) => all(r().opacity(1, 0.5), r().scale(1, 0.5, easeOutBack));

function Chip(parent: Rect, label: string, x: number, y: number, w = 220, color = FG) {
  const c = createRef<Rect>();
  parent.add(
    <Rect ref={c} x={x} y={y} width={w} height={60} radius={12} fill="#ffffff14" stroke={color} lineWidth={2} opacity={0}>
      <Txt text={label} fill={FG} fontSize={24} fontFamily="system-ui, sans-serif" />
    </Rect>,
  );
  return c;
}

export default makeScene2D(function* (view) {
  view.fill('#111418');
  const title = createRef<Txt>(), caption = createRef<Txt>();
  const user = createRef<Rect>(), ctrl = createRef<Rect>(), comp = createRef<Rect>(), store = createRef<Rect>();
  const a1 = createRef<Line>(), a2 = createRef<Line>(), a3 = createRef<Line>(), a4 = createRef<Line>();
  const font = 'system-ui, sans-serif';
  const say = (t: string, _d?: number) => chain(caption().opacity(0, 0.2), caption().text(t, 0), caption().opacity(1, 0.3), waitFor(0.6));

  view.add(
    <>
      <Txt ref={title} text="How Databricks works" fill={FG} fontSize={64} fontWeight={700} y={-470} fontFamily={font} opacity={0} />
      <Txt ref={caption} text="" opacity={0} fill={MUTED} fontSize={34} y={470} fontFamily={font} />
      {Box(user, -720, -150, 300, 220, C.user, 'You', 'notebook · SQL · job')}
      {Box(ctrl, -150, -150, 560, 380, C.ctrl, '1  Control plane', 'run by Databricks')}
      {Box(comp, 560, -150, 560, 380, C.comp, '2  Compute plane', 'clusters / SQL warehouses')}
      {Box(store, 200, 290, 1260, 200, C.store, '3  Cloud storage', 'S3 · ADLS · GCS — your data stays here')}
      <Line ref={a1} points={[[-570, -150], [-430, -150]]} stroke={FG} lineWidth={5} endArrow arrowSize={16} end={0} />
      <Line ref={a2} points={[[130, -150], [280, -150]]} stroke={FG} lineWidth={5} endArrow arrowSize={16} end={0} />
      <Line ref={a3} points={[[560, 40], [560, 190]]} stroke={FG} lineWidth={5} startArrow endArrow arrowSize={16} end={0} />
      <Line ref={a4} points={[[280, -60], [130, -60]]} stroke={C.comp} lineWidth={5} endArrow arrowSize={16} end={0} lineDash={[12, 10]} />
    </>,
  );

  // control plane contents
  const cWorkspace = Chip(ctrl(), 'Workspace UI', -130, 0);
  const cJobs = Chip(ctrl(), 'Jobs & scheduler', 130, 0);
  const cUC = Chip(ctrl(), 'Unity Catalog', -130, 90, 220, '#f5c542');
  const cCM = Chip(ctrl(), 'Cluster manager', 130, 90);
  // compute contents
  const driver = Chip(comp(), 'Driver', 0, -10, 200, C.comp);
  const workers = [-180, 0, 180].map(x => Chip(comp(), 'Worker', x, 110, 150));
  const wLines = [-180, 0, 180].map(x => { const l = createRef<Line>(); comp().add(<Line ref={l} points={[[0, 20], [x, 80]]} stroke={C.comp} lineWidth={3} end={0} />); return l; });
  // storage files (medallion)
  const tiers = [['Bronze', 'raw', '#b07d4f'], ['Silver', 'cleaned', '#aab4be'], ['Gold', 'business-ready', '#e3b341']].map(([t, s, col], i) => Chip(store(), `${t} · ${s}`, -380 + i * 380, 50, 330, col));

  yield* title().opacity(1, 0.6);
  yield* chain(say('You write code in a notebook, SQL editor, or schedule a job.'), show(user));
  yield* waitFor(0.6);

  yield* say('The CONTROL PLANE is Databricks’ managed backend.');
  yield* all(show(ctrl), a1().end(1, 0.6));
  yield* sequence(0.15, ...[cWorkspace, cJobs, cUC, cCM].map(c => c().opacity(1, 0.4)));
  yield* say('Unity Catalog checks who may read which tables (governance).');
  yield* loop(2, () => chain(cUC().scale(1.12, 0.25), cUC().scale(1, 0.25)));
  yield* waitFor(0.4);

  yield* say('The cluster manager starts COMPUTE in your cloud (or serverless).');
  yield* all(show(comp), a2().end(1, 0.6));
  yield* driver().opacity(1, 0.4);
  yield* say('Apache Spark: the driver splits work into tasks for many workers.');
  yield* sequence(0.15, ...workers.map((w, i) => all(wLines[i]().end(1, 0.4), w().opacity(1, 0.4))));

  yield* say('Workers read & write CLOUD STORAGE as Delta Lake tables.');
  yield* all(show(store), a3().end(1, 0.6));
  yield* sequence(0.2, ...tiers.map(t => t().opacity(1, 0.4)));
  yield* say('Delta = Parquet files + a transaction log → ACID, versions, time travel.');
  yield* waitFor(0.8);

  // data packets flowing worker <-> storage
  const packets = [0, 1, 2].map(i => { const p = createRef<Circle>(); view.add(<Circle ref={p} size={22} fill={C.store} x={380 + i * 180} y={-40} opacity={0} />); return p; });
  yield* say('Tasks run in parallel, each on a slice of the data…');
  yield* loop(2, () => all(...packets.map(p => chain(p().opacity(1, 0.1), p().y(220, 0.6), p().y(-40, 0.6), p().opacity(0, 0.1)))));
  yield* say('…and results return to your notebook, dashboard, or BI tool.');
  yield* a4().end(1, 0.6);
  yield* chain(user().scale(1.08, 0.3), user().scale(1, 0.3));
  yield* waitFor(0.6);
  yield* say('Storage and compute are separate: scale or shut down clusters, data stays.');
  yield* all(comp().opacity(0.35, 0.6), a2().opacity(0.35, 0.6), a3().opacity(0.35, 0.6));
  yield* waitFor(1);
  yield* all(comp().opacity(1, 0.6), a2().opacity(1, 0.6), a3().opacity(1, 0.6));
  yield* waitFor(1.5);
});
