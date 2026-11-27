import {makeScene2D, Rect, Txt, Line, Circle, Node} from '@motion-canvas/2d';
import {all, chain, createRef, sequence, waitFor, loop, Reference, linear, easeInOutCubic} from '@motion-canvas/core';

const F = 'Inter, system-ui, sans-serif';
const C = {fg: '#e6edf3', mut: '#8b98a9', src: '#58a6ff', br: '#cd7f32', sv: '#c0c0c0', gd: '#e3b341', sp: '#ff6b4a', uc: '#3fb950', out: '#bc8cff'};

function Box(p: {r: Reference<Rect>; x: number; y: number; w?: number; h?: number; c: string; t: string; s?: string}) {
  return (
    <Rect ref={p.r} x={p.x} y={p.y} width={p.w ?? 260} height={p.h ?? 110} radius={18}
      fill={'#141b26'} stroke={p.c} lineWidth={4} opacity={0} scale={0.8} layout direction={'column'}
      alignItems={'center'} justifyContent={'center'} gap={6}>
      <Txt text={p.t} fill={p.c} fontFamily={F} fontSize={32} fontWeight={700} />
      {p.s ? <Txt text={p.s} fill={C.mut} fontFamily={F} fontSize={22} /> : null}
    </Rect>
  );
}
const show = (r: Reference<Rect>) => all(r().opacity(1, 0.5), r().scale(1, 0.5));

export default makeScene2D(function* (view) {
  const title = createRef<Txt>(), cap = createRef<Txt>();
  const s1 = createRef<Rect>(), s2 = createRef<Rect>(), s3 = createRef<Rect>();
  const ing = createRef<Rect>(), br = createRef<Rect>(), sv = createRef<Rect>(), gd = createRef<Rect>();
  const lake = createRef<Rect>(), spark = createRef<Rect>(), uc = createRef<Rect>();
  const o1 = createRef<Rect>(), o2 = createRef<Rect>(), o3 = createRef<Rect>();
  const lines = createRef<Node>(), dots = createRef<Node>();

  view.add(<>
    <Txt ref={title} y={-470} text={'How Databricks Works'} fill={C.fg} fontFamily={F} fontSize={64} fontWeight={800} opacity={0} />
    <Txt ref={cap} y={470} text={''} fill={C.mut} fontFamily={F} fontSize={34} />
    <Rect ref={lake} x={0} y={60} width={820} height={560} radius={28} stroke={'#2a3546'} lineWidth={3} lineDash={[14, 10]} opacity={0}>
      <Txt y={250} text={'Cloud object storage (S3 / ADLS / GCS) · Delta Lake format'} fill={C.mut} fontFamily={F} fontSize={22} />
    </Rect>
    <Node ref={lines} />
    <Box r={s1} x={-780} y={-150} c={C.src} t={'Databases'} s={'OLTP / CDC'} w={240} />
    <Box r={s2} x={-780} y={20} c={C.src} t={'Files'} s={'CSV · JSON'} w={240} />
    <Box r={s3} x={-780} y={190} c={C.src} t={'Streams'} s={'Kafka · IoT'} w={240} />
    <Box r={ing} x={-310} y={20} c={C.src} t={'Ingest'} s={'Auto Loader'} w={180} h={130} />
    <Box r={br} x={-110} y={20} c={C.br} t={'Bronze'} s={'raw'} w={170} h={130} />
    <Box r={sv} x={100} y={20} c={C.sv} t={'Silver'} s={'cleaned'} w={170} h={130} />
    <Box r={gd} x={310} y={20} c={C.gd} t={'Gold'} s={'aggregated'} w={170} h={130} />
    <Box r={spark} x={0} y={-200} c={C.sp} t={'Spark Compute'} s={'clusters · SQL warehouses'} w={560} />
    <Box r={uc} x={0} y={-370} c={C.uc} t={'Unity Catalog'} s={'governance · lineage · access'} w={820} h={100} />
    <Box r={o1} x={780} y={-150} c={C.out} t={'BI / SQL'} s={'dashboards'} w={240} />
    <Box r={o2} x={780} y={20} c={C.out} t={'ML & AI'} s={'MLflow · models'} w={240} />
    <Box r={o3} x={780} y={190} c={C.out} t={'Apps / APIs'} s={'serving'} w={240} />
    <Node ref={dots} />
  </>);

  const link = (pts: [number, number][], color: string) => {
    const l = <Line points={pts} stroke={color} lineWidth={4} endArrow arrowSize={14} end={0} opacity={0.8} /> as Line;
    lines().add(l);
    return l.end(1, 0.6);
  };

  // 1 Title
  yield* title().opacity(1, 0.8);
  yield* cap().text('One platform: storage, compute and governance — the "Lakehouse".', 0.8);
  yield* waitFor(1.2);

  // 2 Sources
  yield* cap().text('1 · Data arrives from many sources', 0.6);
  yield* sequence(0.2, show(s1), show(s2), show(s3));
  yield* waitFor(0.6);

  // 3 Storage
  yield* cap().text('2 · It lands in your own cloud storage, as open Delta tables', 0.6);
  yield* lake().opacity(1, 0.6);
  yield* show(ing);
  yield* all(link([[-660, -150], [-400, 0]], C.src), link([[-660, 20], [-400, 20]], C.src), link([[-660, 190], [-400, 40]], C.src));
  yield* waitFor(0.5);

  // 4 Medallion
  yield* cap().text('3 · Medallion layers refine data: Bronze → Silver → Gold', 0.6);
  yield* chain(show(br), link([[-220, 20], [-195, 20]], C.br), show(sv), link([[-25, 20], [15, 20]], C.sv), show(gd), link([[185, 20], [225, 20]], C.gd));
  yield* waitFor(0.6);

  // 5 Compute
  yield* cap().text('4 · Spark compute (separate from storage) reads & writes the tables', 0.6);
  yield* show(spark);
  yield* all(link([[-110, -145], [-110, -45]], C.sp), link([[100, -145], [100, -45]], C.sp), link([[310, -145], [310, -45]], C.sp));
  yield* spark().scale(1.06, 0.3, easeInOutCubic).to(1, 0.3);
  yield* waitFor(0.5);

  // 6 Governance
  yield* cap().text('5 · Unity Catalog governs who can see what, and tracks lineage', 0.6);
  yield* show(uc);
  yield* lake().stroke(C.uc, 0.6);
  yield* waitFor(0.6);

  // 7 Consumers
  yield* cap().text('6 · Gold data powers BI, machine learning and apps', 0.6);
  yield* sequence(0.2, show(o1), show(o2), show(o3));
  yield* all(link([[395, 0], [660, -150]], C.out), link([[395, 20], [660, 20]], C.out), link([[395, 40], [660, 190]], C.out));

  // 8 Flow: particles travel the pipeline
  yield* cap().text('End to end: raw events become trusted insights', 0.6);
  const path: [number, number][] = [[-660, 20], [-310, 20], [-110, 20], [100, 20], [310, 20], [660, 20]];
  const colors = [C.src, C.src, C.br, C.sv, C.gd, C.out];
  const runDot = function* () {
    const d = <Circle size={22} fill={C.src} x={path[0][0]} y={path[0][1]} /> as Circle;
    dots().add(d);
    for (let i = 1; i < path.length; i++) {
      yield* all(d.position(path[i], 0.45, linear), d.fill(colors[i], 0.45));
    }
    yield* d.opacity(0, 0.2);
    d.remove();
  };
  yield* sequence(0.25, runDot(), runDot(), runDot(), runDot(), runDot(), runDot());
  yield* waitFor(1.5);
});
