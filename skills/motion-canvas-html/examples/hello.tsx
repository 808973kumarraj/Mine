import { makeScene2D, Circle, Rect, Txt, Layout } from '@motion-canvas/2d';
import { all, createRef, easeOutBack, sequence, waitFor, loop, chain } from '@motion-canvas/core';

export default makeScene2D(function* (view) {
  const box = createRef<Rect>();
  const dot = createRef<Circle>();
  const label = createRef<Txt>();
  view.add(
    <>
      <Rect ref={box} width={0} height={240} radius={24} fill="#e6a700" />
      <Circle ref={dot} size={0} fill="#68abdf" x={-500} />
      <Txt ref={label} text="" fontSize={96} fill="#ffffff" y={300} />
    </>,
  );
  yield* box().width(240, 0.8, easeOutBack);
  yield* all(dot().size(160, 0.6), label().text('Real Motion Canvas', 1.2));
  yield* sequence(0.15, box().rotation(180, 1), box().fill('#ff6470', 1), dot().position.x(500, 1));
  yield* loop(2, () => chain(dot().position.y(-200, 0.4), dot().position.y(0, 0.4)));
  yield* waitFor(0.5);
});
