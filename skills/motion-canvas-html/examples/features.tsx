import { makeScene2D, Code, Latex, Layout, Rect, Txt } from '@motion-canvas/2d';
import { all, createRef, waitFor } from '@motion-canvas/core';

export default makeScene2D(function* (view) {
  const code = createRef<Code>();
  const tex = createRef<Latex>();
  view.add(
    <Layout direction="column" gap={60} alignItems="center" layout>
      <Layout gap={30} layout>
        <Rect width={160} height={100} fill="#e6a700" radius={12} />
        <Rect width={160} height={100} fill="#68abdf" radius={12} />
        <Rect width={160} height={100} fill="#ff6470" radius={12} />
      </Layout>
      <Code ref={code} fontSize={48} code={'const x = 1;'} />
      <Latex ref={tex} tex="E = mc^2" fill="#ffffff" height={100} />
    </Layout>,
  );
  yield* code().code('const x = 1 + 2;', 1);
  yield* tex().tex('E = mc^2 + \\frac{1}{2}', 1);
  yield* waitFor(0.5);
});
