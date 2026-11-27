import {defineConfig} from 'vite';
import motionCanvas from '@motion-canvas/vite-plugin';
import {viteSingleFile} from 'vite-plugin-singlefile';
export default defineConfig({
  plugins: [
    motionCanvas({project: './src/project.ts'}),
    {name: 'html-entry', enforce: 'post', config: () => ({build: {rollupOptions: {input: 'index.html', output: {entryFileNames: '[name].js'}}}}),
     configResolved(c: any) { c.build.rollupOptions.input = 'index.html'; delete c.build.rollupOptions.output.dir; }},
    viteSingleFile(),
  ],
});
