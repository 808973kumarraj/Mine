import project from './project?project';
import {Player, Stage, Vector2} from '@motion-canvas/core';
const size = new Vector2(1920, 1080);
const stage = new Stage();
stage.configure({size, resolutionScale: 1, background: '#0b0f17'});
const player = new Player(project as any, {size, fps: 60, range: [0, Infinity]});
document.getElementById('stage')!.append(stage.finalBuffer);
player.onRender.subscribe(async () => {
  await stage.render(player.playback.currentScene, player.playback.previousScene);
});
const btn = document.getElementById('play') as HTMLButtonElement;
const seek = document.getElementById('seek') as HTMLInputElement;
const time = document.getElementById('time')!;
let playing = true, dragging = false;
player.togglePlayback(true);
player.toggleLoop(true);
player.onStateChanged.subscribe(s => { playing = !s.paused; btn.textContent = playing ? 'Pause' : 'Play'; btn.setAttribute('aria-pressed', String(playing)); });
player.onDurationChanged.subscribe(d => { seek.max = String(d); });
player.onFrameChanged.subscribe(f => {
  if (!dragging) seek.value = String(f);
  time.textContent = (f / 60).toFixed(1) + 's';
});
btn.onclick = () => player.togglePlayback();
seek.oninput = () => { dragging = true; player.requestSeek(+seek.value); };
seek.onchange = () => { dragging = false; };
document.querySelectorAll<HTMLButtonElement>('[data-t]').forEach(b => b.onclick = () => player.requestSeek(+b.dataset.t! * 60));
