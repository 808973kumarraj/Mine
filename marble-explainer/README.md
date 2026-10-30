# Marble machine explainer: API Gateway vs Load Balancer

A 57-second vertical (9:16) explainer that runs as one standalone HTML page.
Open `index.html` in any modern browser. It works offline because fonts and
sound are built into the file.

- Requests are marbles, components are machines, and the counters prove the
  point: 24 bots get in through the load balancer, then the API gateway drops
  in and the bot counter freezes while the app counter keeps climbing.
- Controls: play/pause (Space or click the video), scrub bar, sound, and loop.
  ← and → jump 2 s.
- The soundtrack (lo-fi pad plus effects synced to the physics) is synthesized
  in the browser from the same event list as the visuals.

## Editing an episode

Everything episode-specific is in `window.config` at the top of `index.html`:
topic strings, colours, captions (with underlined key words), camera moves,
machine timings, and the highlighted marbles. The marble run is scripted, not
simulated live, so the counters always land on the same numbers.

## Hooks for capture

- `window.seek(t)` draws the exact frame for time `t` (seconds).
- `window.timeline(t)` renders a frame without touching the player.
- `window.__stats` reports what the scripted run produced (bots let in, pinks
  that reached a server, marbles in the 401 bin, and so on).

## Fonts

Space Grotesk and JetBrains Mono (SIL Open Font License) are inlined as base64.
To refresh them:

```sh
npm pack @fontsource/space-grotesk @fontsource/jetbrains-mono
# untar both, then:
node tools/inline-fonts.mjs <folder-holding-the-untarred-packages>
```
