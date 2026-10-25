"""Narration for "How Delta Lake works", one block per scene, spoken by Kokoro (open source, Apache-2.0).

usage: KOKORO_DIR=<dir with kokoro-v1.0.onnx + voices-v1.0.bin> python script.py OUTDIR
Writes OUTDIR/narration.wav, OUTDIR/scenes.json (each scene's start and length, set by its narration)
and the subtitles as OUTDIR/narration.srt, .vtt and .subtitles.json.
"""
import json, os, sys, numpy as np, soundfile as sf
from kokoro_onnx import Kokoro

VOICE, SR, GAP, PAD = 'af_heart', 24000, 0.35, 0.9      # pause between lines, breathing room at the end of a scene
SCENES = [
    ('title',      ["How does Delta Lake work?"]),
    ('problem',    ["A data lake is just files. Parquet files, sitting in cloud storage.",
                    "But if a job fails halfway through a write, it leaves half the files behind, and every reader sees broken data."]),
    ('log',        ["Delta Lake adds one thing: a transaction log.",
                    "Next to the data files sits a folder called underscore delta log, holding numbered commit files.",
                    "The log is the single source of truth for what the table contains."]),
    ('write',      ["When you write, Delta first writes the new Parquet files.",
                    "Then it adds the next commit to the log, in one atomic step.",
                    "If the job fails before the commit, the new files are simply ignored."]),
    ('read',       ["To read, Spark opens the log first, and replays the commits to get the list of current files.",
                    "Only those files are read. Nothing else."]),
    ('update',     ["Parquet files are never edited in place.",
                    "An update or delete writes a new file, and one commit marks the old file as removed and the new file as added."]),
    ('concurrent', ["What if two writers commit at the same time?",
                    "Both try to create commit number three. Only one can win.",
                    "The other checks for conflicts, and retries as commit number four."]),
    ('timetravel', ["Because old files are kept, and every commit is recorded, you can read the table as it was.",
                    "Version as of one, gives you the table exactly as it looked back then."]),
    ('checkpoint', ["Every ten commits, Delta writes a checkpoint: one Parquet file that summarizes the log so far.",
                    "Readers start from the latest checkpoint, so they stay fast."]),
    ('outro',      ["Files, plus a log. That is Delta Lake."]),
]
SHOW = {"Next to the data files sits a folder called underscore delta log, holding numbered commit files.":
        "Next to the data files sits a folder called _delta_log, holding numbered commit files.",
        "Version as of one, gives you the table exactly as it looked back then.":
        "VERSION AS OF 1 gives you the table exactly as it looked back then."}

out = sys.argv[1]; os.makedirs(out, exist_ok=True)
k = Kokoro(os.path.join(os.environ['KOKORO_DIR'], 'kokoro-v1.0.onnx'), os.path.join(os.environ['KOKORO_DIR'], 'voices-v1.0.bin'))
chunks, cues, scenes, t = [], [], [], 0.0
for name, lines in SCENES:
    s0 = t; t += 0.4                                         # a beat before each scene's first line
    for line in lines:
        audio, _ = k.create(line, voice=VOICE, speed=1.0, lang='en-us')
        chunks.append((t, audio)); cues.append((round(t, 2), round(t + len(audio) / SR + 0.2, 2), SHOW.get(line, line)))
        t += len(audio) / SR + GAP
    t += PAD; scenes.append({'name': name, 'start': round(s0, 3), 'duration': round(t - s0, 3)})
track = np.zeros(int((t + 0.5) * SR), dtype=np.float32)
for t0, a in chunks: i = int(t0 * SR); track[i:i + len(a)] += a
sf.write(os.path.join(out, 'narration.wav'), track / max(1e-6, np.abs(track).max()) * 0.9, SR)
json.dump({'duration': round(t, 3), 'scenes': scenes}, open(os.path.join(out, 'scenes.json'), 'w'), indent=1)
ts = lambda x, sep: f'{int(x // 3600):02d}:{int(x % 3600 // 60):02d}:{int(x % 60):02d}{sep}{int(round(x % 1 * 1000)):03d}'
json.dump([{'start': a, 'end': b, 'text': c} for a, b, c in cues], open(os.path.join(out, 'narration.subtitles.json'), 'w'), indent=1)
with open(os.path.join(out, 'narration.srt'), 'w') as f:
    for i, (a, b, c) in enumerate(cues, 1): f.write(f'{i}\n{ts(a, ",")} --> {ts(b, ",")}\n{c}\n\n')
with open(os.path.join(out, 'narration.vtt'), 'w') as f:
    f.write('WEBVTT\n\n'); [f.write(f'{ts(a, ".")} --> {ts(b, ".")}\n{c}\n\n') for a, b, c in cues]
for s in scenes: print(f"{s['name']:11s} {s['start']:6.2f}s  +{s['duration']:5.2f}s")
print('total', round(t, 2))
