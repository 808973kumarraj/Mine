"""Narration for "How Delta Lake works", one block per scene, spoken by Chatterbox (Resemble AI, open source, MIT).

usage: python script.py OUTDIR      (runs on CPU; the model downloads from Hugging Face on first use)
With voice/ref/my-voice.wav present, the narration is spoken in that voice (the channel owner's own recording).
Writes OUTDIR/narration.wav, OUTDIR/scenes.json (each scene's start and length, set by its narration)
and the subtitles as OUTDIR/narration.srt, .vtt and .subtitles.json.
"""
import json, os, sys, numpy as np, soundfile as sf, torch
from chatterbox.tts import ChatterboxTTS

GAP, PAD = 0.35, 0.9
REF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ref/my-voice.wav')   # the owner's own voice sample: Chatterbox clones its tone
EXAGGERATION, CFG = 0.55, 0.45                         # a little more expressive than neutral, unhurried pacing      # pause between lines, breathing room at the end of a scene
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
torch.manual_seed(7)                                     # same seed, same performance on every run
model = ChatterboxTTS.from_pretrained(device='cpu')
SR = model.sr

def speak(text):
    wav = model.generate(text, audio_prompt_path=REF if os.path.exists(REF) else None, exaggeration=EXAGGERATION, cfg_weight=CFG)
    a = wav.squeeze(0).numpy().astype(np.float32)
    nz = np.nonzero(np.abs(a) > 0.01)[0]                 # trim leading/trailing silence so the subtitle timing is tight
    return a[max(0, nz[0] - int(.03 * SR)): nz[-1] + int(.08 * SR)] if len(nz) else a
chunks, cues, scenes, t = [], [], [], 0.0
for name, lines in SCENES:
    s0 = t; t += 0.4                                         # a beat before each scene's first line
    for line in lines:
        audio = speak(line)
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
