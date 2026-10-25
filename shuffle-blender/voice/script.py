"""Narration for the Blender version: one line per scene, spoken by Kokoro (open source, Apache-2.0).

usage: KOKORO_DIR=<dir with kokoro-v1.0.onnx + voices-v1.0.bin> python script.py OUT.wav
Each line starts at its scene time; a line that would run into the next one is sped up to fit.
"""
import os, sys, numpy as np, soundfile as sf
from kokoro_onnx import Kokoro

VOICE, SR, DUR = 'af_heart', 24000, 80.4
LINES = [
    (0.3,  8.0,  "Spark gives you a hundred executors. Then one group by, and the whole job slows to a crawl."),
    (8.2,  14.0, "Picture a billion orders, spread across those executors."),
    (14.4, 20.0, "You run: d f, dot group by country, dot count."),
    (20.2, 25.0, "To count by country, every row with the same key has to land in the same partition."),
    (25.2, 32.0, "So data moves between executors. That movement is called the shuffle."),
    (32.1, 33.2, "Follow one record."),
    (33.3, 34.2, "Serialize it."),
    (34.3, 36.2, "Write it to disk."),
    (36.4, 41.2, "Send it over the network to another executor."),
    (41.3, 43.2, "The next stage fetches it."),
    (43.3, 44.2, "Deserializes it."),
    (44.3, 46.0, "And finally aggregates."),
    (46.2, 56.0, "Every one of those steps costs time. The next stage can't start until all the data arrives, so its tasks just sit and wait."),
    (56.2, 63.0, "The fix? Move less data. Or spread the work across more executors."),
    (63.2, 67.0, "Filter rows and columns before the shuffle."),
    (67.2, 70.0, "And avoid shuffles you don't need."),
    (70.2, 78.0, "One powerful trick: broadcast the small table to every executor, and skip the shuffle entirely."),
    (78.3, 80.4, "Spark performance. Episode three."),
]

k = Kokoro(os.path.join(os.environ['KOKORO_DIR'], 'kokoro-v1.0.onnx'), os.path.join(os.environ['KOKORO_DIR'], 'voices-v1.0.bin'))
track = np.zeros(int(DUR * SR), dtype=np.float32)
for start, end, text in LINES:
    speed, room = 1.0, end - start - 0.15
    audio, sr = k.create(text, voice=VOICE, speed=speed, lang='en-us')
    if len(audio) / sr > room:                       # too long for its scene: speed up (at most 1.45x)
        speed = min(1.45, len(audio) / sr / room)
        audio, sr = k.create(text, voice=VOICE, speed=speed, lang='en-us')
    i = int(start * SR); audio = audio[: len(track) - i]
    track[i:i + len(audio)] += audio
    print(f'{start:5.1f}s  {len(audio) / sr:4.1f}s of {end - start:4.1f}s  x{speed:.2f}  {text}')
sf.write(sys.argv[1], track / max(1e-6, np.abs(track).max()) * 0.9, SR)
