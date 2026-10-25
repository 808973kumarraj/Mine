"""Write the new soundtrack score as MIDI: 92 BPM, 80.4 s, no voice.

Tracks: soft pad chords, pulsing bass, a light arpeggio that thins out in the calm parts,
and a bell chime on every scene change of the Blender animation (times in SCENES).
usage: python3 score.py OUT.mid
"""
import struct, sys

TPB, BPM, DUR = 480, 92, 80.4
SCENES = [0, 8, 14, 20, 25, 32, 33.2, 34.2, 36.2, 41.2, 43.2, 44.2, 46, 56, 63, 67, 70, 78]
PROG = [(52, 55, 59), (48, 52, 55), (55, 59, 62), (50, 53, 57)]      # Em  C  G  Dm-ish: thoughtful, techy
tick = lambda sec: round(sec * BPM / 60 * TPB)

def vlq(n):
    out = [n & 0x7F]
    while n > 127: n >>= 7; out.insert(0, (n & 0x7F) | 0x80)
    return bytes(out)

def track(name, events, program=None, ch=0):
    ev = [(0, b'\xff\x03' + vlq(len(name)) + name.encode())]
    if program is not None: ev.append((0, bytes([0xC0 | ch, program])))
    ev += events; ev.sort(key=lambda e: e[0]); data, last = b'', 0
    for t, e in ev: data += vlq(t - last) + e; last = t
    data += b'\x00\xff\x2f\x00'
    return b'MTrk' + struct.pack('>I', len(data)) + data

def note(ch, t, d, n, v): return [(t, bytes([0x90 | ch, n, v])), (t + d, bytes([0x80 | ch, n, 0]))]

bar, end = TPB * 4, tick(DUR)
pad, bass, arp, bell = [], [], [], []
calm = lambda sec: 46 <= sec < 56 or sec >= 78        # quieter arpeggio while stage 2 waits, and on the end card
b = 0
while b * bar < end:
    t, ch = b * bar, PROG[b % 4]
    sec = t / TPB * 60 / BPM
    for n in ch: pad += note(0, t, bar - 20, n, 50)
    for q in range(4): bass += note(1, t + q * TPB, TPB - 60, ch[0] - 12, 66 if q == 0 else 52)
    if 2 <= sec < DUR - 2:
        for i in range(8 if not calm(sec) else 4):
            step = TPB // 2 if not calm(sec) else TPB
            arp += note(2, t + i * step, step - 40, ch[i % 3] + 12 + (12 if i % 4 == 3 else 0), 38 if not calm(sec) else 30)
    b += 1
for s in SCENES[1:]:
    bell += note(3, tick(s), TPB * 2, 88, 60) + note(3, tick(s), TPB * 2, 95, 42)
bell += note(3, tick(78.2), TPB * 4, 83, 70) + note(3, tick(78.2), TPB * 4, 90, 55) + note(3, tick(78.2), TPB * 4, 95, 45)

tempo = [(0, b'\xff\x51\x03' + struct.pack('>I', 60_000_000 // BPM)[1:])]
mid = (b'MThd' + struct.pack('>IHHH', 6, 1, 5, TPB) + track('Tempo', tempo) + track('Pad', pad, 89, 0)
       + track('Bass', bass, 38, 1) + track('Arp', arp, 11, 2) + track('Bell', bell, 14, 3))
open(sys.argv[1], 'wb').write(mid)
print(sys.argv[1], len(mid), 'bytes')
