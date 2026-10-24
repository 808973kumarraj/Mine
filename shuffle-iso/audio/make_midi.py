"""Write the music bed as a 3-track MIDI file: pad chords, bass, soft arpeggio. 100 BPM, 80.4 s."""
import struct, sys

TPB, BPM = 480, 100
BARS = 34                                   # 34 bars of 4/4 at 100 BPM = 81.6 s
CHORDS = [(57, 60, 64), (53, 57, 60), (48, 52, 55), (55, 59, 62)]   # Am  F  C  G

def vlq(n):
    out = [n & 0x7F]
    while n > 127:
        n >>= 7; out.insert(0, (n & 0x7F) | 0x80)
    return bytes(out)

def track(events):                           # events: (tick, bytes)
    events.sort(key=lambda e: e[0]); data, last = b'', 0
    for t, ev in events:
        data += vlq(t - last) + ev; last = t
    data += b'\x00\xff\x2f\x00'
    return b'MTrk' + struct.pack('>I', len(data)) + data

def note(ch, t, dur, n, vel):
    return [(t, bytes([0x90 | ch, n, vel])), (t + dur, bytes([0x80 | ch, n, 0]))]

bar = TPB * 4
tempo = [(0, b'\xff\x51\x03' + struct.pack('>I', 60_000_000 // BPM)[1:]), (0, b'\xff\x03\x04Bed ')]
pad, bass, arp = [(0, b'\xff\x03\x03Pad')], [(0, b'\xff\x03\x04Bass')], [(0, b'\xff\x03\x03Arp')]
for b in range(BARS):
    ch = CHORDS[b % 4]; t = b * bar
    for n in ch: pad += note(0, t, bar - 10, n, 54)
    bass += note(1, t, TPB * 2 - 20, ch[0] - 12, 70) + note(1, t + TPB * 2, TPB * 2 - 20, ch[0] - 12, 62)
    if 2 <= b < BARS - 2:                    # arpeggio sits out the first and last two bars
        for i in range(8):
            arp += note(2, t + i * TPB // 2, TPB // 2 - 30, ch[i % 3] + 12 + (12 if i in (3, 7) else 0), 40 + (8 if i % 4 == 0 else 0))

mid = b'MThd' + struct.pack('>IHHH', 6, 1, 4, TPB) + track(tempo) + track(pad) + track(bass) + track(arp)
open(sys.argv[1], 'wb').write(mid)
print(sys.argv[1], len(mid), 'bytes')
