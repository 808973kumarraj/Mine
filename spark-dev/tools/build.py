#!/usr/bin/env python3
"""Inline scenes into template.html -> standalone HTML.
usage: build.py OUT.html [--only s3_shuffle,...]   (s0_header is always included)"""
import sys,glob,os
D=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
out=sys.argv[1]; only=None
if '--only' in sys.argv: only=set(sys.argv[sys.argv.index('--only')+1].split(','))|{'s0_header'}
parts=[]
for f in sorted(glob.glob(D+'/scenes/*.js')):
    n=os.path.basename(f)[:-3]
    if only and n not in only: continue
    parts.append(open(f).read())
tpl=open(D+'/template.html').read()
open(out,'w').write(tpl.replace('/*__SCENES__*/','\n'.join(parts)))
print('built',out,'scenes:',len(parts))
