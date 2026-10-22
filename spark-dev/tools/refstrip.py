#!/usr/bin/env python3
"""Contact strip of ORIGINAL frames to study motion/timing.
usage: refstrip.py START END FPS OUT.png [--y0 0 --y1 600]
Each tile is the original frame scaled to 540x960 coordinates (same as our SVG), cropped to y0..y1, labelled with its time."""
import sys,subprocess,os,tempfile
from PIL import Image, ImageDraw
VIDEO='/tmp/claude-0/-home-user-Mine/3715eb14-b92d-5873-b14a-57546d6e8354/scratchpad/spark.mp4'
a,b,fps,out=float(sys.argv[1]),float(sys.argv[2]),float(sys.argv[3]),sys.argv[4]
y0=int(sys.argv[sys.argv.index('--y0')+1]) if '--y0' in sys.argv else 0
y1=int(sys.argv[sys.argv.index('--y1')+1]) if '--y1' in sys.argv else 600
td=tempfile.mkdtemp()
subprocess.run(['ffmpeg','-loglevel','error','-ss',str(a),'-to',str(b),'-i',VIDEO,'-vf',f'fps={fps},scale=540:960',f'{td}/f%04d.png'],check=True)
fs=sorted(os.listdir(td)); n=len(fs); cols=min(6,n); rows=(n+cols-1)//cols
S=0.5; w,h=int(540*S),int((y1-y0)*S)
sheet=Image.new('RGB',(cols*(w+6),rows*(h+20)),'white'); d=ImageDraw.Draw(sheet)
for i,f in enumerate(fs):
    im=Image.open(f'{td}/{f}').convert('RGB').crop((0,y0,540,y1)).resize((w,h))
    x,y=(i%cols)*(w+6),(i//cols)*(h+20); sheet.paste(im,(x,y+18)); d.text((x+2,y+2),f't={a+i/fps:.2f}s',fill='black')
sheet.save(out); print('wrote',out,n,'frames')
