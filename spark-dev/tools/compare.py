#!/usr/bin/env python3
"""Render the built HTML at given source-video times and compare with the original frames.
usage: compare.py BUILT.html OUTDIR t1,t2,... [--full]
For each t writes OUTDIR/cmp_<t>.png = [ORIGINAL | OURS | OVERLAY(ours 50% over original)]
Coordinates: our SVG is 540x960 = original 1080x1920 scaled by 0.5, so pixels line up 1:1.
By default crops to y 0..600 (the animation area; below that the original shows the presenter panel). --full keeps 0..960.
Also writes OUTDIR/ours_<t>.png and OUTDIR/ref_<t>.png at full 540x960."""
import sys,os,subprocess
from playwright.sync_api import sync_playwright
from PIL import Image, ImageDraw
VIDEO='/tmp/claude-0/-home-user-Mine/3715eb14-b92d-5873-b14a-57546d6e8354/scratchpad/spark.mp4'
html,outdir,ts=sys.argv[1],sys.argv[2],[float(x) for x in sys.argv[3].split(',')]
full='--full' in sys.argv; H=960 if full else 600
os.makedirs(outdir,exist_ok=True)
with sync_playwright() as p:
    b=p.chromium.launch(executable_path='/opt/pw-browsers/chromium-1194/chrome-linux/chrome')
    pg=b.new_page(viewport={'width':540,'height':960},device_scale_factor=1)
    errs=[];pg.on('pageerror',lambda e:errs.append(str(e)));pg.on('console',lambda m:errs.append(m.text) if m.type=='error' else None)
    pg.goto('file://'+os.path.abspath(html)+'?paused=1&cc=0&t=0')
    pg.add_style_tag(content='body{padding:0!important;display:block!important;min-height:0!important}#controls{display:none!important}#stage{width:540px!important;height:960px!important;border-radius:0!important;box-shadow:none!important}')
    pg.evaluate('document.fonts.ready');pg.wait_for_timeout(600)
    for t in ts:
        pg.evaluate(f'window.__render({t})');pg.wait_for_timeout(60)
        pg.locator('#g').screenshot(path=f'{outdir}/ours_{t}.png')
    b.close()
    if errs: print('PAGE ERRORS:',errs[:5])
for t in ts:
    subprocess.run(['ffmpeg','-loglevel','error','-y','-ss',str(t),'-i',VIDEO,'-frames:v','1','-vf','scale=540:960',f'{outdir}/ref_{t}.png'],check=True)
    r=Image.open(f'{outdir}/ref_{t}.png').convert('RGB'); o=Image.open(f'{outdir}/ours_{t}.png').convert('RGB').resize((540,960))
    rc,oc=r.crop((0,0,540,H)),o.crop((0,0,540,H))
    ov=Image.blend(rc,oc,.5)
    sheet=Image.new('RGB',(1620+20,H+22),'white');d=ImageDraw.Draw(sheet)
    for i,(im,lab) in enumerate([(rc,'ORIGINAL'),(oc,'OURS'),(ov,'OVERLAY 50/50')]):
        sheet.paste(im,(i*550,22));d.text((i*550+4,4),f'{lab}  t={t}s',fill='black')
    sheet.save(f'{outdir}/cmp_{t}.png')
print('wrote',len(ts),'comparisons to',outdir)
