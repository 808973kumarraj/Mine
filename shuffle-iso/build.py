"""Inline everything src.html needs into one standalone HTML file.

usage: python3 build.py GSAP.min.js LOTTIE.min.js FONTS.css OUT.html
FONTS.css is a Google Fonts stylesheet; its font URLs are downloaded and embedded.
Inputs made by the creator apps: art/clean/*.svg (VectorCraft), assets/paper-bg.webp
(PhotoCraft + LightCraft), effectcraft/endcard.json (EffectCraft), audio/mix.m4a
(SoundCraft bed mixed under the voice in FilmCraft).
"""
import base64, json, pathlib, re, sys, urllib.request

here = pathlib.Path(__file__).parent
gsap, lottie, fonts, out = sys.argv[1:5]
b64 = lambda p: base64.b64encode(pathlib.Path(p).read_bytes()).decode()
html = (here / 'src.html').read_text()

def embed(m):
    return 'url(data:font/woff2;base64,' + base64.b64encode(urllib.request.urlopen(m.group(1)).read()).decode() + ')'
html = html.replace('<!--FONTS-->', '<style>' + re.sub(r'url\((https://[^)]+)\)', embed, pathlib.Path(fonts).read_text()) + '</style>')

libs = ''.join('<script>' + pathlib.Path(p).read_text().replace('</script', '<\\/script') + '</script>' for p in (gsap, lottie))
html = html.replace('<!--LIBS-->', libs)

clean = lambda t: re.sub(r'<\?xml[^>]*>\s*|<title>.*?</title>', '', t)
art = {p.stem: clean(p.read_text()) for p in sorted((here / 'art/clean').glob('*.svg'))}
icons = {p.stem: clean(p.read_text()) for p in sorted((here / 'icons').glob('*.svg'))}
html = html.replace('/*ART*/{}', json.dumps(art))
html = html.replace('/*ANCHORS*/{}', (here / 'art/clean/sprites.json').read_text())
html = html.replace('/*ICONS*/{}', json.dumps(icons))
html = html.replace('/*ENDCARD*/null', (here / 'effectcraft/endcard.json').read_text())
html = html.replace('PAPER_SRC', 'data:image/webp;base64,' + b64(here / 'assets/paper-bg.webp'))
html = html.replace('AUDIO_SRC', 'data:audio/mp4;base64,' + b64(here / 'audio/mix.m4a'))

pathlib.Path(out).write_text(html)
print(out, round(len(html) / (1 << 20), 1), 'MB')
