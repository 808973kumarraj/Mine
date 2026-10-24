"""Inline everything src.html needs into one standalone file.

usage: python3 build.py VOICE.m4a GSAP.min.js FONTS.css OUT.html
FONTS.css is a Google Fonts stylesheet; its font URLs are downloaded and embedded.
"""
import base64, json, pathlib, re, sys, urllib.request

here = pathlib.Path(__file__).parent
audio, gsap, fonts, out = sys.argv[1:5]

html = (here / 'src.html').read_text()

def embed(m):
    data = urllib.request.urlopen(m.group(1)).read()
    return 'url(data:font/woff2;base64,' + base64.b64encode(data).decode() + ')'
css = re.sub(r'url\((https://[^)]+)\)', embed, pathlib.Path(fonts).read_text())
html = html.replace('<!--FONTS-->', '<style>' + css + '</style>')

html = html.replace('<!--GSAP-->', '<script>' + pathlib.Path(gsap).read_text().replace('</script', '<\\/script') + '</script>')

icons = {p.stem: re.sub(r'<\?xml[^>]*>\s*|<title>.*?</title>', '', p.read_text()) for p in sorted((here / 'icons').glob('*.svg'))}
html = html.replace('/*ICONS*/{}', json.dumps(icons))

html = html.replace('AUDIO_SRC', 'data:audio/mp4;base64,' + base64.b64encode(pathlib.Path(audio).read_bytes()).decode())

pathlib.Path(out).write_text(html)
print(out, len(html) // (1 << 20), 'MB')
