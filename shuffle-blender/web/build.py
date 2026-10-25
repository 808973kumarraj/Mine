"""Pack the Blender scene and its player into one standalone HTML file.

usage: python3 build.py LIBDIR FONTS.css OUT.html
LIBDIR holds three.js r147 (three-three.min.js) and its examples/js files three-GLTFLoader.js,
three-LineSegmentsGeometry.js, three-LineMaterial.js, three-LineSegments2.js.
Inputs: shuffle.glb + shuffle.anim.json (blender -b -P ../shuffle.py -- x --export shuffle.glb),
../../shuffle-iso/assets/paper-bg.webp (PhotoCraft + LightCraft), ../audio/narrated.m4a (Kokoro narration over the SoundCraft score, mixed in FilmCraft).
"""
import base64, pathlib, re, sys, urllib.request

here = pathlib.Path(__file__).parent
libdir, fonts, out = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
b64 = lambda p: base64.b64encode(pathlib.Path(p).read_bytes()).decode()
html = (here / 'src.html').read_text()

def embed(m):
    return 'url(data:font/woff2;base64,' + base64.b64encode(urllib.request.urlopen(m.group(1)).read()).decode() + ')'
html = html.replace('<!--FONTS-->', '<style>' + re.sub(r'url\((https://[^)]+)\)', embed, pathlib.Path(fonts).read_text()) + '</style>')
libs = ['three-three.min.js', 'three-GLTFLoader.js', 'three-LineSegmentsGeometry.js', 'three-LineMaterial.js', 'three-LineSegments2.js']
html = html.replace('<!--LIBS-->', ''.join('<script>' + (libdir / f).read_text().replace('</script', '<\\/script') + '</script>' for f in libs))
html = html.replace('/*ANIM*/null', (here / 'shuffle.anim.json').read_text())
html = html.replace('GLB_DATA', b64(here / 'shuffle.glb'))
html = html.replace('PAPER_SRC', 'data:image/webp;base64,' + b64(here / '../../shuffle-iso/assets/paper-bg.webp'))
html = html.replace('AUDIO_SRC', 'data:audio/mp4;base64,' + b64(here / '../audio/narrated.m4a'))
pathlib.Path(out).write_text(html)
print(out, round(len(html) / (1 << 20), 1), 'MB')
