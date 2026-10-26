"""Build The Energy Ledger as one standalone HTML file (works offline, no server).

usage: python3 build.py [../energy-ledger.html]
Inlines src/style.css (with the fonts as data URIs), the libraries in vendor/
(three.js r147 + its LineSegments2 add-ons, GSAP 3.12.5) and src/app.js into src/index.html.
"""
import base64, pathlib, sys

HERE = pathlib.Path(__file__).parent
out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else HERE.parent / 'energy-ledger.html')
font = lambda n: 'data:font/woff2;base64,' + base64.b64encode((HERE / 'fonts' / n).read_bytes()).decode()
css = (HERE / 'src/style.css').read_text()
for key, name in (('FONT_NEWS_I', 'Newsreader-italic.woff2'), ('FONT_NEWS', 'Newsreader-normal.woff2'), ('FONT_CAVEAT', 'Caveat-normal.woff2')):
    css = css.replace(key, font(name))
libs = ['three.min.js', 'LineSegmentsGeometry.js', 'LineMaterial.js', 'LineSegments2.js', 'gsap.min.js']
safe = lambda js: js.replace('</script', '<\\/script')
scripts = '\n'.join(f'<script>/* {n} */\n{safe((HERE / "vendor" / n).read_text())}\n</script>' for n in libs)
page = (HERE / 'src/index.html').read_text()
page = page.replace('/*CSS*/', css).replace('<!--LIBS-->', scripts).replace('/*APP*/', safe((HERE / 'src/app.js').read_text()))
out.write_text(page)
print(out, round(len(page) / 1024), 'KB')
