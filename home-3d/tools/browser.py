"""Shared helpers for the check scripts: start a headless browser and open the built page.

The page needs WebGL 2. On machines without a GPU the flags below make Chrome render in software
(slow, but correct). Set CHROME=/path/to/chrome to choose a browser; otherwise the system Chrome is
used if present, else Playwright's own Chromium (`python3 -m playwright install chromium`).
"""
import os
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGE = os.path.join(ROOT, 'dist', 'Home_3D_Minimalist_Concrete.html')
FLAGS = ['--use-angle=swiftshader', '--enable-unsafe-swiftshader', '--ignore-gpu-blocklist']
PHONE = dict(viewport=dict(width=412, height=892), device_scale_factor=1, is_mobile=True, has_touch=True)
DESKTOP = dict(viewport=dict(width=960, height=600), device_scale_factor=1)


def chrome_path():
    for p in (os.environ.get('CHROME'), '/usr/bin/google-chrome', '/usr/bin/chromium', '/usr/bin/chromium-browser'):
        if p and os.path.exists(p):
            return p
    return None  # Playwright's bundled Chromium


async def open_page(playwright, phone=True, page=PAGE):
    """Returns (browser, page, messages). messages collects page errors and console errors."""
    browser = await playwright.chromium.launch(executable_path=chrome_path(), args=FLAGS)
    context = await browser.new_context(**(PHONE if phone else DESKTOP))
    pg = await context.new_page()
    messages = []
    pg.on('pageerror', lambda e: messages.append('PAGE ERROR: ' + str(e)))
    pg.on('console', lambda m: messages.append('CONSOLE ERROR: ' + m.text[:300]) if m.type == 'error' else None)
    await pg.goto('file://' + page)
    return browser, pg, messages


async def wait_for_light(pg, limit=300):
    """Waits until the baked lighting has finished. Returns home3d.gi()."""
    start = time.time()
    state = None
    while time.time() - start < limit:
        await pg.wait_for_timeout(1000)
        state = await pg.evaluate('() => home3d.gi()')
        if not state['baking'] and state['progress'] > 0:
            break
    return state
