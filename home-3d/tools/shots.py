"""Screenshots of the built page from chosen viewpoints (saved to shots/).

    python3 tools/shots.py <m|d> name:x,z,yawDeg,pitchDeg ... [--night]
    python3 tools/shots.py <m|d> name:view=front|back|left|right|top|iso ...
    python3 tools/shots.py <m|d> name:tour=<seconds> ...

m = phone layout (412 x 892, upright), d = desktop layout (960 x 600).
Walk-mode positions are in metres: x east, z south; yaw 0 looks north, -90 looks east; pitch is up/down.

Examples (good viewpoints):
    living room   living:0.3,-0.1,-66,-8        lounge  lounge:3.6,-0.05,-30,-12
    kitchen       kitchen:-2.5,-0.7,60,-10      TV room tv:5.4,3.6,65,-8
"""
import asyncio
import os
import sys

from playwright.async_api import async_playwright

from browser import ROOT, open_page, wait_for_light


async def main():
    phone = sys.argv[1] == 'm'
    night = '--night' in sys.argv
    shots = [a for a in sys.argv[2:] if ':' in a]
    out = os.path.join(ROOT, 'shots')
    os.makedirs(out, exist_ok=True)
    async with async_playwright() as p:
        browser, pg, messages = await open_page(p, phone=phone)
        light = await wait_for_light(pg)
        print('lighting ready:', light['progress'] == 1, '| rays', light['rays'])
        await pg.evaluate("() => document.body.classList.add('recording')")     # hides the buttons, keeps the minimap
        if night:
            await pg.evaluate("() => home3d.set('night', true, true)")
        for shot in shots:
            name, spec = shot.split(':', 1)
            if spec.startswith('view='):
                await pg.evaluate(f"() => {{ if (document.body.classList.contains('walking')) document.getElementById('exitWalk').click(); home3d.view('{spec[5:]}', true); }}")
            elif spec.startswith('tour='):
                await pg.evaluate(f"() => {{ document.body.classList.add('touring'); home3d.tourAt({float(spec[5:])}); }}")
            else:
                x, z, yaw, pitch = spec.split(',')
                await pg.evaluate(f'() => home3d.cam({x}, {z}, {yaw}, {pitch})')
            await pg.wait_for_timeout(2600)
            await pg.screenshot(path=os.path.join(out, name + '.png'))
            print('saved shots/' + name + '.png')
        print('\n'.join(messages) or 'no page errors')
        await browser.close()


asyncio.run(main())
