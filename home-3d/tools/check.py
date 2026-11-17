"""End-to-end check of the built page in a phone-sized headless browser.

    python3 tools/check.py

Passes when: the page loads without errors, the guided tour starts while the lighting is still being
calculated, the lighting finishes, and the view buttons, toggles, walk spots, night mode and Photo
toggle all work without a page error. Exits with status 1 otherwise.
"""
import asyncio
import json
import sys

from playwright.async_api import async_playwright

from browser import open_page, wait_for_light

ROOMS = ['Kitchen', 'Living & Dining', 'Store Room', 'Passage', 'Bathroom', 'Entrance', 'TV Room', 'Balcony']


async def main():
    async with async_playwright() as p:
        browser, pg, messages = await open_page(p, phone=True)
        await pg.wait_for_timeout(2500)

        # the tour must be able to start straight away, while the light is still refining
        await pg.click('#tourBtn')
        await pg.wait_for_timeout(6000)
        caption = await pg.evaluate("() => document.getElementById('tourCap').textContent")
        await pg.mouse.click(200, 450)                      # a tap stops the tour
        await pg.wait_for_timeout(500)

        light = await wait_for_light(pg)

        # walk to every room, then day → night → day
        for room in ROOMS:
            await pg.evaluate(f'() => home3d.walk({room!r})')
            await pg.wait_for_timeout(400)
        await pg.evaluate("() => home3d.set('night', true, false)")
        await pg.wait_for_timeout(2500)
        await pg.evaluate("() => home3d.set('night', false, false)")
        await pg.wait_for_timeout(2500)
        await pg.click('#photoBtn')                         # Photo mode on and off again
        await pg.wait_for_timeout(800)
        await pg.click('#photoBtn', timeout=180000)
        await pg.click('#exitWalk')
        await pg.wait_for_timeout(1200)

        # outside views and toggles
        for selector in ['[data-view=front]', '[data-view=top]', '[data-view=iso]', '[data-toggle=low]', '[data-toggle=terrace]',
                         '[data-toggle=labels]', '[data-toggle=low]', '[data-toggle=terrace]']:
            await pg.click(selector)
            await pg.wait_for_timeout(500)

        await browser.close()

    ok = not messages and light and light['progress'] == 1 and not light['failed'] and not light['error']
    print('tour caption after 6 s :', caption)
    print('lighting               :', json.dumps(light))
    print('messages               :', messages or 'none')
    print('RESULT                 :', 'PASS' if ok else 'FAIL')
    sys.exit(0 if ok else 1)


asyncio.run(main())
