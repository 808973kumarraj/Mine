#!/usr/bin/env python3
"""
Hermes Agent automation with Playwright
Automates chatting with Hermes web UI
"""

from playwright.sync_api import sync_playwright
import time
import json
from pathlib import Path

def chat_with_hermes(message: str, headless=True):
    """
    Send a message to Hermes Agent and get the response

    Args:
        message: The chat message to send
        headless: Run browser in headless mode (no visual window)
    """

    with sync_playwright() as p:
        # Launch browser using pre-installed Chromium
        print(f"🚀 Launching Chromium...")
        browser = p.chromium.launch(
            executable_path='/opt/pw-browsers/chromium',
            headless=headless,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        page = browser.new_page()

        # Navigate to Hermes
        print(f"📍 Opening Hermes at localhost:9119...")
        page.goto('http://localhost:9119', wait_until='networkidle', timeout=30000)

        # Wait for page to load
        time.sleep(3)

        # Take screenshot of initial state
        page.screenshot(path='/tmp/hermes_1_initial.png')
        print(f"📸 Screenshot: /tmp/hermes_1_initial.png")

        # Find input field and send message
        print(f"💬 Sending message: '{message}'")
        try:
            # Look for input field (adjust selector if needed)
            input_selectors = [
                'input[type="text"]',
                'textarea',
                '[contenteditable="true"]',
                'input.message-input',
                'input[placeholder*="message"]',
                'input[placeholder*="Message"]',
            ]

            input_found = False
            for selector in input_selectors:
                try:
                    if page.query_selector(selector):
                        print(f"   ✓ Found input with selector: {selector}")
                        page.fill(selector, message)
                        input_found = True
                        break
                except:
                    continue

            if not input_found:
                print("   ⚠️  Could not find input field. Here's the page HTML snippet:")
                page.screenshot(path='/tmp/hermes_2_before_send.png')
                html = page.content()[:2000]
                print(html)
                browser.close()
                return

            # Take screenshot before sending
            page.screenshot(path='/tmp/hermes_2_before_send.png')

            # Find and click send button
            button_selectors = [
                'button:has-text("Send")',
                'button[type="submit"]',
                'button.send-button',
                'button:visible',
            ]

            button_found = False
            for selector in button_selectors:
                try:
                    if page.query_selector(selector):
                        print(f"   ✓ Found button with selector: {selector}")
                        page.click(selector)
                        button_found = True
                        break
                except:
                    continue

            if button_found:
                # Wait for response
                print(f"⏳ Waiting for response...")
                time.sleep(5)

                # Take screenshot after response
                page.screenshot(path='/tmp/hermes_3_response.png')
                print(f"📸 Screenshot: /tmp/hermes_3_response.png")

                # Try to extract response text
                try:
                    response_text = page.inner_text('body')
                    print(f"\n✅ Response received!")
                    print(f"{'='*50}")
                    print(response_text[-1000:])  # Last 1000 chars
                    print(f"{'='*50}")
                except:
                    print("Response received (see screenshot)")
            else:
                print("   ⚠️  Could not find send button")
                page.screenshot(path='/tmp/hermes_2_no_button.png')

        except Exception as e:
            print(f"❌ Error: {e}")
            page.screenshot(path='/tmp/hermes_error.png')

        finally:
            browser.close()
            print(f"\n✨ Done!")

if __name__ == '__main__':
    import sys

    # Get message from command line or use default
    if len(sys.argv) > 1:
        message = ' '.join(sys.argv[1:])
    else:
        message = "What is Hermes Agent?"

    print(f"\n🤖 Hermes Playwright Automation")
    print(f"{'='*50}")
    chat_with_hermes(message, headless=True)
