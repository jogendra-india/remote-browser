#!/usr/bin/env python3
"""
Remote Browser — browse the web through this machine.

PC A opens http://<this-machine>:9500 in a browser tab.
A headless Chrome runs here; its screen is streamed to PC A via WebSocket.
Mouse / keyboard events from PC A are forwarded to the headless Chrome.
All actual web traffic originates from THIS machine — PC A only talks to this server.
"""

import asyncio
import json
import os
import platform
import shutil
import subprocess
import sys
import time

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

import aiohttp
from aiohttp import web

CDP_PORT = int(os.environ.get("CDP_PORT", 9223))
SERVER_PORT = int(os.environ.get("PORT", 9500))
VIEWPORT_W = int(os.environ.get("VIEWPORT_W", 1366))
VIEWPORT_H = int(os.environ.get("VIEWPORT_H", 768))
JPEG_QUALITY = int(os.environ.get("JPEG_QUALITY", 70))

HERE = os.path.dirname(os.path.abspath(__file__))


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def find_chrome():
    system = platform.system()
    if system == "Linux":
        names = ["google-chrome-stable", "google-chrome", "chromium-browser", "chromium"]
    elif system == "Darwin":
        names = ["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"]
    elif system == "Windows":
        names = [
            os.path.expandvars(r"%PROGRAMFILES%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%PROGRAMFILES(X86)%\Google\Chrome\Application\chrome.exe"),
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        ]
    else:
        names = ["google-chrome", "chromium"]
    for n in names:
        if shutil.which(n) or os.path.isfile(n):
            return n
    return None


class RemoteBrowser:
    def __init__(self):
        self.chrome_proc = None
        self.cdp_ws = None
        self._cdp_session = None
        self.clients: set[web.WebSocketResponse] = set()
        self._msg_id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self.current_url = ""
        self.current_title = ""
        self.vw = VIEWPORT_W
        self.vh = VIEWPORT_H
        self._chrome_path = None
        self._stopping = False
        self.session_target_id = None

    async def start(self):
        self._chrome_path = find_chrome()
        if not self._chrome_path:
            sys.exit("Chrome / Chromium not found on this machine.")
        self._launch_chrome()
        await self._connect_and_init()
        asyncio.create_task(self._supervise())
        asyncio.create_task(self._tab_janitor())
        log(f"Ready — viewport {self.vw}x{self.vh}")

    def _launch_chrome(self):
        profile = os.path.join("/tmp", ".chromium-cache")
        args = [
            self._chrome_path,
            "--headless=new",
            f"--remote-debugging-port={CDP_PORT}",
            f"--user-data-dir={profile}",
            f"--window-size={self.vw},{self.vh}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-sync",
            "--disable-translate",
            "--disable-background-networking",
            "--mute-audio",
            "--no-sandbox",
            "about:blank",
        ]
        self.chrome_proc = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        log(f"Chrome launched PID={self.chrome_proc.pid}")

    async def _connect_and_init(self):
        """(Re)connect the CDP WebSocket and (re)apply page setup."""
        ws_url = await self._wait_for_cdp()
        self._cdp_session = aiohttp.ClientSession()
        self.cdp_ws = await self._cdp_session.ws_connect(
            ws_url, max_msg_size=50 * 1024 * 1024
        )
        log("CDP WebSocket connected")
        asyncio.create_task(self._cdp_listen())

        await self._cdp_call("Page.enable")
        await self._cdp_call("Runtime.enable")
        await self._apply_user_agent()
        await self._cdp_call("Emulation.setDeviceMetricsOverride", {
            "width": self.vw, "height": self.vh,
            "deviceScaleFactor": 1, "mobile": False,
        })
        await self._cdp_call("Page.addScriptToEvaluateOnNewDocument", {
            # Keep everything in this single tab: JS popups become same-tab
            # navigations, and target="_blank" links are rewritten to _self.
            # A new foreground tab would otherwise background this one and
            # pause its screencast (freezing the remote view).
            "source": (
                "window.open=function(u){if(u)location.href=u;return null;};"
                "document.addEventListener('click',function(e){"
                "var a=e.target&&e.target.closest?e.target.closest('a[target]'):null;"
                "if(a&&a.target&&a.target!=='_self')a.target='_self';"
                "},true);"
            )
        })
        await self._start_screencast()

    async def _apply_user_agent(self):
        """Drop the 'HeadlessChrome' UA token that bot-protection (Akamai/IRCTC
        etc.) blocks, while keeping the real Chrome version in sync."""
        r = await self._cdp_call("Runtime.evaluate", {
            "expression": "navigator.userAgent", "returnByValue": True,
        })
        ua = ""
        if r:
            ua = r.get("result", {}).get("result", {}).get("value", "") or ""
        ua = ua.replace("HeadlessChrome", "Chrome")
        if ua:
            await self._cdp_call("Emulation.setUserAgentOverride", {"userAgent": ua})
            log(f"UA override: {ua}")

    async def _supervise(self):
        """Watch the CDP link; if Chrome or the connection dies, recover."""
        while not self._stopping:
            await asyncio.sleep(2)
            if self._stopping:
                break
            if self.cdp_ws is not None and not self.cdp_ws.closed:
                continue

            log("CDP link down — recovering")
            self._notify_clients_status("Reconnecting browser…")
            try:
                if self._cdp_session and not self._cdp_session.closed:
                    await self._cdp_session.close()
            except Exception:
                pass
            self._pending.clear()

            # Relaunch Chrome if the process has exited.
            if self.chrome_proc is None or self.chrome_proc.poll() is not None:
                log("Chrome process gone — relaunching")
                self._launch_chrome()

            try:
                await self._connect_and_init()
                # Restore whatever page the user was on.
                if self.current_url.startswith(("http://", "https://", "file://")):
                    await self._cdp_call("Page.navigate", {"url": self.current_url})
                log("CDP recovered")
                self._notify_clients_status("Connected")
            except Exception as e:
                log(f"Recovery attempt failed: {e}")
                # Loop will retry on the next tick.

    async def _tab_janitor(self):
        """Safety net: if a stray tab still gets created (a popup the click
        rewrite missed), it would background the session tab and freeze the
        screencast. Close any page target that isn't our session tab so the
        view always stays on the controllable tab."""
        while not self._stopping:
            await asyncio.sleep(1.5)
            if self._stopping or not self.session_target_id:
                continue
            try:
                async with aiohttp.ClientSession() as s:
                    resp = await s.get(
                        f"http://127.0.0.1:{CDP_PORT}/json",
                        timeout=aiohttp.ClientTimeout(total=2),
                    )
                    targets = await resp.json()
                    for t in targets:
                        if (t.get("type") == "page"
                                and t.get("id")
                                and t["id"] != self.session_target_id):
                            log(f"Closing stray tab: {t.get('url', '')[:60]}")
                            await s.put(
                                f"http://127.0.0.1:{CDP_PORT}/json/close/{t['id']}"
                            )
            except Exception:
                pass

    def _notify_clients_status(self, text):
        self._broadcast_nowait(json.dumps({"type": "status", "text": text}))

    async def stop(self):
        self._stopping = True
        if self.cdp_ws:
            await self.cdp_ws.close()
        if self._cdp_session:
            await self._cdp_session.close()
        if self.chrome_proc:
            self.chrome_proc.terminate()

    async def _wait_for_cdp(self):
        for attempt in range(60):
            try:
                session = aiohttp.ClientSession()
                resp = await session.get(
                    f"http://127.0.0.1:{CDP_PORT}/json",
                    timeout=aiohttp.ClientTimeout(total=2),
                )
                tabs = await resp.json()
                resp.close()
                await session.close()
                # Pick the real page target, not an extension background page.
                page = next(
                    (t for t in tabs
                     if t.get("type") == "page"
                     and t.get("webSocketDebuggerUrl")),
                    None,
                )
                if not page:
                    await asyncio.sleep(0.5)
                    continue
                self.session_target_id = page.get("id")
                log(f"CDP ready (attempt {attempt}) — {page.get('url', '')}")
                return page["webSocketDebuggerUrl"]
            except Exception:
                try:
                    await session.close()
                except Exception:
                    pass
                await asyncio.sleep(0.5)
        raise RuntimeError("Chrome did not start (CDP not reachable).")

    # ── CDP transport ───────────────────────────────────────────────

    def _next_id(self):
        self._msg_id += 1
        return self._msg_id

    async def _cdp_call(self, method, params=None):
        """Send CDP command and wait for response."""
        mid = self._next_id()
        msg = {"id": mid, "method": method}
        if params:
            msg["params"] = params
        fut = asyncio.get_event_loop().create_future()
        self._pending[mid] = fut
        await self.cdp_ws.send_json(msg)
        try:
            result = await asyncio.wait_for(fut, timeout=15)
            if "error" in result:
                log(f"CDP error {method}: {result['error']}")
            return result
        except asyncio.TimeoutError:
            self._pending.pop(mid, None)
            log(f"CDP timeout: {method}")
            return None

    async def _cdp_fire(self, method, params=None):
        """Send CDP command, don't wait for response."""
        mid = self._next_id()
        msg = {"id": mid, "method": method}
        if params:
            msg["params"] = params
        try:
            await self.cdp_ws.send_json(msg)
        except Exception as e:
            log(f"CDP send error ({method}): {e}")

    async def _cdp_listen(self):
        log("CDP listener started")
        try:
            async for msg in self.cdp_ws:
                if msg.type == aiohttp.WSMsgType.CLOSED:
                    break
                if msg.type == aiohttp.WSMsgType.ERROR:
                    log(f"CDP WS error: {self.cdp_ws.exception()}")
                    break
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue

                data = json.loads(msg.data)

                # Response to a command we sent
                if "id" in data:
                    fut = self._pending.pop(data["id"], None)
                    if fut and not fut.done():
                        fut.set_result(data)
                    continue

                method = data.get("method", "")

                if method == "Page.screencastFrame":
                    p = data["params"]
                    # Ack immediately (fire-and-forget)
                    await self._cdp_fire("Page.screencastFrameAck", {
                        "sessionId": p["sessionId"]
                    })
                    # Broadcast frame (non-blocking per client)
                    frame_json = json.dumps({
                        "type": "frame",
                        "data": p["data"],
                        "w": p["metadata"]["deviceWidth"],
                        "h": p["metadata"]["deviceHeight"],
                    })
                    self._broadcast_nowait(frame_json)

                elif method == "Page.frameNavigated":
                    frame = data["params"].get("frame", {})
                    if not frame.get("parentId"):
                        self.current_url = frame.get("url", "")
                        log(f"Navigated: {self.current_url}")
                        self._broadcast_nowait(json.dumps({
                            "type": "url", "url": self.current_url,
                        }))

                elif method == "Page.domContentEventFired":
                    asyncio.create_task(self._update_title())

                elif method == "Page.javascriptDialogOpening":
                    await self._cdp_fire("Page.handleJavaScriptDialog", {"accept": True})

        except Exception as e:
            log(f"CDP listener died: {e}")

        log("CDP listener ended")

    def _broadcast_nowait(self, text):
        """Schedule sends to all clients without blocking the CDP listener."""
        dead = [c for c in self.clients if c.closed]
        for c in dead:
            self.clients.discard(c)
        for c in list(self.clients):
            asyncio.create_task(self._safe_send(c, text))

    async def _safe_send(self, ws, text):
        try:
            await ws.send_str(text)
        except Exception:
            self.clients.discard(ws)

    async def _update_title(self):
        r = await self._cdp_call("Runtime.evaluate", {
            "expression": "document.title", "returnByValue": True,
        })
        if r:
            t = r.get("result", {}).get("result", {}).get("value", "")
            if t != self.current_title:
                self.current_title = t
                self._broadcast_nowait(json.dumps({"type": "title", "title": t}))

    # ── Screencast ──────────────────────────────────────────────────

    async def _start_screencast(self):
        r = await self._cdp_call("Page.startScreencast", {
            "format": "jpeg",
            "quality": JPEG_QUALITY,
            "maxWidth": self.vw,
            "maxHeight": self.vh,
            "everyNthFrame": 1,
        })
        log(f"Screencast started: {r is not None}")

    # ── Actions from client ─────────────────────────────────────────

    async def navigate(self, url):
        if not url.startswith(("http://", "https://", "file://")):
            url = "https://" + url
        log(f"Navigate request: {url}")
        self.current_url = url
        r = await self._cdp_call("Page.navigate", {"url": url})
        log(f"Navigate result: {r}")

    async def go_back(self):
        await self._cdp_fire("Runtime.evaluate", {"expression": "history.back()"})

    async def go_forward(self):
        await self._cdp_fire("Runtime.evaluate", {"expression": "history.forward()"})

    async def reload(self):
        await self._cdp_fire("Page.reload")

    async def mouse(self, d):
        et = d["eventType"]
        params = {"type": et, "x": d["x"], "y": d["y"]}

        modifiers = 0
        if d.get("altKey"):   modifiers |= 1
        if d.get("ctrlKey"):  modifiers |= 2
        if d.get("metaKey"):  modifiers |= 4
        if d.get("shiftKey"): modifiers |= 8
        params["modifiers"] = modifiers

        if et in ("mousePressed", "mouseReleased"):
            btn = d.get("button", "left")
            params["button"] = btn
            params["clickCount"] = d.get("clickCount", 1)
            params["buttons"] = {"left": 1, "right": 2, "middle": 4}.get(btn, 1)
        elif et == "mouseMoved":
            # During a drag the client reports which buttons are held; CDP needs
            # this bitmask, otherwise it treats the move as a hover and no text
            # selection happens.
            buttons = d.get("buttons", 0)
            params["buttons"] = buttons
            if buttons & 1:
                params["button"] = "left"
        elif et == "mouseWheel":
            params["button"] = "none"
            params["deltaX"] = d.get("deltaX", 0)
            params["deltaY"] = d.get("deltaY", 0)

        await self._cdp_fire("Input.dispatchMouseEvent", params)

    async def key(self, d):
        et = d["eventType"]
        key = d.get("key", "")
        code = d.get("code", "")
        kc = d.get("keyCode", 0)

        modifiers = 0
        if d.get("altKey"):   modifiers |= 1
        if d.get("ctrlKey"):  modifiers |= 2
        if d.get("metaKey"):  modifiers |= 4
        if d.get("shiftKey"): modifiers |= 8

        cdp_type = "rawKeyDown" if et == "keyDown" else "keyUp"
        await self._cdp_fire("Input.dispatchKeyEvent", {
            "type": cdp_type, "key": key, "code": code,
            "windowsVirtualKeyCode": kc, "nativeVirtualKeyCode": kc,
            "modifiers": modifiers,
        })

        if et == "keyDown" and len(key) == 1 and not (d.get("ctrlKey") or d.get("metaKey")):
            await self._cdp_fire("Input.dispatchKeyEvent", {
                "type": "char", "key": key, "code": code,
                "text": key, "unmodifiedText": key,
                "windowsVirtualKeyCode": kc, "modifiers": modifiers,
            })

    async def get_selection(self):
        """Return the text currently selected in the remote page."""
        r = await self._cdp_call("Runtime.evaluate", {
            "expression": "window.getSelection().toString()",
            "returnByValue": True,
        })
        if r:
            return r.get("result", {}).get("result", {}).get("value", "") or ""
        return ""

    async def paste(self, text):
        """Insert text into the focused element of the remote page."""
        if not text:
            return
        await self._cdp_fire("Input.insertText", {"text": text})

    async def resize(self, w, h):
        w, h = max(w, 320), max(h, 200)
        self.vw, self.vh = w, h
        await self._cdp_call("Emulation.setDeviceMetricsOverride", {
            "width": w, "height": h, "deviceScaleFactor": 1, "mobile": False,
        })
        await self._start_screencast()


# ── HTTP / WS handlers ──────────────────────────────────────────────

browser = RemoteBrowser()


async def index_handler(request):
    return web.FileResponse(os.path.join(HERE, "client.html"))


async def ws_handler(request):
    ws = web.WebSocketResponse(max_msg_size=10 * 1024 * 1024)
    await ws.prepare(request)
    browser.clients.add(ws)
    log(f"Client connected ({len(browser.clients)} total)")

    try:
        await ws.send_json({"type": "url", "url": browser.current_url})
        await ws.send_json({"type": "title", "title": browser.current_title})
    except Exception:
        browser.clients.discard(ws)
        return ws

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.ERROR:
                break
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                d = json.loads(msg.data)
            except json.JSONDecodeError:
                continue

            action = d.get("type")
            if action == "navigate":
                await browser.navigate(d.get("url", ""))
            elif action == "back":
                await browser.go_back()
            elif action == "forward":
                await browser.go_forward()
            elif action == "reload":
                await browser.reload()
            elif action == "mouse":
                await browser.mouse(d)
            elif action == "key":
                await browser.key(d)
            elif action == "copy":
                text = await browser.get_selection()
                try:
                    await ws.send_json({"type": "selection", "text": text})
                except Exception:
                    pass
            elif action == "paste":
                await browser.paste(d.get("text", ""))
            elif action == "resize":
                await browser.resize(d.get("width", 1366), d.get("height", 768))
    except Exception as e:
        log(f"Client handler error: {e}")
    finally:
        browser.clients.discard(ws)
        log(f"Client disconnected ({len(browser.clients)} total)")
    return ws


async def on_startup(app):
    await browser.start()


async def on_cleanup(app):
    await browser.stop()


def main():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/", index_handler)
    app.router.add_get("/ws", ws_handler)
    log(f"Starting on http://0.0.0.0:{SERVER_PORT}")
    web.run_app(app, host="0.0.0.0", port=SERVER_PORT, print=None)


if __name__ == "__main__":
    main()
