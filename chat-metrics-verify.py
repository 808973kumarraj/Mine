#!/usr/bin/env python3
"""
Verify chat-metrics.csv and render a verification dashboard + screenshot.

Every value on the dashboard is computed at run time:
  - SHA-256 of the CSV / HTML files, and their git blob ids vs HEAD
  - a SHA-256 per CSV row, chained into one manifest root
  - a field-by-field cross-check of each row against a fresh
    `hermes sessions export` of the session store
  - summary statistics over the CSV

Usage: python3 chat-metrics-verify.py
Exit code is 0 only if every check passed.
"""

import csv
import hashlib
import html
import json
import statistics
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "chat-metrics.csv"
HTML_PATH = ROOT / "chat-metrics.html"
OUT_HTML = ROOT / "chat-metrics-verification.html"
OUT_PNG = ROOT / "screenshots" / "chat-metrics-verification.png"
CHROMIUM = "/opt/pw-browsers/chromium"
EXPECTED_ROWS = 16


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def run(*cmd: str) -> str:
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()


def load_store() -> dict:
    """Fresh export of the Hermes session store, keyed by 8-char session id."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "sessions.jsonl"
        subprocess.run(
            ["hermes", "sessions", "export", "--format", "jsonl", "--yes", str(out)],
            capture_output=True, text=True, check=True,
        )
        sessions = [json.loads(line) for line in out.read_text().splitlines() if line.strip()]
    return {s["id"].removeprefix("local-")[:8]: s for s in sessions}


def check_row(row: dict, session: dict | None) -> list[tuple[str, bool]]:
    """Compare one CSV row with its session-store record."""
    if session is None:
        return [("session in store", False)]
    msgs = session.get("messages") or []
    first_user = next((m.get("content") for m in msgs if m.get("role") == "user"), None)
    total = sum(session.get(k) or 0 for k in
                ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens"))
    started = datetime.fromtimestamp(session["started_at"], timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return [
        ("session in store", True),
        ("message", first_user == row["Message"]),
        ("start time", started == row["Time (UTC)"]),
        ("model", session.get("model") == row["Model Used"]),
        ("output tokens", (session.get("output_tokens") or 0) == int(row["Output Tokens"])),
        ("total tokens", total == int(row["Total Tokens (incl. cache)"])),
        ("tool calls", (session.get("tool_call_count") or 0) == int(row["Tool Calls"])),
    ]


def git_file_state(path: Path) -> dict:
    rel = path.name
    work = run("git", "hash-object", rel)
    head = run("git", "rev-parse", f"HEAD:{rel}")
    return {
        "blob": work,
        "matches_head": work == head,
        "commit": run("git", "log", "-1", "--format=%H", "--", rel),
        "commit_date": run("git", "log", "-1", "--format=%cI", "--", rel),
    }


def collect() -> dict:
    csv_bytes = CSV_PATH.read_bytes()
    html_bytes = HTML_PATH.read_bytes()
    with CSV_PATH.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        fields = reader.fieldnames
        rows = list(reader)

    try:
        store, store_error = load_store(), None
    except Exception as exc:  # store unreachable: report it, don't hide it
        store, store_error = {}, f"{type(exc).__name__}: {exc}"

    chain = sha256(b"chat-metrics-manifest-v1")
    records = []
    for row in rows:
        canonical = json.dumps([row[f] for f in fields], ensure_ascii=False, separators=(",", ":"))
        row_hash = sha256(canonical.encode("utf-8"))
        chain = sha256((chain + row_hash).encode("ascii"))
        checks = check_row(row, store.get(row["Session"])) if not store_error else []
        records.append({
            "n": int(row["#"]), "time": row["Time (UTC)"], "session": row["Session"],
            "model": row["Model Used"], "type": row["Type"],
            "out": int(row["Output Tokens"]), "total": int(row["Total Tokens (incl. cache)"]),
            "secs": float(row["Response Time (s)"]), "tools": int(row["Tool Calls"]),
            "hash": row_hash, "checks": checks,
            "ok": bool(checks) and all(ok for _, ok in checks),
        })

    done = [r for r in records if r["type"] != "Error"]
    failed = [r for r in records if r["type"] == "Error"]
    out_total = sum(r["out"] for r in records)
    tok_total = sum(r["total"] for r in records)
    html_text = html_bytes.decode("utf-8")

    file_checks = [
        (f"CSV holds exactly {EXPECTED_ROWS} records", len(records) == EXPECTED_ROWS),
        ("Record numbers are 1..N with no gaps", [r["n"] for r in records] == list(range(1, len(records) + 1))),
        ("Session ids are unique", len({r["session"] for r in records}) == len(records)),
        ("Timestamps are in ascending order", [r["time"] for r in records] == sorted(r["time"] for r in records)),
        ("HTML table has one row per CSV record", html_text.count('<tr><td class="n">') == len(records)),
        ("HTML totals equal CSV sums", f"{out_total:,}" in html_text and f"{tok_total:,}" in html_text),
        ("Session store reachable", store_error is None),
        ("Every record matches the session store", all(r["ok"] for r in records)),
    ]
    git = {"csv": git_file_state(CSV_PATH), "html": git_file_state(HTML_PATH)}
    file_checks += [
        ("chat-metrics.csv identical to committed blob", git["csv"]["matches_head"]),
        ("chat-metrics.html identical to committed blob", git["html"]["matches_head"]),
    ]

    return {
        "generated": datetime.now(timezone.utc),
        "records": records, "done": done, "failed": failed,
        "out_total": out_total, "tok_total": tok_total,
        "secs_total": sum(r["secs"] for r in records),
        "tools_total": sum(r["tools"] for r in records),
        "median_secs": statistics.median(r["secs"] for r in done) if done else 0.0,
        "mean_secs": statistics.mean(r["secs"] for r in done) if done else 0.0,
        "types": Counter(r["type"] for r in records),
        "models": Counter(r["model"] for r in records),
        "csv_sha": sha256(csv_bytes), "csv_size": len(csv_bytes),
        "html_sha": sha256(html_bytes), "html_size": len(html_bytes),
        "chain": chain, "git": git, "head": run("git", "rev-parse", "HEAD"),
        "branch": run("git", "rev-parse", "--abbrev-ref", "HEAD"),
        "file_checks": file_checks, "store_error": store_error,
        "store_sessions": len(store),
        "field_checks": sum(len(r["checks"]) for r in records),
        "field_passed": sum(ok for r in records for _, ok in r["checks"]),
    }


CSS = """
:root{--bg:#0b1020;--panel:#131a2c;--panel2:#182138;--line:#26314d;--text:#e7ecf7;--mute:#8d9bbb;
--accent:#6ea8fe;--ok:#4ade80;--okbg:#10301f;--bad:#ff7b88;--badbg:#431820;--warn:#ffd166}
*{box-sizing:border-box}
body{margin:0;padding:28px 32px 24px;width:1600px;background:var(--bg);color:var(--text);
font:13px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
.mono{font-family:ui-monospace,Menlo,Consolas,"DejaVu Sans Mono",monospace}
header{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px}
h1{margin:0;font-size:24px;letter-spacing:-.01em}
.sub{color:var(--mute);margin-top:4px}
.stamp{text-align:right}
.pill{display:inline-block;padding:6px 14px;border-radius:999px;font-weight:700;font-size:13px;letter-spacing:.04em}
.pill.ok{background:var(--okbg);color:var(--ok);border:1px solid #1f6b3f}
.pill.bad{background:var(--badbg);color:var(--bad);border:1px solid #8a2c3a}
.stamp .ts{margin-top:8px;font-size:15px}
.stamp .lbl,.lbl{color:var(--mute);font-size:11px;text-transform:uppercase;letter-spacing:.07em}
.cards{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:16px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.card b{display:block;font-size:26px;margin-top:2px;font-variant-numeric:tabular-nums}
.card small{color:var(--mute)}
.grid{display:grid;grid-template-columns:560px 1fr;gap:16px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px 18px;margin-bottom:16px}
.panel h2{margin:0 0 12px;font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--mute)}
.kv{margin-bottom:11px}.kv:last-child{margin-bottom:0}
.kv .v{font-size:12px;color:var(--accent);word-break:break-all}
.kv .m{color:var(--mute);font-size:11px}
.chk{display:flex;gap:9px;padding:4px 0;border-bottom:1px solid var(--line)}
.chk:last-child{border-bottom:0}
.y{color:var(--ok);font-weight:700}.x{color:var(--bad);font-weight:700}
.bar{display:grid;grid-template-columns:150px 1fr 34px;gap:10px;align-items:center;margin:6px 0}
.bar .t{height:9px;background:var(--panel2);border-radius:5px;overflow:hidden}
.bar .t i{display:block;height:100%;border-radius:5px;background:var(--accent)}
.bar span:last-child{text-align:right;font-variant-numeric:tabular-nums}
table{border-collapse:collapse;width:100%}
th,td{padding:7px 10px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}
th{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--mute);background:var(--panel2)}
tbody tr:nth-child(even){background:#10172a}
.r{text-align:right;font-variant-numeric:tabular-nums}
td.mono,td .mono{font-size:12px}
.badge{display:inline-block;padding:1px 9px;border-radius:999px;font-size:11px;font-weight:600}
.Answer{background:#143a2a;color:#6ee7a8}.Explanation{background:#15325c;color:#8cc0ff}
.Error{background:var(--badbg);color:#ff9aa5}.Task{background:#463a12;color:#ffd666}
tfoot td{font-weight:700;background:var(--panel2);border-bottom:0}
.scope{color:var(--mute);font-size:12px;border-top:1px solid var(--line);padding-top:12px}
.scope b{color:var(--text)}
"""


def esc(value) -> str:
    return html.escape(str(value))


def mark(ok: bool) -> str:
    return '<span class="y">&#10003;</span>' if ok else '<span class="x">&#10007;</span>'


def bars(counter: Counter, total: int, colors: dict | None = None) -> str:
    out = []
    for name, count in counter.most_common():
        color = (colors or {}).get(name, "var(--accent)")
        out.append(
            f'<div class="bar"><span class="mono">{esc(name)}</span>'
            f'<div class="t"><i style="width:{100 * count / total:.1f}%;background:{color}"></i></div>'
            f"<span>{count}</span></div>"
        )
    return "".join(out)


def render(d: dict) -> str:
    recs, n = d["records"], len(d["records"])
    checks = d["file_checks"]
    passed = sum(ok for _, ok in checks)
    all_ok = passed == len(checks)
    pill = (f'<span class="pill ok">&#10003; ALL {len(checks)} CHECKS PASSED</span>' if all_ok else
            f'<span class="pill bad">&#10007; {len(checks) - passed} OF {len(checks)} CHECKS FAILED</span>')
    ts = d["generated"]

    rows = "".join(
        f'<tr><td class="r">{r["n"]}</td><td class="mono">{esc(r["time"])}</td>'
        f'<td class="mono">{esc(r["session"])}</td>'
        f'<td><span class="badge {esc(r["type"])}">{esc(r["type"])}</span></td>'
        f'<td class="mono">{esc(r["model"])}</td>'
        f'<td class="r">{r["out"]:,}</td><td class="r">{r["total"]:,}</td>'
        f'<td class="r">{r["secs"]:.1f} s</td><td class="r">{r["tools"]}</td>'
        f'<td class="mono" style="color:var(--accent)">{r["hash"][:24]}&hellip;</td>'
        f'<td>{mark(r["ok"])} <span class="mono">{sum(ok for _, ok in r["checks"])}/{len(r["checks"])}</span></td></tr>'
        for r in recs
    )
    check_html = "".join(f'<div class="chk">{mark(ok)}<span>{esc(label)}</span></div>' for label, ok in checks)
    type_colors = {"Answer": "#6ee7a8", "Explanation": "#8cc0ff", "Error": "#ff9aa5", "Task": "#ffd666"}
    g = d["git"]
    store_note = (f'{d["store_sessions"]} sessions in store at verification time' if not d["store_error"]
                  else f'store error: {esc(d["store_error"])}')

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Chat metrics verification</title>
<style>{CSS}</style></head><body>
<header>
  <div>
    <h1>Chat response metrics &mdash; verification report</h1>
    <div class="sub">Dataset: <span class="mono">chat-metrics.csv</span> &middot; cross-checked against the Hermes session store
      (<span class="mono">hermes sessions export</span>) &middot; branch <span class="mono">{esc(d["branch"])}</span></div>
  </div>
  <div class="stamp">{pill}
    <div class="ts mono">{ts.strftime("%Y-%m-%d %H:%M:%S")} UTC</div>
    <div class="lbl">Verified at (host clock) &middot; epoch {int(ts.timestamp())}</div>
  </div>
</header>

<div class="cards">
  <div class="card"><span class="lbl">Records verified</span><b>{sum(r["ok"] for r in recs)} / {n}</b><small>{d["field_passed"]} of {d["field_checks"]} field checks passed</small></div>
  <div class="card"><span class="lbl">Completed / failed</span><b>{len(d["done"])} / {len(d["failed"])}</b><small>{100 * len(d["done"]) / n:.1f}% of turns got a model reply</small></div>
  <div class="card"><span class="lbl">Output tokens</span><b>{d["out_total"]:,}</b><small>sum over {n} sessions</small></div>
  <div class="card"><span class="lbl">Total tokens</span><b>{d["tok_total"]:,}</b><small>input + output + cache</small></div>
  <div class="card"><span class="lbl">Response time</span><b>{d["secs_total"]:,.1f} s</b><small>median {d["median_secs"]:.1f} s &middot; mean {d["mean_secs"]:.1f} s (completed)</small></div>
  <div class="card"><span class="lbl">Tool calls</span><b>{d["tools_total"]}</b><small>across {sum(1 for r in recs if r["tools"])} sessions</small></div>
</div>

<div class="grid">
  <div>
    <div class="panel"><h2>Hash verification (SHA-256)</h2>
      <div class="kv"><div class="lbl">chat-metrics.csv &middot; {d["csv_size"]:,} bytes</div><div class="v mono">{d["csv_sha"]}</div>
        <div class="m mono">git blob {g["csv"]["blob"][:12]} {mark(g["csv"]["matches_head"])} matches HEAD</div></div>
      <div class="kv"><div class="lbl">chat-metrics.html &middot; {d["html_size"]:,} bytes</div><div class="v mono">{d["html_sha"]}</div>
        <div class="m mono">git blob {g["html"]["blob"][:12]} {mark(g["html"]["matches_head"])} matches HEAD</div></div>
      <div class="kv"><div class="lbl">Manifest root &middot; chained hash of all {n} row hashes</div><div class="v mono">{d["chain"]}</div></div>
      <div class="kv"><div class="lbl">Git commit containing the dataset</div><div class="v mono">{g["csv"]["commit"]}</div>
        <div class="m mono">committed {esc(g["csv"]["commit_date"])}</div></div>
    </div>
    <div class="panel"><h2>Integrity checks &middot; {passed} / {len(checks)} passed</h2>{check_html}</div>
    <div class="panel"><h2>Records by type</h2>{bars(d["types"], n, type_colors)}</div>
    <div class="panel"><h2>Records by model</h2>{bars(d["models"], n)}</div>
  </div>
  <div>
    <div class="panel"><h2>Per-record ledger &middot; {n} records &middot; {store_note}</h2>
      <table><thead><tr><th class="r">#</th><th>Time (UTC)</th><th>Session</th><th>Type</th><th>Model</th>
        <th class="r">Out tok</th><th class="r">Total tok</th><th class="r">Time</th><th class="r">Tools</th>
        <th>Row SHA-256</th><th>Store match</th></tr></thead>
      <tbody>{rows}</tbody>
      <tfoot><tr><td></td><td colspan="4">Total</td><td class="r">{d["out_total"]:,}</td><td class="r">{d["tok_total"]:,}</td>
        <td class="r">{d["secs_total"]:,.1f} s</td><td class="r">{d["tools_total"]}</td><td colspan="2"></td></tr></tfoot></table>
    </div>
    <div class="panel"><h2>How to reproduce</h2>
      <div class="mono" style="color:var(--accent)">sha256sum chat-metrics.csv chat-metrics.html &nbsp;&amp;&amp;&nbsp; python3 chat-metrics-verify.py</div>
      <div class="sub">Row hash = SHA-256 of the row's fields as compact JSON (UTF-8). Manifest root = each row hash folded into the previous
      link, so changing, removing or reordering any row changes the root. Store match compares session id, message, start time, model,
      output tokens, total tokens and tool calls with the live session store.</div>
    </div>
  </div>
</div>

<div class="scope"><b>Scope.</b> This report shows that the dataset is internally consistent, identical to what is committed in git, and
agrees with the session store on the machine where it ran. {len(d["failed"])} of the {n} recorded turns are errors that never reached a model.
The hashes are unsigned and the timestamp comes from the host clock, so they detect later changes to the files but are not independent
third-party attestation.</div>
</body></html>"""


def screenshot() -> None:
    from playwright.sync_api import sync_playwright

    OUT_PNG.parent.mkdir(exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=CHROMIUM, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=2)
        page.goto(OUT_HTML.as_uri())
        page.screenshot(path=str(OUT_PNG), full_page=True)
        browser.close()


def main() -> int:
    data = collect()
    OUT_HTML.write_text(render(data), encoding="utf-8")
    screenshot()
    failed = [label for label, ok in data["file_checks"] if not ok]
    print(f"verified at  {data['generated'].strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"records      {sum(r['ok'] for r in data['records'])}/{len(data['records'])} match the session store")
    print(f"csv sha256   {data['csv_sha']}")
    print(f"manifest     {data['chain']}")
    print(f"checks       {len(data['file_checks']) - len(failed)}/{len(data['file_checks'])} passed")
    for label in failed:
        print(f"  FAILED: {label}")
    for r in data["records"]:
        for label, ok in r["checks"]:
            if not ok:
                print(f"  record {r['n']} ({r['session']}): {label} differs from store")
    print(f"report       {OUT_HTML}")
    print(f"screenshot   {OUT_PNG}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
