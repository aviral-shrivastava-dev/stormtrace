"""Verify that every panel in the served Grafana dashboard returns data.

A provisioned dashboard can load, look complete, and still render
"No data" for any panel whose query is missing or returns zero series.
This script fetches the dashboard Grafana actually serves and runs every
panel's target expressions through Grafana's own /api/ds/query -- the
same path the browser panels use -- then reports frames and last values.

    python src/verify_dashboard.py

Exits 1 when any panel has no targets or any target returns no data.

Some metrics are deliberately ABSENT rather than zero: the SatNOGS good
pass rate has no honest value until a pass completes, and publishing 0
would read as "everything failed". Those panels are listed in
OPTIONAL_PANELS: an empty result is reported as an expected gap rather
than a failure, and a non-empty result is verified normally.

Prerequisites: docker compose up -d and the API running on port 8000.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

GRAFANA = "http://127.0.0.1:3000"
DASHBOARD_UID = "stormtrace-research"

# Panel titles whose metric may legitimately not exist yet, with the reason.
OPTIONAL_PANELS = {
    "SatNOGS Good Pass Rate": (
        "the API omits this metric until at least one pass has completed"
    ),
}



def fetch_dashboard() -> dict:
    response = urllib.request.urlopen(
        GRAFANA + f"/api/dashboards/uid/{DASHBOARD_UID}", timeout=20
    )
    return json.loads(response.read())["dashboard"]


def run_query(expr: str, time_from: str, time_to: str) -> list[dict]:
    body = {
        "queries": [
            {
                "refId": "A",
                "datasource": {"type": "prometheus", "uid": "prometheus"},
                "expr": expr,
                "range": True,
                "instant": False,
                "format": "time_series",
            }
        ],
        "from": time_from,
        "to": time_to,
    }
    request = urllib.request.Request(
        GRAFANA + "/api/ds/query",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    response = json.loads(urllib.request.urlopen(request, timeout=30).read())
    return response["results"]["A"].get("frames", [])


def frame_summary(frame: dict) -> str:
    fields = frame.get("schema", {}).get("fields", [])
    labels = fields[1].get("labels", {}) if len(fields) > 1 else {}
    values = frame.get("data", {}).get("values", [])
    points = len(values[0]) if values else 0
    last = values[1][-1] if values and len(values) > 1 and values[1] else None
    metric_label = labels.get("metric") or labels.get("reliability_class") or labels.get("source_group") or ""
    suffix = f" [{metric_label}]" if metric_label else ""
    return f"{points} points, last={last}{suffix}"


def main() -> int:
    try:
        dashboard = fetch_dashboard()
    except (urllib.error.URLError, TimeoutError, ValueError,
            json.JSONDecodeError, KeyError) as error:
        print(f"Could not fetch dashboard from Grafana: {error}", file=sys.stderr)
        print("Is the stack running? docker compose up -d", file=sys.stderr)
        return 1


    time_from = dashboard.get("time", {}).get("from", "now-6h")
    time_to = dashboard.get("time", {}).get("to", "now")
    panels = dashboard.get("panels", [])

    print(f"Dashboard: {dashboard.get('title')!r} (uid {dashboard.get('uid')})")
    print(f"Time range: {time_from} .. {time_to}")
    print()

    failures = 0
    expected_gaps = 0
    for panel in panels:
        title = panel.get("title", f"panel {panel.get('id')}")
        targets = panel.get("targets") or []
        if not targets:
            print(f"[FAIL] {title}: no query targets -- panel cannot render data")
            failures += 1
            continue

        optional_reason = OPTIONAL_PANELS.get(title)
        panel_failed = False
        for target in targets:
            expr = target.get("expr")
            if not expr:
                print(f"[FAIL] {title}: target {target.get('refId')} has no expr")
                panel_failed = True
                continue
            try:
                frames = run_query(expr, time_from, time_to)
            except urllib.error.HTTPError as error:
                print(f"[FAIL] {title} :: {expr}: HTTP {error.code}")
                panel_failed = True
                continue
            except (urllib.error.URLError, TimeoutError, ValueError,
                    json.JSONDecodeError, KeyError) as error:
                print(f"[FAIL] {title} :: {expr}: {error}")
                panel_failed = True
                continue

            if not frames:
                if optional_reason:
                    print(f"[GAP ] {title} :: {expr}: no data yet -- {optional_reason}")
                    expected_gaps += 1
                else:
                    print(f"[FAIL] {title} :: {expr}: no data")
                    panel_failed = True
            else:
                summaries = "; ".join(frame_summary(frame) for frame in frames[:3])
                more = f" (+{len(frames) - 3} more)" if len(frames) > 3 else ""
                print(f"[ OK ] {title} :: {expr} -> {len(frames)} series: {summaries}{more}")

        if panel_failed:
            failures += 1

    print()
    if failures:
        print(f"{failures} panel(s) failed verification.", file=sys.stderr)
        return 1
    if expected_gaps:
        print(
            f"All {len(panels)} panels verified: {expected_gaps} expected gap(s), "
            "every other target returns data."
        )
    else:
        print(f"All {len(panels)} panels verified: every target returns data.")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
