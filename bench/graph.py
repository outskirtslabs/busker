#!/usr/bin/env python3
"""Render benchmark JSON into deterministic SVG throughput charts."""

import json
import pathlib
import sys
from html import escape

BUSKER_COLOR = "#e85d04"
OTHER_COLOR = "#8d99ae"
WIDTH = 960
HEIGHT = 420
LEFT = 210
RIGHT = 70
TOP = 65
BOTTOM = 70


def number(value):
    return 0.0 if value is None else float(value)


def chart(scenario, output):
    results = [result for result in scenario["results"] if result["status"] == "ok"]
    excluded = [result for result in scenario["results"] if result["status"] != "ok"]
    results.sort(key=lambda result: number(result["measurements"]["requests-per-second"]), reverse=True)
    maximum = max((number(result["measurements"]["requests-per-second"]) for result in results), default=1.0)
    plot_width = WIDTH - LEFT - RIGHT
    plot_height = HEIGHT - TOP - BOTTOM - (30 if excluded else 0)
    step = plot_height / max(len(results), 1)
    rows = []

    for index, result in enumerate(results):
        value = number(result["measurements"]["requests-per-second"])
        y = TOP + index * step + step * 0.2
        width = plot_width * value / maximum
        color = BUSKER_COLOR if result["adapter"] == "busker" else OTHER_COLOR
        rows.extend(
            [
                f'<text x="{LEFT - 12}" y="{y + 20:.1f}" text-anchor="end">{escape(result["label"])}</text>',
                f'<rect x="{LEFT}" y="{y:.1f}" width="{width:.1f}" height="{step * 0.6:.1f}" fill="{color}"/>',
                f'<text x="{LEFT + width + 8:.1f}" y="{y + 20:.1f}">{value:,.0f}</text>',
            ]
        )

    title = f'{scenario["scenario"].capitalize()} handler throughput'
    output.write_text(
        "\n".join(
            [
                '<?xml version="1.0" encoding="UTF-8"?>',
                f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
                '<style>text { font-family: sans-serif; fill: #1f2937; font-size: 16px; }</style>',
                f'<text x="{LEFT}" y="35" font-size="24px">{title}</text>',
                f'<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" y2="{HEIGHT - BOTTOM}" stroke="#374151"/>',
                f'<line x1="{LEFT}" y1="{HEIGHT - BOTTOM}" x2="{WIDTH - RIGHT}" y2="{HEIGHT - BOTTOM}" stroke="#374151"/>',
                *rows,
                *([f'<text x="{LEFT}" y="{HEIGHT - 45}">Excluded: {escape(", ".join(result["label"] for result in excluded))}</text>'] if excluded else []),
                f'<text x="{LEFT}" y="{HEIGHT - 25}">Requests per second</text>',
                '</svg>',
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def main(source, destination):
    data = json.loads(pathlib.Path(source).read_text(encoding="utf-8"))
    destination = pathlib.Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    for scenario in data["scenarios"]:
        chart(scenario, destination / f'{scenario["scenario"]}-throughput.svg')


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: graph.py RESULTS.json OUTPUT-DIRECTORY")
    main(*sys.argv[1:])
