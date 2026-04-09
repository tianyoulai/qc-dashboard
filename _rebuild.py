#!/usr/bin/env python3
"""Rebuild index.html — clean single-file dashboard with correct execution order"""
import re

import os
BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, 'dashboard', 'index.html')

with open(SRC, 'r') as f:
    html = f.read()

# Extract parts
css_match = re.search(r'<style>(.*?)</style>', html, re.DOTALL)
css = css_match.group(1)

body_start = html.find('<body>') + 6
first_script = html.find('<script>', body_start)
body_html = html[body_start:first_script]

db_match = re.search(r'const DB_DATA = (\{.*?\});', html, re.DOTALL)
db_data = db_match.group(1)

print(f"Extracted: CSS={len(css)}, Body={len(body_html)}, Data={len(db_data)}")

# Read the JS logic from a separate file (to avoid Python quoting issues)
with open(os.path.join(BASE, '_dashboard_logic.js'), 'r') as f:
    js_logic = f.read()

with open(os.path.join(BASE, '_boot.js'), 'r') as f:
    boot = f.read()

parts = [
    '<!DOCTYPE html>\n<html lang="zh-CN">\n<head>',
    '\n<meta charset="UTF-8">',
    '\n<meta name="viewport" content="width=device-width, initial-scale=1.0">',
    '\n<title>质检数据看板</title>',
    '\n<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>',
    f'\n<style>\n{css}\n</style>',
    '\n</head>\n<body>',
    body_html,
    '\n<script>\n// === ① DATA ===\n',
    f'const DB_DATA = {db_data};\n\n',
    js_logic,
    '\n// === BOOT ===\n',
    boot,
    '\n</script>\n</body>\n</html>'
]

with open(SRC, 'w') as f:
    f.write('\n'.join(parts))

print(f'Done! {len("".join(parts))//1024}KB')
