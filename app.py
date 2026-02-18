import json
import csv
import io
import time
import hashlib
import requests
from urllib.parse import urlparse
from flask import Flask, request, render_template_string, Response, stream_with_context

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>JSON to CSV Converter</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
    background: #0f0f1a;
    color: #e0e0e0;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .card {
    background: #1a1a2e;
    border: 1px solid #2a2a4a;
    border-radius: 16px;
    padding: 48px 40px;
    width: 100%;
    max-width: 560px;
    box-shadow: 0 20px 60px rgba(0,0,0,.4);
  }
  h1 {
    font-size: 1.75rem;
    font-weight: 700;
    margin-bottom: 8px;
    background: linear-gradient(135deg, #60a5fa, #a78bfa);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .subtitle { color: #888; font-size: .9rem; margin-bottom: 32px; }
  label { display: block; font-weight: 600; margin-bottom: 8px; font-size: .95rem; }
  input[type="text"] {
    width: 100%;
    padding: 14px 16px;
    border: 1px solid #2a2a4a;
    border-radius: 10px;
    background: #12121f;
    color: #e0e0e0;
    font-size: 1rem;
    outline: none;
    transition: border-color .2s;
  }
  input[type="text"]:focus { border-color: #60a5fa; }
  input[type="text"]::placeholder { color: #555; }
  button {
    margin-top: 20px;
    width: 100%;
    padding: 14px;
    border: none;
    border-radius: 10px;
    background: linear-gradient(135deg, #3b82f6, #7c3aed);
    color: #fff;
    font-size: 1rem;
    font-weight: 600;
    cursor: pointer;
    transition: opacity .2s;
  }
  button:hover { opacity: .9; }
  button:disabled { opacity: .5; cursor: wait; }
  .error {
    margin-top: 16px;
    padding: 12px 16px;
    background: #2d1b1b;
    border: 1px solid #5c2a2a;
    border-radius: 10px;
    color: #f87171;
    font-size: .9rem;
  }
  .spinner {
    display: inline-block;
    width: 16px; height: 16px;
    border: 2px solid #fff4;
    border-top-color: #fff;
    border-radius: 50%;
    animation: spin .6s linear infinite;
    vertical-align: middle;
    margin-right: 8px;
  }
  @keyframes spin { to { transform: rotate(360deg); } }
  footer { margin-top: 32px; text-align: center; color: #555; font-size: .8rem; }
</style>
</head>
<body>
<div class="card">
  <h1>JSON &rarr; CSV</h1>
  <p class="subtitle">Paste a JSON URL and download the flattened CSV instantly.</p>
  <form id="form" action="/convert" method="POST">
    <label for="url">JSON URL</label>
    <input type="text" id="url" name="url" placeholder="https://api.example.com/data.json" required autocomplete="off">
    <button type="submit" id="btn">Convert &amp; Download</button>
  </form>
  {% if error %}
  <div class="error">{{ error }}</div>
  {% endif %}
  <footer>Flattens nested JSON arrays into clean CSV files.</footer>
</div>
<script>
  const form = document.getElementById('form');
  const btn = document.getElementById('btn');
  form.addEventListener('submit', () => {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>Converting…';
    setTimeout(() => { btn.disabled = false; btn.textContent = 'Convert & Download'; }, 30000);
  });
</script>
</body>
</html>
"""


def flatten(obj, parent_key="", sep="."):
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten(v, new_key, sep))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{parent_key}[{i}]"
            items.update(flatten(v, new_key, sep))
    else:
        items[parent_key] = obj
    return items


def find_array(data):
    """Dig into nested dicts to find the first large list."""
    node = data
    while isinstance(node, dict):
        found = False
        for value in node.values():
            if isinstance(value, list) and len(value) > 0:
                return value
            if isinstance(value, dict):
                node = value
                found = True
                break
        if not found:
            break
    return node if isinstance(node, list) else [data]


def csv_name_from_url(url):
    parsed = urlparse(url)
    import os
    name = os.path.basename(parsed.path)
    if name:
        name = os.path.splitext(name)[0]
    if not name:
        name = parsed.netloc.replace(".", "_")
    return name + ".csv"


@app.route("/")
def index():
    return render_template_string(HTML)


@app.route("/convert", methods=["POST"])
def convert():
    url = (request.form.get("url") or "").strip()
    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return render_template_string(HTML, error="Please enter a valid URL starting with http:// or https://")

    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        return render_template_string(HTML, error=f"Could not fetch URL: {e}")
    except json.JSONDecodeError:
        return render_template_string(HTML, error="The URL did not return valid JSON.")

    if isinstance(data, dict):
        data = find_array(data)
    if not isinstance(data, list):
        return render_template_string(HTML, error="JSON must be an array or contain one.")

    flat_rows = []
    all_keys = set()
    for row in data:
        flat = flatten(row)
        flat_rows.append(flat)
        all_keys.update(flat.keys())

    columns = sorted(all_keys)
    filename = csv_name_from_url(url)

    def generate():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        for row in flat_rows:
            writer.writerow(row)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
