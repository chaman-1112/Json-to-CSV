import json
import csv
import io
import requests
from flask import Flask, send_file, request, jsonify, Response, stream_with_context
from urllib.parse import urlparse

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024


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


def build_csv_response(data, filename):
    if isinstance(data, dict):
        data = find_array(data)
    if not isinstance(data, list) or len(data) == 0:
        return None, "JSON must contain a non-empty array."

    flat_rows = []
    all_keys = set()
    for row in data:
        flat = flatten(row)
        flat_rows.append(flat)
        all_keys.update(flat.keys())

    columns = sorted(all_keys)

    def generate():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)
        for row in flat_rows:
            writer.writerow(row)
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    ), None


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/from-json", methods=["POST"])
def from_json():
    """Called by the bookmarklet — receives raw JSON text, returns CSV download."""
    raw = (request.form.get("json_text") or "").strip()
    filename = (request.form.get("filename") or "converted").strip() + ".csv"
    if not raw:
        return "No JSON data received.", 400
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        return f"Invalid JSON: {e}", 400

    resp, err = build_csv_response(data, filename)
    if err:
        return err, 422
    return resp


@app.route("/proxy", methods=["POST"])
def proxy():
    """Server-side fetch for URLs that block direct browser requests (CORS)."""
    body = request.get_json(silent=True) or {}
    url = (body.get("url") or "").strip()
    cookies_str = (body.get("cookies") or "").strip()

    if not url or not (url.startswith("http://") or url.startswith("https://")):
        return jsonify(error="Invalid URL."), 400

    cookie_jar = {}
    if cookies_str:
        for pair in cookies_str.split(";"):
            pair = pair.strip()
            if "=" in pair:
                k, v = pair.split("=", 1)
                cookie_jar[k.strip()] = v.strip()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
    }

    try:
        resp = requests.get(url, timeout=120, cookies=cookie_jar or None, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        return jsonify(error=(
            "Server could not reach that URL. The site may be blocking server requests. "
            "Use the Bookmarklet instead — it fetches directly from your browser."
        )), 502
    except requests.exceptions.Timeout:
        return jsonify(error="Request timed out."), 504
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        if code in (401, 403):
            return jsonify(error=(
                f"Access denied (HTTP {code}). "
                "Use the Bookmarklet instead — it runs in your browser where you're already logged in."
            )), 403
        return jsonify(error=f"HTTP error {code}."), 502
    except requests.exceptions.RequestException:
        return jsonify(error="Failed to fetch the URL."), 502

    raw = resp.content
    if b"<html" in raw[:1000].lower() or b"<!doctype" in raw[:1000].lower():
        return jsonify(error=(
            "Got a login page instead of JSON. "
            "Use the Bookmarklet — it fetches from your browser where you're already logged in."
        )), 401

    return Response(raw, content_type="application/json")


if __name__ == "__main__":
    app.run(debug=True, port=5000)
