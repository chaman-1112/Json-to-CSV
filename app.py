import json
import csv
import io
import os
import ijson
import requests
from flask import Flask, send_file, request, jsonify, Response, stream_with_context
from urllib.parse import urlparse

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROXY_CHUNK = 64 * 1024  # 64 KB chunks for streaming proxy


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


def find_array_prefix(stream):
    """Use ijson to find the prefix of the first top-level array in the JSON."""
    parser = ijson.parse(stream)
    path_stack = []
    for prefix, event, value in parser:
        if event == "start_array":
            return prefix
        if event == "start_map":
            path_stack.append(prefix)
            continue
        if event in ("map_key", "string", "number", "boolean", "null"):
            continue
        if event == "end_map":
            if path_stack:
                path_stack.pop()
            continue
    return ""


def stream_csv_from_file(stream, filename):
    """Two-pass streaming: first pass collects all keys, second pass writes CSV rows."""
    raw = stream.read()

    columns = set()
    for item in ijson.items(io.BytesIO(raw), "item"):
        flat = flatten(item)
        columns.update(flat.keys())
    columns = sorted(columns)

    if not columns:
        return None, "JSON must contain a non-empty array of objects."

    def generate():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate(0)

        for item in ijson.items(io.BytesIO(raw), "item"):
            flat = flatten(item)
            writer.writerow(flat)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)

    return Response(
        stream_with_context(generate()),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    ), None


@app.route("/")
def index():
    return send_file(os.path.join(BASE_DIR, "index.html"))


@app.route("/from-json", methods=["POST"])
def from_json():
    """Called by the bookmarklet — receives raw JSON text, returns CSV download."""
    raw = (request.form.get("json_text") or "").strip()
    filename = (request.form.get("filename") or "converted").strip() + ".csv"
    if not raw:
        return "No JSON data received.", 400
    try:
        json.loads(raw[:4096])
    except json.JSONDecodeError:
        pass

    stream = io.BytesIO(raw.encode("utf-8"))
    resp, err = stream_csv_from_file(stream, filename)
    if err:
        return err, 422
    return resp


@app.route("/proxy", methods=["POST"])
def proxy():
    """Streaming proxy — forwards the remote JSON to the browser chunk-by-chunk.
    The browser handles all parsing/conversion client-side."""
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
        upstream = requests.get(
            url, timeout=300, cookies=cookie_jar or None,
            headers=headers, stream=True,
        )
        upstream.raise_for_status()
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

    peek = next(upstream.iter_content(1024), b"")
    if b"<html" in peek[:1000].lower() or b"<!doctype" in peek[:1000].lower():
        upstream.close()
        return jsonify(error=(
            "Got a login page instead of JSON. "
            "Use the Bookmarklet — it fetches from your browser where you're already logged in."
        )), 401

    def stream_chunks():
        yield peek
        for chunk in upstream.iter_content(PROXY_CHUNK):
            yield chunk
        upstream.close()

    return Response(
        stream_with_context(stream_chunks()),
        content_type=upstream.headers.get("Content-Type", "application/json"),
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
