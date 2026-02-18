import json
import csv
import sys
import os
import time
import requests
from urllib.parse import urlparse


def is_url(path):
    """Check if the given string is a URL."""
    return path.startswith("http://") or path.startswith("https://")


def flatten(obj, parent_key="", sep="."):
    """Flatten a nested dict/list into a single-level dict with dot-notation keys."""
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


def fetch_json_from_url(url):
    """Fetch JSON data from a URL."""
    print(f"Fetching JSON from URL: {url}")
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        sys.exit(1)

    content_mb = len(response.content) / (1024 * 1024)
    print(f"Downloaded {content_mb:.1f} MB")

    try:
        return response.json()
    except json.JSONDecodeError as e:
        print(f"Error: Response is not valid JSON -> {e}")
        sys.exit(1)


def csv_name_from_url(url):
    """Derive a CSV filename from the URL."""
    parsed = urlparse(url)
    name = os.path.basename(parsed.path)
    if name:
        name = os.path.splitext(name)[0]
    if not name:
        name = parsed.netloc.replace(".", "_")
    return name + ".csv"


def convert(source, csv_path=None):
    start = time.time()

    if is_url(source):
        data = fetch_json_from_url(source)
        if csv_path is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            csv_path = os.path.join(script_dir, csv_name_from_url(source))
    else:
        json_path = source
        if not os.path.isfile(json_path):
            print(f"Error: File not found -> {json_path}")
            sys.exit(1)

        file_size = os.path.getsize(json_path) / (1024 * 1024)
        print(f"Reading {json_path} ({file_size:.1f} MB) ...")

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

    if isinstance(data, dict):
        # Recursively dig into nested dicts to find the first large array
        path_parts = []
        node = data
        while isinstance(node, dict):
            found = False
            for key, value in node.items():
                if isinstance(value, list) and len(value) > 0:
                    path_parts.append(key)
                    print(f"Found array at '{'.'.join(path_parts)}' with {len(value)} items.")
                    node = value
                    found = True
                    break
                elif isinstance(value, dict):
                    path_parts.append(key)
                    node = value
                    found = True
                    break
            if not found:
                break
        data = node if isinstance(node, list) else [data]

    if not isinstance(data, list):
        print("Error: JSON root is not an array or an object containing an array.")
        sys.exit(1)

    print(f"Loaded {len(data)} records in {time.time() - start:.1f}s. Flattening ...")

    # Flatten all rows and collect every column name
    flat_rows = []
    all_keys = set()
    for i, row in enumerate(data):
        flat = flatten(row)
        flat_rows.append(flat)
        all_keys.update(flat.keys())
        if (i + 1) % 50_000 == 0:
            print(f"  Processed {i + 1} / {len(data)} rows ...")

    # Sort columns for consistent output
    columns = sorted(all_keys)

    if csv_path is None:
        csv_path = os.path.splitext(json_path)[0] + ".csv"

    print(f"Writing {len(flat_rows)} rows x {len(columns)} columns -> {csv_path}")

    # Write to a temp file first (this always succeeds)
    tmp_path = csv_path + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat_rows)

    # Try to replace the target file; if it's locked, save with an alternate name
    try:
        if os.path.exists(csv_path):
            os.remove(csv_path)
        os.rename(tmp_path, csv_path)
    except PermissionError:
        alt_path = csv_path.replace(".csv", f"_{int(time.time())}.csv")
        os.rename(tmp_path, alt_path)
        print(f"  {os.path.basename(csv_path)} is locked by another program.")
        print(f"  Saved as: {os.path.basename(alt_path)}")
        csv_path = alt_path

    out_size = os.path.getsize(csv_path) / (1024 * 1024)
    elapsed = time.time() - start
    print(f"Done! {out_size:.1f} MB written in {elapsed:.1f}s")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # No argument — ask for URL or auto-detect local .json files
        source = input("Paste a JSON URL (or press Enter to auto-detect local files): ").strip()

        if not source:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            json_files = [f for f in os.listdir(script_dir) if f.lower().endswith(".json")]
            if len(json_files) == 0:
                print("Usage:  python convert.py <url_or_file> [output.csv]")
                print("   or:  Drop a .json file in the same folder and run without arguments.")
                sys.exit(1)
            elif len(json_files) == 1:
                source = os.path.join(script_dir, json_files[0])
                print(f"Auto-detected: {json_files[0]}")
            else:
                print("Multiple .json files found. Pick one:")
                for i, f in enumerate(json_files, 1):
                    size = os.path.getsize(os.path.join(script_dir, f)) / (1024 * 1024)
                    print(f"  {i}. {f} ({size:.1f} MB)")
                choice = input("Enter number: ").strip()
                source = os.path.join(script_dir, json_files[int(choice) - 1])
    else:
        source = sys.argv[1]

    csv_path = sys.argv[2] if len(sys.argv) > 2 else None
    convert(source, csv_path)
