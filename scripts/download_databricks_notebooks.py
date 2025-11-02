#!/usr/bin/env python3
import os
import argparse
import requests

def download_notebooks(host, token, output_dir):
    headers = {"Authorization": f"Bearer {token}"}
    os.makedirs(output_dir, exist_ok=True)
    response = requests.get(f"{host}/api/2.0/workspace/list?path=/", headers=headers)
    response.raise_for_status()
    items = response.json().get("objects", [])
    for item in items:
        if item["object_type"] == "NOTEBOOK":
            path = item["path"]
            name = os.path.basename(path)
            out_file = os.path.join(output_dir, f"{name}.dbc")
            print(f"Downloading {path} -> {out_file}")
            r = requests.get(f"{host}/api/2.0/workspace/export", headers=headers, params={"path": path, "format": "SOURCE"})
            r.raise_for_status()
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(r.text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download all Databricks notebooks.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    download_notebooks(args.host, args.token, args.output)
