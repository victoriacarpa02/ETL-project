#!/usr/bin/env python3
import os
import argparse
import requests

def restore_notebooks(host, token, source_dir):
    headers = {"Authorization": f"Bearer {token}"}
    for root, _, files in os.walk(source_dir):
        for f in files:
            local_path = os.path.join(root, f)
            db_path = os.path.join("/", os.path.relpath(local_path, source_dir)).replace("\\", "/").replace(".dbc", "")
            print(f"Uploading notebook {f} → {db_path}")
            with open(local_path, "r", encoding="utf-8") as fh:
                content = fh.read()
            data = {
                "path": db_path,
                "language": "PYTHON",
                "overwrite": True,
                "content": content.encode("utf-8").decode("utf-8")
            }
            r = requests.post(f"{host}/api/2.0/workspace/import", headers=headers, json=data)
            if r.status_code != 200:
                print(f"Failed to upload {f}: {r.status_code} {r.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Restore Databricks notebooks from backup.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    restore_notebooks(args.host, args.token, args.source)
