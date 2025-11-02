#!/usr/bin/env python3
import os
import json
import hashlib
import argparse

def sha256sum(file_path):
    """Compute SHA256 for a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    parser = argparse.ArgumentParser(description="Generate manifest.json for all backup folders.")
    parser.add_argument("--root", required=True, help="Root folder of all backups (e.g., local_backup)")
    args = parser.parse_args()

    manifest = {}

    for root, _, files in os.walk(args.root):
        for file in files:
            path = os.path.join(root, file)
            rel_path = os.path.relpath(path, start=args.root)
            manifest[rel_path.replace("\\", "/")] = sha256sum(path)

    with open("manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"Manifest generated with {len(manifest)} entries covering ADF, Databricks, dependencies, and results.")

if __name__ == "__main__":
    main()
