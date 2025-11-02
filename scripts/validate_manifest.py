#!/usr/bin/env python3
import os
import json
import hashlib
import argparse
import sys

def sha256sum(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    parser = argparse.ArgumentParser(description="Validate manifest.json against actual files.")
    parser.add_argument("--root", required=True, help="Root directory of extracted backup (e.g., restore_workspace)")
    parser.add_argument("--manifest", required=True, help="Path to manifest.json file")
    args = parser.parse_args()

    with open(args.manifest, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    print(f"Validating {len(manifest)} files listed in manifest.json...")

    mismatches = []
    missing = []

    for rel_path, recorded_hash in manifest.items():
        abs_path = os.path.join(args.root, rel_path)
        if not os.path.exists(abs_path):
            missing.append(rel_path)
            continue
        actual_hash = sha256sum(abs_path)
        if actual_hash != recorded_hash:
            mismatches.append(rel_path)

    if missing or mismatches:
        print("Integrity check failed.")
        if missing:
            print(f"Missing files ({len(missing)}):")
            for f in missing:
                print(f"  - {f}")
        if mismatches:
            print(f"Checksum mismatches ({len(mismatches)}):")
            for f in mismatches:
                print(f"  - {f}")
        sys.exit(1)
    else:
        print("All files match manifest checksums. Backup integrity confirmed.")

if __name__ == "__main__":
    main()
