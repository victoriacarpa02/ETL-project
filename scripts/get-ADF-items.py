#!/usr/bin/env python3
"""
This script retrieves Azure Data Factory items (pipelines, datasets, linked services, triggers)
via REST API using ClientSecretCredential.
It is intended for use until ADF is directly connected to Git.
"""

import argparse
import json
import os
import sys
import requests
from azure.identity import ClientSecretCredential


def get_adf_item(subscription_id, rg_name, adf_name, file_type, item_name, token):
    """Generic REST GET for ADF item."""
    base_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{rg_name}/providers/Microsoft.DataFactory/factories/{adf_name}"
    endpoint_map = {
        "pipeline": "pipelines",
        "dataset": "datasets",
        "linkedService": "linkedServices",
        "trigger": "triggers"
    }

    if file_type not in endpoint_map:
        raise ValueError(f"Unsupported file_type: {file_type}")

    url = f"{base_url}/{endpoint_map[file_type]}/{item_name}?api-version=2018-06-01"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise RuntimeError(f"[{response.status_code}] Failed to retrieve {file_type} '{item_name}': {response.text}")


def main():
    parser = argparse.ArgumentParser(description="Backup Data Factory items by list (v2).")
    parser.add_argument("--subscription_id", required=True)
    parser.add_argument("--tenant_id", required=True)
    parser.add_argument("--client_id", required=True)
    parser.add_argument("--client_secret", required=True)
    parser.add_argument("--resource_group_name", required=True)
    parser.add_argument("--data_factory_name", required=True)
    parser.add_argument("--item_list", required=True, help="Path to text file with item names")
    parser.add_argument("--file_type", required=True, choices=["pipeline", "dataset", "linkedService", "trigger"])
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--fail_if_absent", required=True, choices=["true", "false"])
    args = parser.parse_args()

    # Prepare auth
    print("Authenticating with Azure AD...")
    credential = ClientSecretCredential(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret
    )
    token = credential.get_token("https://management.azure.com/.default").token

    # Load item list
    with open(args.item_list, "r") as f:
        item_names = [line.strip() for line in f if line.strip()]

    if not item_names:
        print("⚠️ No items found in list file.")
        sys.exit(0)

    os.makedirs(args.output_dir, exist_ok=True)

    success, failed = [], []
    for name in item_names:
        clean_name = os.path.splitext(os.path.basename(name))[0]
        try:
            item_data = get_adf_item(
                subscription_id=args.subscription_id,
                rg_name=args.resource_group_name,
                adf_name=args.data_factory_name,
                file_type=args.file_type,
                item_name=clean_name,
                token=token
            )
            output_path = os.path.join(args.output_dir, f"{clean_name}.json")
            with open(output_path, "w", encoding="utf-8") as out:
                json.dump(item_data, out, indent=2, ensure_ascii=False)
            success.append(clean_name)
            print(f"##[debug] Retrieved {args.file_type}: {clean_name}")
        except Exception as e:
            print(f"Error retrieving {args.file_type} '{clean_name}': {e}")
            failed.append(clean_name)
            if args.fail_if_absent.lower() == "true":
                print("##[error] Fail_if_absent=True, stopping execution.")
                sys.exit(1)

    print("\n========== Summary ==========")
    print(f"Retrieved: {len(success)}")
    print(f"Failed: {len(failed)}")
    if failed:
        print("Failed items:", ", ".join(failed))


if __name__ == "__main__":
    main()
