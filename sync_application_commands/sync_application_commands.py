# sync_application_commands.py
#
# Description
# ===========
#
# Syncs current Discord application commands with commands.json:
#   - Updates commands registered with Discord that differ from commands.json
#   - Deletes commands registered with Discord that are not in commands.json
#   - Adds commands in commands.json that are not yet registered with Discord
#
# Usage
# =====
#
# $ python sync_application_commands.py --application_id 123 --bot_token abc --commands commands.json

import argparse
import requests
import json
import sys
import time

parser = argparse.ArgumentParser(
    description="Sync Discord application commands"
)
parser.add_argument(
    "--application_id", required=True, help="Discord application ID"
)
parser.add_argument("--bot_token", required=True, help="Discord bot token")
parser.add_argument("--commands", required=True, help="Path to commands.json")
parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Print what would change without making any modifications",
)
args = parser.parse_args()


def request_with_retry(method, url, **kwargs):
    while True:
        r = requests.request(method, url, **kwargs)
        if r.status_code == 429:
            retry_after = float(r.headers.get("Retry-After", 1))
            print(f"Rate limited, retrying after {retry_after}s")
            time.sleep(retry_after)
        else:
            r.raise_for_status()
            return r


with open(args.commands) as f:
    desired_commands = json.load(f)

url = (
    f"https://discord.com/api/v10/applications/{args.application_id}/commands"
)
headers = {"Authorization": f"Bot {args.bot_token}"}

existing_commands = request_with_retry("GET", url, headers=headers).json()

actual = {cmd["name"]: cmd for cmd in existing_commands}
expected = {cmd["name"]: cmd for cmd in desired_commands}


def commands_differ(actual_cmd, expected_cmd):
    return any(actual_cmd.get(k) != v for k, v in expected_cmd.items())


to_remove = actual.keys() - expected.keys()
to_add = expected.keys() - actual.keys()
to_update = {
    name
    for name in actual.keys() & expected.keys()
    if commands_differ(actual[name], expected[name])
}

if not to_remove and not to_add and not to_update:
    print("Commands are already in sync")
    sys.exit(0)

for name in to_update:
    if args.dry_run:
        print(f"Would update /{name}")
    else:
        request_with_retry(
            "PATCH",
            f'{url}/{actual[name]["id"]}',
            headers=headers,
            json=expected[name],
        )
        print(f"Updated /{name}")

for name in to_remove:
    if args.dry_run:
        print(f"Would delete /{name}")
    else:
        request_with_retry(
            "DELETE", f'{url}/{actual[name]["id"]}', headers=headers
        )
        print(f"Deleted /{name}")

for name in to_add:
    if args.dry_run:
        print(f"Would add /{name}")
    else:
        request_with_retry("POST", url, headers=headers, json=expected[name])
        print(f"Added /{name}")
