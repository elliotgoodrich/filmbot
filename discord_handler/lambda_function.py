# lambda_function.py
#
# Description
# ===========
#
# This script will be invoked when Discord sends application/slash commands
#
# Requirements
# ============
#
# The environment variable `FILMBOT_PUBLIC_KEY` must be set to the public key
# of the Discord Application

import os
import json
from discord_handler import handle_discord
from nacl.signing import VerifyKey
from nacl.exceptions import BadSignatureError

MESSAGE_WITH_SOURCE = 4


def verify_signature(event):
    header = event["params"]["header"]
    auth_sig = header["x-signature-ed25519"]
    auth_ts = header["x-signature-timestamp"]
    message = auth_ts.encode() + event["rawBody"].encode()
    verify_key = VerifyKey(bytes.fromhex(os.environ["FILMBOT_PUBLIC_KEY"]))
    try:
        verify_key.verify(message, bytes.fromhex(auth_sig))
    except Exception as e:
        raise Exception(f"[UNAUTHORIZED] Invalid request signature: {e}")


def lambda_handler(event, context):
    print(f"in={json.dumps(event)}")
    verify_signature(event)
    response = handle_discord(event, os.environ["AWS_REGION"])
    print(f"out={json.dumps(response)}")
    return response
