# delete_command.py
#
# Description
# ===========
#
# This application will remove a particular command.
#
# Usage
# =====
#
# $ python delete_command.py [CommandID]
# $ <Response [204]>

import requests
import json
import sys

f = open("../config.json")

config = json.load(f)
commandID = sys.argv[1]
url = f'https://discord.com/api/v10/applications/{config["application_id"]["value"]}/commands/{commandID}'
headers = {"Authorization": f'Bot {config["bot_token"]["value"]}'}
print(requests.delete(url, headers=headers))
