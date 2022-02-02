# register_application_commands.py
#
# Description
# ===========
#
# This application will register the Discord application commands
#   - /vote
#   - /nominate
#   - /peek
#   - /watch
#   - /here
# so that they are visible and accessible within Discord.
#
# Usage
# =====
#
# $ python register_application_commands.py

import requests
import json

f = open("../config.json")

config = json.load(f)
url = f'https://discord.com/api/v8/applications/{config["application_id"]["value"]}/commands'

commands = [
    {
        "name": "vote",
        "type": 1,
        "description": "Cast your vote for the next film to watch",
        "options": [
            {
                "name": "film",
                "description": "The film you want to vote for",
                "type": 3,
                "required": True,
                "autocomplete": True,
            }
        ],
    },
    {
        "name": "nominate",
        "type": 1,
        "description": "Nominate your next film",
        "options": [
            {
                "name": "film",
                "description": "The name of the film you would like to nominate",
                "type": 3,
                "required": True,
            }
        ],
    },
    {
        "name": "peek",
        "type": 1,
        "description": "Display the current set of nominations",
    },
    {
        "name": "watch",
        "type": 1,
        "description": "Indicate that the specified film is being watched and take attendance",
        "options": [
            {
                "name": "film",
                "description": "The film currently being watched",
                "type": 3,
                "required": True,
                "autocomplete": True,
            }
        ],
    },
    {
        "name": "here",
        "type": 1,
        "description": "Register attendance for the film currently being watched",
    },
]

headers = {"Authorization": f'Bot {config["bot_token"]["value"]}'}

for command in commands:
    r = requests.post(url, headers=headers, json=command)
    print(r.json())
