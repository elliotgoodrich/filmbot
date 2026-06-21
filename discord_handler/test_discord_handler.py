import os
import unittest
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_dynamodb
from discord_handler import (
    handle_discord,
    DiscordRequest,
    DiscordResponse,
    DiscordFlag,
    DiscordStyle,
    DiscordMessageComponent,
    MessageComponentID,
)
from filmbot import TABLE_NAME, key_map

AWS_REGION = "eu-west-2"

SHAME_COMPONENT = {
    "type": DiscordMessageComponent.ACTION_ROW,
    "components": [
        {
            "type": DiscordMessageComponent.BUTTON,
            "label": "Publicly Shame",
            "style": DiscordStyle.DANGER,
            "custom_id": MessageComponentID.SHAME,
        }
    ],
}


def set_db(client, data):
    try:
        client.delete_table(TableName=TABLE_NAME)
    except Exception:
        pass

    client.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    if not data:
        return

    # Add data
    items = []
    for guild_id in data:
        for record in data[guild_id]:
            keyed = key_map(record)
            keyed["PK"] = {"S": guild_id}
            items.append(
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": keyed,
                    }
                }
            )

    client.transact_write_items(TransactItems=items)


class TestDiscordHandler(unittest.TestCase):
    mock_dynamodb = mock_dynamodb()

    def setUp(self):
        """
        Mock `dynamodb2` and create the tables we expect.
        """

        # Set unlimited length for assertEqual diff lengths
        self.maxDiff = None

        self.mock_dynamodb.start()
        boto3.setup_default_session()
        self.dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        self.dynamodb_client = boto3.client("dynamodb", region_name=AWS_REGION)

        set_db(self.dynamodb_client, {})

        # Check all tables have been created
        self.assertEqual(
            self.dynamodb_client.list_tables()["TableNames"],
            [TABLE_NAME],
        )
        pass

    def tearDown(self):
        """
        Unmock `dynamodb2`.
        """
        self.mock_dynamodb.stop()
        pass

    def test_invalid_message(self):
        self.assertRaises(
            Exception,
            lambda: handle_discord({"body-json": {"type": -1}}, None),
        )

    def test_ping(self):
        self.assertEqual(
            handle_discord({"body-json": {"type": DiscordRequest.PING}}, None),
            {"type": DiscordResponse.PONG},
        )

    def test_maintenance_mode(self):
        maintenance_message = {
            "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": "The bot is currently under maintenance. Please try again later.",
                "flags": DiscordFlag.EPHEMERAL_FLAG,
            },
        }

        with patch.dict(os.environ, {"MAINTENANCE_MODE": "1"}):
            # PING still responds normally
            self.assertEqual(
                handle_discord(
                    {"body-json": {"type": DiscordRequest.PING}}, None
                ),
                {"type": DiscordResponse.PONG},
            )
            # APPLICATION_COMMAND returns maintenance message
            self.assertEqual(
                handle_discord(
                    {
                        "body-json": {
                            "type": DiscordRequest.APPLICATION_COMMAND,
                            "data": {"name": "peek"},
                            "guild_id": "123",
                            "member": {"user": {"id": "abc"}},
                        }
                    },
                    None,
                ),
                maintenance_message,
            )
            # MESSAGE_COMPONENT returns maintenance message
            self.assertEqual(
                handle_discord(
                    {
                        "body-json": {
                            "type": DiscordRequest.MESSAGE_COMPONENT,
                            "data": {
                                "component_type": DiscordMessageComponent.BUTTON,
                                "custom_id": MessageComponentID.SHAME,
                            },
                            "guild_id": "123",
                        }
                    },
                    None,
                ),
                maintenance_message,
            )
            # APPLICATION_COMMAND_AUTOCOMPLETE returns empty choices
            self.assertEqual(
                handle_discord(
                    {
                        "body-json": {
                            "type": DiscordRequest.APPLICATION_COMMAND_AUTOCOMPLETE,
                            "data": {"name": "vote"},
                            "guild_id": "123",
                            "member": {"user": {"id": "abc"}},
                        }
                    },
                    None,
                ),
                {
                    "type": DiscordResponse.APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
                    "data": {"choices": []},
                },
            )

        # Empty string does not trigger maintenance mode
        with patch.dict(os.environ, {"MAINTENANCE_MODE": ""}, clear=False):
            self.assertEqual(
                handle_discord(
                    {
                        "body-json": {
                            "type": DiscordRequest.APPLICATION_COMMAND,
                            "data": {"name": "peek"},
                            "guild_id": "123",
                            "member": {"user": {"id": "abc"}},
                        }
                    },
                    self.dynamodb_client,
                ),
                {
                    "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                    "data": {
                        "content": "There are no current nominations. Each user can nominate with the `/nominate` command.",
                        "flags": DiscordFlag.EPHEMERAL_FLAG,
                    },
                },
            )

    def test_workflow(self):
        # 1. Check /peek and /history with an empty DB
        # 2. Check /nominate
        # 3. Check /vote
        # 4. Check /watch
        # 5. Check shame button
        # 6. Check /here (TODO: Also check the application command)

        # 1. Check /peek and /history with an empty DB
        # /peek
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "peek",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "There are no current nominations. Each user can nominate with the `/nominate` command.",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /history
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "history",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "No films have yet been watched.",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )

        # 2. Check /nominate
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "nominate",
                            "options": [{"value": "My Film Name"}],
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "<@abc> has successfully nominated My Film Name.\n"
                    + "\n"
                    + "The current list of nominations are:\n"
                    + "1. <@abc> My Film Name (0 votes)",
                },
            },
        )

        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "nominate",
                            "options": [{"value": "My Film Name 2"}],
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "Unable to nominate a film as you have already nominated one",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {"imdb_id": "tt012345"}
        with patch("discord_handler.requests.get", return_value=mock_response):
            self.assertEqual(
                handle_discord(
                    {
                        "body-json": {
                            "type": DiscordRequest.APPLICATION_COMMAND,
                            "data": {
                                "name": "nominate",
                                "options": [
                                    {"value": "TMDB:123456:My Other Film"}
                                ],
                            },
                            "guild_id": "123",
                            "member": {
                                "user": {
                                    "id": "def",
                                },
                            },
                        }
                    },
                    self.dynamodb_client,
                ),
                {
                    "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                    "data": {
                        "content": "<@def> has successfully nominated My Other Film.\n"
                        + "\n"
                        + "The current list of nominations are:\n"
                        + "1. <@abc> My Film Name (0 votes)\n"
                        + "2. <@def> My Other Film (0 votes) [IMDB](<https://imdb.com/title/tt012345>)",
                    },
                },
            )

        # /peek
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "peek",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "The current list of nominations are:\n"
                    + "1. My Film Name (0 🗳) <@abc>\n"
                    + "2. [My Other Film](<https://imdb.com/title/tt012345>) (0 🗳) <@def>\n\n"
                    + "and these users need to vote:\n"
                    + "- <@abc>\n"
                    + "- <@def>",
                    "components": [SHAME_COMPONENT],
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /history
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "history",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "No films have yet been watched.",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )

        # 3. Check /vote
        # Get autocomplete
        actual = handle_discord(
            {
                "body-json": {
                    "type": DiscordRequest.APPLICATION_COMMAND_AUTOCOMPLETE,
                    "data": {
                        "name": "vote",
                    },
                    "guild_id": "123",
                    "member": {
                        "user": {
                            "id": "def",
                        },
                    },
                }
            },
            self.dynamodb_client,
        )
        filmguid = actual["data"]["choices"][0]["value"]
        self.assertEqual(
            actual,
            {
                "type": DiscordResponse.APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
                "data": {
                    "choices": [{"name": "My Film Name", "value": filmguid}],
                },
            },
        )

        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "vote",
                            "options": [
                                {"value": filmguid},
                            ],
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "def",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "<@def> has voted for My Film Name",
                },
            },
        )
        # /peek
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "peek",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "The current list of nominations are:\n"
                    + "1. My Film Name (1 🗳) <@abc>\n"
                    + "2. [My Other Film](<https://imdb.com/title/tt012345>) (0 🗳) <@def>\n\n"
                    + "and these users need to vote:\n"
                    + "- <@abc>",
                    "components": [SHAME_COMPONENT],
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /history
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "history",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "No films have yet been watched.",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )

        # 4. Check /watch
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "watch",
                            "options": [
                                {"value": filmguid},
                            ],
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "def",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "Started watching My Film Name!\n\n"
                    + "Everyone other than <@def> should record their attendance below or using `/here`.\n\n"
                    + "<@abc> can now nominated their next suggestion with `/nominate`.\n",
                    "components": [
                        {
                            "type": DiscordMessageComponent.ACTION_ROW,
                            "components": [
                                {
                                    "type": DiscordMessageComponent.BUTTON,
                                    "label": "Register Attendance",
                                    "style": DiscordStyle.PRIMARY,
                                    "custom_id": MessageComponentID.ATTENDANCE,
                                }
                            ],
                        },
                    ],
                },
            },
        )
        # /peek
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "peek",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "The current list of nominations are:\n"
                    + "1. [My Other Film](<https://imdb.com/title/tt012345>) (1 🗳) <@def>\n"
                    + "2. [No nomination] <@abc>\n\n"
                    + "and these users need to vote:\n"
                    + "- <@def>\n"
                    + "- <@abc>",
                    "components": [SHAME_COMPONENT],
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /history
        actual = handle_discord(
            {
                "body-json": {
                    "type": DiscordRequest.APPLICATION_COMMAND,
                    "data": {
                        "name": "history",
                    },
                    "guild_id": "123",
                    "member": {
                        "user": {
                            "id": "abc",
                        },
                    },
                }
            },
            self.dynamodb_client,
        )
        self.assertEqual(
            actual,
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": actual["data"]["content"],
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        self.assertRegex(actual["data"]["content"], "My Film Name")
        self.assertRegex(actual["data"]["content"], "<@abc>")

        # 5. Check shame button
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.MESSAGE_COMPONENT,
                        "data": {
                            "component_type": DiscordMessageComponent.BUTTON,
                            "custom_id": MessageComponentID.SHAME,
                        },
                        "guild_id": "123",
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "These users need to nominate:\n- <@abc>\n\n"
                    + "These users need to vote:\n- <@abc>\n- <@def>"
                },
            },
        )

        # 6. Check /here
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "here",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "<@abc> has attended",
                },
            },
        )
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "peek",
                        },
                        "guild_id": "123",
                        "member": {
                            "user": {
                                "id": "abc",
                            },
                        },
                    }
                },
                self.dynamodb_client,
            ),
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": "The current list of nominations are:\n"
                    + "1. [My Other Film](<https://imdb.com/title/tt012345>) (1 🗳) <@def>\n"
                    + "2. [No nomination] <@abc>\n\n"
                    + "and these users need to vote:\n"
                    + "- <@def>\n"
                    + "- <@abc>",
                    "components": [SHAME_COMPONENT],
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )

    def test_retire_user(self):
        from datetime import datetime, timedelta
        from filmbot import key_map

        guild_id = "retire-guild"
        user_id = "user-to-retire"
        other_user_id = "other-user"
        film_id = "film-abc"

        d = datetime(2024, 6, 1, 12, 0, 0)

        original_db = {
            guild_id: [
                {
                    "SK": f"DISCORDUSER#{user_id}",
                    "NominatedFilmID": film_id,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"DISCORDUSER#{other_user_id}",
                    "NominatedFilmID": None,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id}",
                    "FilmName": "Film To Retire",
                    "IMDbID": None,
                    "DiscordUserID": user_id,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#WATCHED#{(d - timedelta(days=5)).isoformat()}#old-film",
                    "FilmName": "Old Film",
                    "IMDbID": None,
                    "DiscordUserID": other_user_id,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": {other_user_id},
                    "DateNominated": (d - timedelta(days=6)).isoformat(),
                },
            ]
        }
        set_db(self.dynamodb_client, original_db)

        # /retire when the user attended a recent film should return an informative ephemeral message
        set_db(
            self.dynamodb_client,
            {
                guild_id: [
                    {
                        "SK": f"DISCORDUSER#{user_id}",
                        "NominatedFilmID": None,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"DISCORDUSER#{other_user_id}",
                        "NominatedFilmID": None,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"FILM#WATCHED#{(d - timedelta(days=1)).isoformat()}#recent-film",
                        "FilmName": "Recent Film",
                        "IMDbID": None,
                        "DiscordUserID": other_user_id,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": {user_id},
                        "DateNominated": (d - timedelta(days=2)).isoformat(),
                    },
                ]
            },
        )
        result = handle_discord(
            {
                "body-json": {
                    "type": DiscordRequest.APPLICATION_COMMAND,
                    "data": {
                        "name": "retire",
                        "options": [{"value": user_id}],
                    },
                    "guild_id": guild_id,
                    "member": {"user": {"id": other_user_id}},
                }
            },
            self.dynamodb_client,
        )
        self.assertEqual(
            result,
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": f"<@{user_id}> cannot be retired as they attended one of the last 5 films",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )

        # Reset to the original eligible DB state
        set_db(self.dynamodb_client, original_db)

        # /retire prompts for confirmation
        result = handle_discord(
            {
                "body-json": {
                    "type": DiscordRequest.APPLICATION_COMMAND,
                    "data": {
                        "name": "retire",
                        "options": [{"value": user_id}],
                    },
                    "guild_id": guild_id,
                    "member": {"user": {"id": other_user_id}},
                }
            },
            self.dynamodb_client,
        )
        self.assertEqual(
            result,
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": (
                        f"Please confirm whether you would like to retire <@{user_id}>.\n"
                        f"The following checks have passed:\n"
                        f"- <@{user_id}> has not cast a preference vote\n"
                        f"- No other user is currently voting for their nominated film\n"
                        f"- <@{user_id}> has not attended any of the last 5 films"
                    ),
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                    "components": [
                        {
                            "type": DiscordMessageComponent.ACTION_ROW,
                            "components": [
                                {
                                    "type": DiscordMessageComponent.BUTTON,
                                    "label": "Confirm",
                                    "style": DiscordStyle.DANGER,
                                    "custom_id": MessageComponentID.CONFIRM_RETIRE
                                    + user_id,
                                }
                            ],
                        }
                    ],
                },
            },
        )

        # User should still be in the DB
        from filmbot import FilmBot

        still_registered = FilmBot(
            DynamoDBClient=self.dynamodb_client, GuildID=guild_id
        ).get_users()
        self.assertIn(user_id, still_registered)

        # Pressing the Confirm button should actually retire the user
        result = handle_discord(
            {
                "body-json": {
                    "type": DiscordRequest.MESSAGE_COMPONENT,
                    "data": {
                        "component_type": DiscordMessageComponent.BUTTON,
                        "custom_id": MessageComponentID.CONFIRM_RETIRE
                        + user_id,
                    },
                    "guild_id": guild_id,
                    "member": {"user": {"id": other_user_id}},
                }
            },
            self.dynamodb_client,
        )
        self.assertEqual(
            result,
            {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": f"<@{user_id}> has been retired.",
                },
            },
        )

        # User should now be gone
        after_retire = FilmBot(
            DynamoDBClient=self.dynamodb_client, GuildID=guild_id
        ).get_users()
        self.assertNotIn(user_id, after_retire)

        # Pressing Confirm again (e.g. double-click) should give an ephemeral error
        result = handle_discord(
            {
                "body-json": {
                    "type": DiscordRequest.MESSAGE_COMPONENT,
                    "data": {
                        "component_type": DiscordMessageComponent.BUTTON,
                        "custom_id": MessageComponentID.CONFIRM_RETIRE
                        + user_id,
                    },
                    "guild_id": guild_id,
                    "member": {"user": {"id": other_user_id}},
                }
            },
            self.dynamodb_client,
        )
        self.assertEqual(
            result["type"], DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE
        )
        self.assertEqual(result["data"]["flags"], DiscordFlag.EPHEMERAL_FLAG)


if __name__ == "__main__":
    unittest.main()
