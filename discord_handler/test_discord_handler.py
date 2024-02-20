import unittest
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

    def test_workflow(self):
        # 1. Check /peek, /history, /naughty with an empty DB
        # 2. Check /nominate
        # 3. Check /vote
        # 4. Check /watch
        # 5. Check shame button
        # 6. Check /here (TODO: Also check the application command)

        # 1. Check /peek, /history, /naughty with an empty DB
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
                    "content": "The current list of nominations are:\n",
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
                    "content": "Here are the films that have been watched:\n",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /naughty
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "naughty",
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
                    "content": "There are no outstanding tasks.",
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

        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "nominate",
                            "options": [
                                {"value": "IMDB:012345:My Other Film"}
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
                    + "2.  [My Other Film](<https://imdb.com/title/tt012345>) (0 🗳) <@def>",
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
                    "content": "Here are the films that have been watched:\n",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /naughty
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "naughty",
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
                    "content": "These users need to vote:\n- <@abc>\n- <@def>",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                    "components": [
                        {
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
                    ],
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
                    + "2.  [My Other Film](<https://imdb.com/title/tt012345>) (0 🗳) <@def>",
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
                    "content": "Here are the films that have been watched:\n",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )
        # /naughty
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "naughty",
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
                    "content": "These users need to vote:\n- <@abc>",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                    "components": [
                        {
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
                    ],
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
                    + "1.  [My Other Film](<https://imdb.com/title/tt012345>) (1 🗳) <@def>\n"
                    + "2. [No nomination] <@abc>",
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
        # /naughty
        self.assertEqual(
            handle_discord(
                {
                    "body-json": {
                        "type": DiscordRequest.APPLICATION_COMMAND,
                        "data": {
                            "name": "naughty",
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
                    "content": "These users need to nominate:\n- <@abc>\n\n"
                    + "These users need to vote:\n- <@abc>\n- <@def>",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                    "components": [
                        {
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
                    ],
                },
            },
        )

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
                    + "1.  [My Other Film](<https://imdb.com/title/tt012345>) (1 🗳) <@def>\n"
                    + "2. [No nomination] <@abc>",
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
