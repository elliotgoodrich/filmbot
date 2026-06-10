import unittest
import boto3
from math import factorial
from itertools import permutations
from moto import mock_dynamodb
from filmbot import (
    FilmBot,
    TABLE_NAME,
    AttendanceStatus,
    VotingStatus,
    Film,
    User,
    unkey_map,
    key_map,
)
from datetime import datetime, timedelta
from uuid import uuid1
from UserError import UserError
from embed_nominations_migration import migrate
import copy

AWS_REGION = "eu-west-2"


def grab_db(client):
    kwargs = {
        "TableName": TABLE_NAME,
        "Select": "ALL_ATTRIBUTES",
        "ReturnConsumedCapacity": "NONE",
    }

    done = False
    start_key = None
    records = {}
    while not done:
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        response = client.scan(**kwargs)
        for record in response.get("Items"):
            r = unkey_map(record)
            del r["PK"]
            records.setdefault(record["PK"]["S"], []).append(r)
        start_key = response.get("LastEvaluatedKey", None)
        done = start_key is None

    # Sort by the sort key
    for key in records:
        records[key].sort(
            key=lambda n: n["SK"],
        )

    return records


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
        migrate(client, dry_run=False)
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
    migrate(client, dry_run=False)


class snapshot:
    def __init__(self, client):
        self.client = client
        self.state = grab_db(self.client)

    def __enter__(self):
        return copy.deepcopy(self.state)

    def __exit__(self, exc_type, exc_value, exc_tb):
        set_db(self.client, self.state)


class TestFilmBot(unittest.TestCase):
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
        self.assertEqual(grab_db(self.dynamodb_client), {})
        pass

    def tearDown(self):
        """
        Unmock `dynamodb2`.
        """
        self.mock_dynamodb.stop()
        pass

    def test_get_users(self):
        guild1 = "guild1"
        user_id1 = "123"
        set_db(
            self.dynamodb_client,
            {
                guild1: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "NominatedFilmID": None,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    }
                ]
            },
        )

        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild1)
        self.assertEqual(
            filmbot.get_users(),
            {
                user_id1: User(
                    DiscordUserID=user_id1,
                    VoteID=None,
                    AttendanceVoteID=None,
                )
            },
        )

        user_id2 = "user2"
        film_id = "fake-film-id"
        film_id2 = "fake-film-id2"
        guild2 = "second-guild"
        d = datetime(2001, 1, 1, 5, 0, 0, 123)
        set_db(
            self.dynamodb_client,
            {
                guild1: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "NominatedFilmID": film_id,
                        "VoteID": film_id2,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"FILM#NOMINATED#{film_id}",
                        "FilmName": "Fake Film",
                        "IMDbID": None,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": d.isoformat(),
                    },
                    {
                        "SK": f"DISCORDUSER#{user_id2}",
                        "NominatedFilmID": None,
                        "VoteID": film_id,
                        "AttendanceVoteID": film_id2,
                    },
                ],
                guild2: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "NominatedFilmID": None,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    }
                ],
            },
        )

        # After migration: user1 gets film embedded; VoteID film_id2 has no
        # FILM#NOMINATED# record so it's cleared. user2's VoteID film_id maps
        # to user_id1 (nominator of film_id).
        self.assertEqual(
            filmbot.get_users(),
            {
                user_id1: User(
                    DiscordUserID=user_id1,
                    VoteID=None,
                    AttendanceVoteID=None,
                    FilmID=film_id,
                    FilmName="Fake Film",
                    IMDbID=None,
                    CastVotes=0,
                    AttendanceVotes=0,
                    DateNominated=d,
                ),
                user_id2: User(
                    DiscordUserID=user_id2,
                    VoteID=user_id1,
                    AttendanceVoteID=film_id2,
                ),
            },
        )
        pass

    def test_get_nominations(self):
        guild = "TEST-GUILD"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild)

        # Get that it works with 0 films
        self.assertEqual(filmbot.get_nominations(), [])

        # Check that it works a few films
        d = datetime(2001, 1, 1, 5, 0, 0, 123)
        input_films = [
            {
                "SK": "FILM#NOMINATED#film1",
                "FilmName": "FilmName1",
                "IMDbID": None,
                "DiscordUserID": "UserA",
                "CastVotes": 0,
                "AttendanceVotes": 7,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film2",
                "FilmName": "FilmName2",
                "IMDbID": "0234567",
                "DiscordUserID": "UserB",
                "CastVotes": 3,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film3",
                "FilmName": "FilmName3",
                "IMDbID": "0345678",
                "DiscordUserID": "UserC",
                "CastVotes": 2,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film4",
                "FilmName": "FilmName4",
                "IMDbID": "03456789",
                "DiscordUserID": "UserD",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film5",
                "FilmName": "FilmName5",
                "IMDbID": "04567890",
                "DiscordUserID": "UserE",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": (d + timedelta(seconds=1)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{d.isoformat()}#film6",
                "FilmName": "FilmName6",
                "IMDbID": "05678901",
                "DiscordUserID": "UserF",
                "CastVotes": 10,
                "AttendanceVotes": 9,
                "UsersAttended": set(["A", "B", "C"]),
                "DateNominated": d.isoformat(),
            },
        ]
        # DISCORDUSER# records so migration can embed film data
        input_users = [
            {
                "SK": "DISCORDUSER#UserA",
                "NominatedFilmID": "film1",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserB",
                "NominatedFilmID": "film2",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserC",
                "NominatedFilmID": "film3",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserD",
                "NominatedFilmID": "film4",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserE",
                "NominatedFilmID": "film5",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
        ]

        expected = [
            Film(
                FilmID="film1",
                FilmName="FilmName1",
                IMDbID=None,
                DiscordUserID="UserA",
                CastVotes=0,
                AttendanceVotes=7,
                UsersAttended=None,
                DateNominated=d,
                DateWatched=None,
            ),
            Film(
                FilmID="film2",
                FilmName="FilmName2",
                IMDbID="0234567",
                DiscordUserID="UserB",
                CastVotes=3,
                AttendanceVotes=3,
                UsersAttended=None,
                DateNominated=d,
                DateWatched=None,
            ),
            Film(
                FilmID="film4",
                FilmName="FilmName4",
                IMDbID="03456789",
                DiscordUserID="UserD",
                CastVotes=2,
                AttendanceVotes=4,
                UsersAttended=None,
                DateNominated=d,
                DateWatched=None,
            ),
            Film(
                FilmID="film5",
                FilmName="FilmName5",
                IMDbID="04567890",
                DiscordUserID="UserE",
                CastVotes=2,
                AttendanceVotes=4,
                UsersAttended=None,
                DateNominated=d + timedelta(seconds=1),
                DateWatched=None,
            ),
            Film(
                FilmID="film3",
                FilmName="FilmName3",
                IMDbID="0345678",
                DiscordUserID="UserC",
                CastVotes=2,
                AttendanceVotes=3,
                UsersAttended=None,
                DateNominated=d,
                DateWatched=None,
            ),
        ]

        # Test all permutation of the input to make sure that we are actually
        # sorting the rows
        count = 0
        for input in permutations(input_films):
            set_db(self.dynamodb_client, {guild: list(input) + input_users})

            self.assertEqual(filmbot.get_nominations(), expected)
            count += 1

        assert count == factorial(len(input_films))

    def test_get_users_by_nomination(self):
        guild = "TEST-GUILD"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild)

        # Get that it works with an empty database
        self.assertEqual(filmbot.get_users_by_nomination(), [])

        # Check that it works a few films and users
        d = datetime(2001, 1, 1, 5, 0, 0, 123)
        input_users = [
            {
                "SK": "DISCORDUSER#UserA",
                "NominatedFilmID": "film1",
                "VoteID": "film2",
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserB",
                "NominatedFilmID": "film2",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserC",
                "NominatedFilmID": "film3",
                "VoteID": "film1",
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserD",
                "NominatedFilmID": "film4",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserE",
                "NominatedFilmID": "film5",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserF",
                "NominatedFilmID": None,
                "VoteID": "film4",
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserG",
                "NominatedFilmID": None,
                "VoteID": None,
                "AttendanceVoteID": None,
            },
        ]
        input_films = [
            {
                "SK": "FILM#NOMINATED#film1",
                "FilmName": "FilmName1",
                "IMDbID": None,
                "DiscordUserID": "UserA",
                "CastVotes": 0,
                "AttendanceVotes": 7,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film2",
                "FilmName": "FilmName2",
                "IMDbID": "0234567",
                "DiscordUserID": "UserB",
                "CastVotes": 3,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film3",
                "FilmName": "FilmName3",
                "IMDbID": "0345678",
                "DiscordUserID": "UserC",
                "CastVotes": 2,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film4",
                "FilmName": "FilmName4",
                "IMDbID": "03456789",
                "DiscordUserID": "UserD",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film5",
                "FilmName": "FilmName5",
                "IMDbID": "04567890",
                "DiscordUserID": "UserE",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": (d + timedelta(seconds=1)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{d.isoformat()}#film6",
                "FilmName": "FilmName6",
                "IMDbID": "05678901",
                "DiscordUserID": "UserF",
                "CastVotes": 10,
                "AttendanceVotes": 9,
                "UsersAttended": set(["A", "B", "C"]),
                "DateNominated": d.isoformat(),
            },
        ]

        # After migration: VoteIDs (film IDs) are replaced with the nominator's
        # Discord user ID. Film data is embedded in each DISCORDUSER# record.
        expected = [
            {
                "User": User(
                    DiscordUserID="UserA",
                    VoteID="UserB",  # voted for film2 → nominator is UserB
                    AttendanceVoteID=None,
                    FilmID="film1",
                    FilmName="FilmName1",
                    IMDbID=None,
                    CastVotes=0,
                    AttendanceVotes=7,
                    DateNominated=d,
                ),
                "Film": Film(
                    FilmID="film1",
                    FilmName="FilmName1",
                    IMDbID=None,
                    DiscordUserID="UserA",
                    CastVotes=0,
                    AttendanceVotes=7,
                    UsersAttended=None,
                    DateNominated=d,
                    DateWatched=None,
                ),
            },
            {
                "User": User(
                    DiscordUserID="UserB",
                    VoteID=None,
                    AttendanceVoteID=None,
                    FilmID="film2",
                    FilmName="FilmName2",
                    IMDbID="0234567",
                    CastVotes=3,
                    AttendanceVotes=3,
                    DateNominated=d,
                ),
                "Film": Film(
                    FilmID="film2",
                    FilmName="FilmName2",
                    IMDbID="0234567",
                    DiscordUserID="UserB",
                    CastVotes=3,
                    AttendanceVotes=3,
                    UsersAttended=None,
                    DateNominated=d,
                    DateWatched=None,
                ),
            },
            {
                "User": User(
                    DiscordUserID="UserD",
                    VoteID=None,
                    AttendanceVoteID=None,
                    FilmID="film4",
                    FilmName="FilmName4",
                    IMDbID="03456789",
                    CastVotes=2,
                    AttendanceVotes=4,
                    DateNominated=d,
                ),
                "Film": Film(
                    FilmID="film4",
                    FilmName="FilmName4",
                    IMDbID="03456789",
                    DiscordUserID="UserD",
                    CastVotes=2,
                    AttendanceVotes=4,
                    UsersAttended=None,
                    DateNominated=d,
                    DateWatched=None,
                ),
            },
            {
                "User": User(
                    DiscordUserID="UserE",
                    VoteID=None,
                    AttendanceVoteID=None,
                    FilmID="film5",
                    FilmName="FilmName5",
                    IMDbID="04567890",
                    CastVotes=2,
                    AttendanceVotes=4,
                    DateNominated=d + timedelta(seconds=1),
                ),
                "Film": Film(
                    FilmID="film5",
                    FilmName="FilmName5",
                    IMDbID="04567890",
                    DiscordUserID="UserE",
                    CastVotes=2,
                    AttendanceVotes=4,
                    UsersAttended=None,
                    DateNominated=d + timedelta(seconds=1),
                    DateWatched=None,
                ),
            },
            {
                "User": User(
                    DiscordUserID="UserC",
                    VoteID="UserA",  # voted for film1 → nominator is UserA
                    AttendanceVoteID=None,
                    FilmID="film3",
                    FilmName="FilmName3",
                    IMDbID="0345678",
                    CastVotes=2,
                    AttendanceVotes=3,
                    DateNominated=d,
                ),
                "Film": Film(
                    FilmID="film3",
                    FilmName="FilmName3",
                    IMDbID="0345678",
                    DiscordUserID="UserC",
                    CastVotes=2,
                    AttendanceVotes=3,
                    UsersAttended=None,
                    DateNominated=d,
                    DateWatched=None,
                ),
            },
            {
                "User": User(
                    DiscordUserID="UserF",
                    VoteID="UserD",  # voted for film4 → nominator is UserD
                    AttendanceVoteID=None,
                ),
                "Film": None,
            },
            {
                "User": User(
                    DiscordUserID="UserG",
                    VoteID=None,
                    AttendanceVoteID=None,
                ),
                "Film": None,
            },
        ]

        # Test all permutation of the films to make sure that we are actually
        # sorting the rows
        count = 0
        for input in permutations(input_films):
            set_db(
                self.dynamodb_client, {guild: list(input) + list(input_users)}
            )

            self.assertEqual(filmbot.get_users_by_nomination(), expected)
            count += 1

        assert count == factorial(len(input_films))

    def test_get_watched_films(self):
        guild = "TEST-GUILD"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild)

        # Get that it works with 0 films
        self.assertEqual(filmbot.get_watched_films(), [])

        # Check that it works a few films
        d = datetime(2001, 1, 1, 5, 0, 0, 123)
        input_films = [
            {
                "SK": f"FILM#WATCHED#{d.isoformat()}#film1",
                "FilmName": "FilmName1",
                "IMDbID": "05678901",
                "DiscordUserID": "UserA",
                "CastVotes": 1,
                "AttendanceVotes": 2,
                "UsersAttended": set(["A", "B", "C"]),
                "DateNominated": (d - timedelta(seconds=10)).isoformat(),
            },
            {
                # Nominated user should not appear in watched films
                "SK": "DISCORDUSER#UserB",
                "NominatedFilmID": "film2",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "FILM#NOMINATED#film2",
                "FilmName": "FilmName2",
                "IMDbID": None,
                "DiscordUserID": "UserB",
                "CastVotes": 3,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{(d - timedelta(seconds=1)).isoformat()}#film3",
                "FilmName": "FilmName3",
                "IMDbID": "123456",
                "DiscordUserID": "UserC",
                "CastVotes": 5,
                "AttendanceVotes": 6,
                "UsersAttended": set(["D"]),
                "DateNominated": (d - timedelta(seconds=5)).isoformat(),
            },
        ]

        expected = [
            Film(
                FilmID="film1",
                FilmName="FilmName1",
                IMDbID="05678901",
                DiscordUserID="UserA",
                CastVotes=1,
                AttendanceVotes=2,
                UsersAttended=set(["A", "B", "C"]),
                DateNominated=d - timedelta(seconds=10),
                DateWatched=d,
            ),
            Film(
                FilmID="film3",
                FilmName="FilmName3",
                IMDbID="123456",
                DiscordUserID="UserC",
                CastVotes=5,
                AttendanceVotes=6,
                UsersAttended=set(["D"]),
                DateNominated=d - timedelta(seconds=5),
                DateWatched=d - timedelta(seconds=1),
            ),
        ]

        # Test all permutation of the input to make sure that we are actually
        # sorting the rows
        count = 0
        for input in permutations(input_films):
            set_db(self.dynamodb_client, {guild: input})

            self.assertEqual(filmbot.get_watched_films(), expected)
            nextKey = "FILM#WATCHED#2001-01-01T05:00:00.000123#film1"
            self.assertEqual(
                filmbot.get_watched_films_after(Limit=1),
                ([expected[0]], nextKey),
            )
            self.assertEqual(
                filmbot.get_watched_films_after(Limit=2), (expected, None)
            )
            self.assertEqual(
                filmbot.get_watched_films_after(Limit=100), (expected, None)
            )
            self.assertEqual(
                filmbot.get_watched_films_after(
                    Limit=1, ExclusiveStartKey=nextKey
                ),
                ([expected[1]], None),
            )
            count += 1

        assert count == factorial(len(input_films))

    def test_get_all_films(self):
        guild = "TEST-GUILD"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild)

        # Get that it works with 0 films
        self.assertEqual(filmbot.get_all_films(), [])

        d = datetime(2001, 1, 1, 5, 0, 0, 123)
        input_films = [
            {
                "SK": "FILM#NOMINATED#film1",
                "FilmName": "FilmName1",
                "DiscordUserID": "UserA",
                "IMDbID": "0123",
                "CastVotes": 0,
                "AttendanceVotes": 7,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film2",
                "FilmName": "FilmName2",
                "IMDbID": "0124",
                "DiscordUserID": "UserB",
                "CastVotes": 3,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": (d + timedelta(seconds=2)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{(d + timedelta(seconds=1)).isoformat()}#film3",
                "FilmName": "FilmName3",
                "IMDbID": "0125",
                "DiscordUserID": "UserC",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": set(["A", "B", "C"]),
                "DateNominated": (d + timedelta(seconds=1)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{d.isoformat()}#film4",
                "FilmName": "FilmName4",
                "IMDbID": "0126",
                "DiscordUserID": "UserD",
                "CastVotes": 10,
                "AttendanceVotes": 9,
                "UsersAttended": set(["A"]),
                "DateNominated": (d + timedelta(seconds=3)).isoformat(),
            },
        ]
        # DISCORDUSER# records so migration can embed film data for nominations
        input_users = [
            {
                "SK": "DISCORDUSER#UserA",
                "NominatedFilmID": "film1",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
            {
                "SK": "DISCORDUSER#UserB",
                "NominatedFilmID": "film2",
                "VoteID": None,
                "AttendanceVoteID": None,
            },
        ]

        expected = [
            Film(
                FilmID="film1",
                FilmName="FilmName1",
                IMDbID="0123",
                DiscordUserID="UserA",
                CastVotes=0,
                AttendanceVotes=7,
                UsersAttended=None,
                DateNominated=d,
                DateWatched=None,
            ),
            Film(
                FilmID="film3",
                FilmName="FilmName3",
                IMDbID="0125",
                DiscordUserID="UserC",
                CastVotes=2,
                AttendanceVotes=4,
                UsersAttended=set(["A", "B", "C"]),
                DateNominated=d + timedelta(seconds=1),
                DateWatched=d + timedelta(seconds=1),
            ),
            Film(
                FilmID="film2",
                FilmName="FilmName2",
                IMDbID="0124",
                DiscordUserID="UserB",
                CastVotes=3,
                AttendanceVotes=3,
                UsersAttended=None,
                DateNominated=d + timedelta(seconds=2),
                DateWatched=None,
            ),
            Film(
                FilmID="film4",
                FilmName="FilmName4",
                IMDbID="0126",
                DiscordUserID="UserD",
                CastVotes=10,
                AttendanceVotes=9,
                UsersAttended=set(["A"]),
                DateNominated=d + timedelta(seconds=3),
                DateWatched=d,
            ),
        ]

        # Test all permutation of the input to make sure that we are actually
        # sorting the rows
        count = 0
        for input in permutations(input_films):
            set_db(self.dynamodb_client, {guild: list(input) + input_users})

            self.assertEqual(filmbot.get_all_films(), expected)
            count += 1

        assert count == factorial(len(input_films))

    def test_nominate_film(self):
        user_id1 = "user1"
        imdb1 = "012341"
        film_id1 = str(uuid1())
        film_name1 = "My Film"
        time1 = datetime(2001, 1, 2, 3, 4, 5, 123)

        guild1 = "GUILD1"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild1)

        filmbot.nominate_film(
            DiscordUserID=user_id1,
            FilmName=film_name1,
            IMDbID=imdb1,
            NewFilmID=film_id1,
            DateTime=time1,
        )

        self.assertEqual(
            grab_db(self.dynamodb_client),
            {
                guild1: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "FilmID": film_id1,
                        "FilmName": film_name1,
                        "IMDbID": imdb1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "DateNominated": time1.isoformat(),
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                ]
            },
        )

        # Check nominating fails when you already have a nomination
        film_name2 = "My Film 2: The Sequel"
        imdb2 = "012342"
        with self.assertRaises(UserError):
            filmbot.nominate_film(
                DiscordUserID=user_id1,
                FilmName=film_name2,
                IMDbID=imdb2,
                NewFilmID=film_id1,
                DateTime=time1,
            )

        user_id2 = "user2"
        film_id2 = str(uuid1())
        time2 = datetime(2002, 1, 2, 3, 4, 5, 678)
        filmbot.nominate_film(
            DiscordUserID=user_id2,
            FilmName=film_name2,
            IMDbID=imdb2,
            NewFilmID=film_id2,
            DateTime=time2,
        )

        expected = {
            guild1: [
                {
                    "SK": f"DISCORDUSER#{user_id1}",
                    "FilmID": film_id1,
                    "FilmName": film_name1,
                    "IMDbID": imdb1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "DateNominated": time1.isoformat(),
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"DISCORDUSER#{user_id2}",
                    "FilmID": film_id2,
                    "FilmName": film_name2,
                    "IMDbID": imdb2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "DateNominated": time2.isoformat(),
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
            ]
        }
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        guild2 = "guild2"
        filmbot2 = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild2)
        # Nominate exactly the same as we did for guild1 and check that it's
        # fine and all added under the other guild PK
        filmbot2.nominate_film(
            DiscordUserID=user_id1,
            FilmName=film_name1,
            IMDbID=imdb1,
            NewFilmID=film_id1,
            DateTime=time1,
        )

        self.assertEqual(
            grab_db(self.dynamodb_client),
            {
                guild1: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "FilmID": film_id1,
                        "FilmName": film_name1,
                        "IMDbID": imdb1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "DateNominated": time1.isoformat(),
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"DISCORDUSER#{user_id2}",
                        "FilmID": film_id2,
                        "FilmName": film_name2,
                        "IMDbID": imdb2,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "DateNominated": time2.isoformat(),
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                ],
                guild2: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "FilmID": film_id1,
                        "FilmName": film_name1,
                        "IMDbID": imdb1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "DateNominated": time1.isoformat(),
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                ],
            },
        )

    def test_workflow(self):
        """
        Test voting, watching, and attendance
        """
        guild1 = "Guild1"
        user_id1 = "User1"
        user_id2 = "User2"
        user_id3 = "User3"
        film_id1 = "Film1"
        film_id2 = "Film2"
        film_id3 = "Film3"
        imdb1 = "0121"
        imdb2 = "0122"
        imdb3 = "0123"
        imdb4 = "0124"
        imdb5 = "0125"
        film_watched = "Film4"
        d = datetime(2010, 1, 2, 3, 4, 5, 678)
        ages_ago = d - timedelta(days=100)

        # Load old-schema data; migration runs inside set_db
        set_db(
            self.dynamodb_client,
            {
                guild1: [
                    {
                        "SK": f"DISCORDUSER#{user_id1}",
                        "NominatedFilmID": film_id1,
                        "VoteID": None,
                        "AttendanceVoteID": "dummy",
                    },
                    {
                        "SK": f"DISCORDUSER#{user_id2}",
                        "NominatedFilmID": film_id2,
                        "VoteID": None,
                        "AttendanceVoteID": "dummy2",
                    },
                    {
                        "SK": f"DISCORDUSER#{user_id3}",
                        "NominatedFilmID": film_id3,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"FILM#NOMINATED#{film_id1}",
                        "FilmName": "My Film 1",
                        "IMDbID": imdb1,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": d.isoformat(),
                    },
                    {
                        "SK": f"FILM#NOMINATED#{film_id2}",
                        "FilmName": "My Film 2",
                        "IMDbID": imdb2,
                        "DiscordUserID": user_id2,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": d.isoformat(),
                    },
                    {
                        "SK": f"FILM#NOMINATED#{film_id3}",
                        "FilmName": "My Film 3",
                        "IMDbID": imdb3,
                        "DiscordUserID": user_id3,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": d.isoformat(),
                    },
                    {
                        "SK": f"FILM#WATCHED#{ages_ago.isoformat()}#Super-old-film",
                        "FilmName": "My Film 4 (Watched)",
                        "IMDbID": imdb4,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": ages_ago.isoformat(),
                    },
                    {
                        "SK": f"FILM#WATCHED#{d.isoformat()}#{film_watched}",
                        "FilmName": "My Film 5 (Watched)",
                        "IMDbID": imdb5,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": d.isoformat(),
                    },
                ]
            },
        )

        # Post-migration expected state: DISCORDUSER# records embed film data,
        # FILM#NOMINATED# records are gone.
        # Sorted by SK: DISCORDUSER#User1 < User2 < User3 < FILM#WATCHED#{ages_ago}... < FILM#WATCHED#{d}...
        expected = {
            guild1: [
                {
                    "SK": f"DISCORDUSER#{user_id1}",
                    "FilmID": film_id1,
                    "FilmName": "My Film 1",
                    "IMDbID": imdb1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "DateNominated": d.isoformat(),
                    "VoteID": None,
                    "AttendanceVoteID": "dummy",
                },
                {
                    "SK": f"DISCORDUSER#{user_id2}",
                    "FilmID": film_id2,
                    "FilmName": "My Film 2",
                    "IMDbID": imdb2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "DateNominated": d.isoformat(),
                    "VoteID": None,
                    "AttendanceVoteID": "dummy2",
                },
                {
                    "SK": f"DISCORDUSER#{user_id3}",
                    "FilmID": film_id3,
                    "FilmName": "My Film 3",
                    "IMDbID": imdb3,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "DateNominated": d.isoformat(),
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"FILM#WATCHED#{ages_ago.isoformat()}#Super-old-film",
                    "FilmName": "My Film 4 (Watched)",
                    "IMDbID": imdb4,
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": ages_ago.isoformat(),
                },
                {
                    "SK": f"FILM#WATCHED#{d.isoformat()}#{film_watched}",
                    "FilmName": "My Film 5 (Watched)",
                    "IMDbID": imdb5,
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
            ]
        }

        # Indices into `expected`
        USER_1 = 0
        USER_2 = 1
        USER_3 = 2

        self.assertEqual(grab_db(self.dynamodb_client), expected)
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild1)

        # Check we can't vote if we're not registered
        with self.assertRaises(UserError):
            filmbot.cast_preference_vote(
                DiscordUserID="not registered", NominatorUserID=user_id1
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't vote for a user without a nomination
        with self.assertRaises(UserError):
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, NominatorUserID="not existent"
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can vote with no previous vote
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, NominatorUserID=user_id2
            )[0],
            VotingStatus.UNCOMPLETE,
        )

        expected[guild1][USER_2]["CastVotes"] += 1
        expected[guild1][USER_1]["VoteID"] = user_id2
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can vote for the same user again (shortcut path in the code)
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, NominatorUserID=user_id2
            )[0],
            VotingStatus.UNCOMPLETE,
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can change our vote
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, NominatorUserID=user_id3
            )[0],
            VotingStatus.UNCOMPLETE,
        )
        expected[guild1][USER_2]["CastVotes"] -= 1
        expected[guild1][USER_3]["CastVotes"] += 1
        expected[guild1][USER_1]["VoteID"] = user_id3
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't vote for our own nomination
        with self.assertRaises(UserError):
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, NominatorUserID=user_id1
            ),
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check that we know when voting is finished
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id2, NominatorUserID=user_id1
            )[0],
            VotingStatus.UNCOMPLETE,
        )
        expected[guild1][USER_1]["CastVotes"] += 1
        expected[guild1][USER_2]["VoteID"] = user_id1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id3, NominatorUserID=user_id1
            )[0],
            VotingStatus.COMPLETE,
        )
        expected[guild1][USER_1]["CastVotes"] += 1
        expected[guild1][USER_3]["VoteID"] = user_id1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check that we can change votes when voting is finished
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id3, NominatorUserID=user_id2
            )[0],
            VotingStatus.COMPLETE,
        )
        expected[guild1][USER_1]["CastVotes"] -= 1
        expected[guild1][USER_2]["CastVotes"] += 1
        expected[guild1][USER_3]["VoteID"] = user_id2
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can vote for the same user once voting is finished
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id3, NominatorUserID=user_id2
            )[0],
            VotingStatus.COMPLETE,
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        good_time = d + timedelta(hours=24)
        bad_time = good_time - timedelta(seconds=1)

        # Check that we can't watch a film for a non-existent user
        with self.assertRaises(UserError):
            filmbot.start_watching_film(
                NominatorUserID="non existent",
                DateTime=good_time,
                PresentUserIDs=[user_id1],
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't watch another film within 24 hours
        with self.assertRaises(UserError):
            filmbot.start_watching_film(
                NominatorUserID=user_id1,
                DateTime=bad_time,
                PresentUserIDs=[user_id1],
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can watch a film with multiple users initially present
        with snapshot(self.dynamodb_client) as exp:
            self.assertEqual(
                filmbot.start_watching_film(
                    NominatorUserID=user_id1,
                    DateTime=good_time,
                    PresentUserIDs=[user_id1, user_id2, user_id3],
                ),
                Film(
                    FilmID=film_id1,
                    FilmName="My Film 1",
                    IMDbID=imdb1,
                    DiscordUserID=user_id1,
                    CastVotes=1,
                    AttendanceVotes=0,
                    UsersAttended=set([user_id1, user_id2, user_id3]),
                    DateNominated=d,
                    DateWatched=good_time,
                ),
            )

            # User1's film fields removed, VoteID/AttendanceVoteID updated
            del exp[guild1][USER_1]["FilmID"]
            del exp[guild1][USER_1]["FilmName"]
            del exp[guild1][USER_1]["IMDbID"]
            del exp[guild1][USER_1]["CastVotes"]
            del exp[guild1][USER_1]["AttendanceVotes"]
            del exp[guild1][USER_1]["DateNominated"]
            exp[guild1][USER_1]["VoteID"] = None
            exp[guild1][USER_1]["AttendanceVoteID"] = film_id1

            # User2 present, not nominator, has film2 → AttendanceVotes increments
            exp[guild1][USER_2]["VoteID"] = None
            exp[guild1][USER_2]["AttendanceVoteID"] = film_id1
            exp[guild1][USER_2]["AttendanceVotes"] += 1

            # User3 present, not nominator, has film3 → AttendanceVotes increments
            exp[guild1][USER_3]["VoteID"] = None
            exp[guild1][USER_3]["AttendanceVoteID"] = film_id1
            exp[guild1][USER_3]["AttendanceVotes"] += 1

            exp[guild1].append(
                {
                    "SK": f"FILM#WATCHED#{good_time.isoformat()}#{film_id1}",
                    "FilmName": "My Film 1",
                    "IMDbID": imdb1,
                    "DiscordUserID": user_id1,
                    "CastVotes": 1,
                    "AttendanceVotes": 0,
                    "UsersAttended": set([user_id1, user_id2, user_id3]),
                    "DateNominated": d.isoformat(),
                }
            )
            self.assertEqual(grab_db(self.dynamodb_client), exp)

        # Check we can watch a film with just one user
        self.assertEqual(
            filmbot.start_watching_film(
                NominatorUserID=user_id1,
                DateTime=good_time,
                PresentUserIDs=[user_id1],
            ),
            Film(
                FilmID=film_id1,
                FilmName="My Film 1",
                IMDbID=imdb1,
                DiscordUserID=user_id1,
                CastVotes=1,
                AttendanceVotes=0,
                UsersAttended=set([user_id1]),
                DateNominated=d,
                DateWatched=good_time,
            ),
        )

        # User1 film fields cleared, present → AttendanceVoteID set
        del expected[guild1][USER_1]["FilmID"]
        del expected[guild1][USER_1]["FilmName"]
        del expected[guild1][USER_1]["IMDbID"]
        del expected[guild1][USER_1]["CastVotes"]
        del expected[guild1][USER_1]["AttendanceVotes"]
        del expected[guild1][USER_1]["DateNominated"]
        expected[guild1][USER_1]["VoteID"] = None
        expected[guild1][USER_1]["AttendanceVoteID"] = film_id1
        # User2 not present: vote and attendance cleared
        expected[guild1][USER_2]["VoteID"] = None
        expected[guild1][USER_2]["AttendanceVoteID"] = None
        # User3 not present: vote cleared
        expected[guild1][USER_3]["VoteID"] = None

        expected[guild1].append(
            {
                "SK": f"FILM#WATCHED#{good_time.isoformat()}#{film_id1}",
                "FilmName": "My Film 1",
                "IMDbID": imdb1,
                "DiscordUserID": user_id1,
                "CastVotes": 1,
                "AttendanceVotes": 0,
                "UsersAttended": set([user_id1]),
                "DateNominated": d.isoformat(),
            }
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # After watching: list is now 5 items sorted by SK:
        # 0: DISCORDUSER#User1 (no film fields)
        # 1: DISCORDUSER#User2 (has film2)
        # 2: DISCORDUSER#User3 (has film3)
        # 3: FILM#WATCHED#{ages_ago}#Super-old-film
        # 4: FILM#WATCHED#{d}#Film4
        # 5: FILM#WATCHED#{good_time}#Film1
        USER_1 = 0
        USER_2 = 1
        USER_3 = 2
        FILM_1 = 5

        # Check we can't record attendance before the film is watched
        too_early = good_time - timedelta(seconds=1)
        with self.assertRaises(UserError):
            filmbot.record_attendance_vote(
                DiscordUserID=user_id2, DateTime=too_early
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't record attendance after the film has finished
        # (which is hard-coded to 4 hours right now)
        too_late = good_time + timedelta(hours=4, seconds=1)
        with self.assertRaises(UserError):
            filmbot.record_attendance_vote(
                DiscordUserID=user_id2, DateTime=too_late
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check that recording attendance for user1 (already marked present) is a noop
        self.assertEqual(
            filmbot.record_attendance_vote(
                DiscordUserID=user_id1, DateTime=good_time
            ),
            AttendanceStatus.ALREADY_REGISTERED,
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can record attendance on the cutoff with a user who has a film
        just_in_time = good_time + timedelta(hours=4)
        self.assertEqual(
            filmbot.record_attendance_vote(
                DiscordUserID=user_id2, DateTime=just_in_time
            ),
            AttendanceStatus.REGISTERED,
        )
        expected[guild1][USER_2]["AttendanceVoteID"] = film_id1
        expected[guild1][FILM_1]["UsersAttended"].add(user_id2)
        expected[guild1][USER_2]["AttendanceVotes"] += 1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can record attendance for a user who has a nominated film (user3)
        self.assertEqual(
            filmbot.record_attendance_vote(
                DiscordUserID=user_id3, DateTime=just_in_time
            ),
            AttendanceStatus.REGISTERED,
        )
        expected[guild1][USER_3]["AttendanceVoteID"] = film_id1
        expected[guild1][FILM_1]["UsersAttended"].add(user_id3)
        expected[guild1][USER_3]["AttendanceVotes"] += 1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

    def test_retire_user(self):
        guild = "TEST-GUILD"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild)

        user_id1 = "user1"
        user_id2 = "user2"
        user_id3 = "user3"
        film_id1 = "film-id-1"
        film_id2 = "film-id-2"
        film_id3 = "film-id-3"

        d = datetime(2024, 1, 1, 12, 0, 0)

        # Cannot retire or check a user who doesn't exist
        with self.assertRaises(UserError):
            filmbot.retire_user(DiscordUserID=user_id1)
        with self.assertRaises(UserError):
            filmbot.can_retire(DiscordUserID=user_id1)

        base_data = {
            guild: [
                {
                    "SK": f"DISCORDUSER#{user_id1}",
                    "NominatedFilmID": film_id1,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"DISCORDUSER#{user_id2}",
                    "NominatedFilmID": film_id2,
                    "VoteID": film_id1,  # user2 has voted for user1's film
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"DISCORDUSER#{user_id3}",
                    "NominatedFilmID": None,
                    "VoteID": film_id1,  # user3 has voted but has no nomination
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id1}",
                    "FilmName": "Film 1",
                    "IMDbID": None,
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id2}",
                    "FilmName": "Film 2",
                    "IMDbID": None,
                    "DiscordUserID": user_id2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#WATCHED#{(d - timedelta(days=5)).isoformat()}#{film_id3}",
                    "FilmName": "Watched Film",
                    "IMDbID": None,
                    "DiscordUserID": user_id2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": {user_id1},
                    "DateNominated": (d - timedelta(days=6)).isoformat(),
                },
            ]
        }
        set_db(self.dynamodb_client, base_data)

        # Cannot retire user1: they attended a recent film and others voted for their film
        self.assertIsNotNone(filmbot.can_retire(DiscordUserID=user_id1))
        with self.assertRaises(UserError):
            filmbot.retire_user(DiscordUserID=user_id1)

        # Cannot retire user2: they have an outstanding vote
        self.assertIsNotNone(filmbot.can_retire(DiscordUserID=user_id2))
        with self.assertRaises(UserError):
            filmbot.retire_user(DiscordUserID=user_id2)

        # Cannot retire user3: they have an outstanding vote
        self.assertIsNotNone(filmbot.can_retire(DiscordUserID=user_id3))
        with self.assertRaises(UserError):
            filmbot.retire_user(DiscordUserID=user_id3)

        # Set up an eligible scenario: user3 has no nomination, no vote, no recent attendance
        eligible_data = {
            guild: [
                {
                    "SK": f"DISCORDUSER#{user_id1}",
                    "NominatedFilmID": film_id1,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"DISCORDUSER#{user_id2}",
                    "NominatedFilmID": film_id2,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"DISCORDUSER#{user_id3}",
                    "NominatedFilmID": None,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id1}",
                    "FilmName": "Film 1",
                    "IMDbID": None,
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id2}",
                    "FilmName": "Film 2",
                    "IMDbID": None,
                    "DiscordUserID": user_id2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#WATCHED#{(d - timedelta(days=5)).isoformat()}#{film_id3}",
                    "FilmName": "Watched Film",
                    "IMDbID": None,
                    "DiscordUserID": user_id2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": {user_id2},
                    "DateNominated": (d - timedelta(days=6)).isoformat(),
                },
            ]
        }
        set_db(self.dynamodb_client, eligible_data)

        # After migration: VoteIDs film_id1 → user_id1 for user2 and user3.
        # FILM#NOMINATED# records are deleted; film data is embedded in DISCORDUSER#.

        # user3: no nomination, no vote, no recent attendance
        self.assertIsNone(filmbot.can_retire(DiscordUserID=user_id3))
        filmbot.retire_user(DiscordUserID=user_id3)
        db = grab_db(self.dynamodb_client)
        user_sks = [r["SK"] for r in db[guild]]
        self.assertNotIn(f"DISCORDUSER#{user_id3}", user_sks)
        self.assertIn(f"DISCORDUSER#{user_id1}", user_sks)
        self.assertIn(f"DISCORDUSER#{user_id2}", user_sks)

        # user1: no vote, no one has voted for their film, no recent attendance.
        # Retiring user1 removes DISCORDUSER#user1 (which contains their film data).
        self.assertIsNone(filmbot.can_retire(DiscordUserID=user_id1))
        filmbot.retire_user(DiscordUserID=user_id1)
        db = grab_db(self.dynamodb_client)
        user_sks = [r["SK"] for r in db[guild]]
        self.assertNotIn(f"DISCORDUSER#{user_id1}", user_sks)
        self.assertIn(f"DISCORDUSER#{user_id2}", user_sks)

        # CastVotes > 0 from a previous round should not block retirement when no
        # user currently has VoteID pointing at user2.
        # user2 has CastVotes=3 (leftover from old rounds) but no live voters.
        set_db(
            self.dynamodb_client,
            {
                guild: [
                    {
                        "SK": f"DISCORDUSER#{user_id2}",
                        "NominatedFilmID": film_id2,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"FILM#NOMINATED#{film_id2}",
                        "FilmName": "Film 2",
                        "IMDbID": None,
                        "DiscordUserID": user_id2,
                        "CastVotes": 3,  # leftover from previous rounds
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": d.isoformat(),
                    },
                    {
                        "SK": f"FILM#WATCHED#{(d - timedelta(days=5)).isoformat()}#{film_id3}",
                        "FilmName": "Watched Film",
                        "IMDbID": None,
                        "DiscordUserID": user_id2,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": {user_id1},  # user2 not in attendance
                        "DateNominated": (d - timedelta(days=6)).isoformat(),
                    },
                ]
            },
        )
        self.assertIsNone(filmbot.can_retire(DiscordUserID=user_id2))
        filmbot.retire_user(DiscordUserID=user_id2)
        db = grab_db(self.dynamodb_client)
        user_sks = [r["SK"] for r in db[guild]]
        self.assertNotIn(f"DISCORDUSER#{user_id2}", user_sks)


if __name__ == "__main__":
    unittest.main()
