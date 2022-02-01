import unittest
import boto3
from math import factorial
from itertools import permutations
from moto import mock_dynamodb2
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
import copy

AWS_REGION = "eu-west-2"


def grab_db(client):
    kwargs = {
        "TableName": TABLE_NAME,
        "Select": "ALL_ATTRIBUTES",
        "ReturnConsumedCapacity": "NONE",
    }

    members_table = client.scan(**kwargs)
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


class snapshot:
    def __init__(self, client):
        self.client = client
        self.state = grab_db(self.client)

    def __enter__(self):
        return copy.deepcopy(self.state)

    def __exit__(self, exc_type, exc_value, exc_tb):
        set_db(self.client, self.state)


class TestFilmBot(unittest.TestCase):
    mock_dynamodb2 = mock_dynamodb2()

    def setUp(self):
        """
        Mock `dynamodb2` and create the tables we expect.
        """

        # Set unlimited length for assertEqual diff lengths
        self.maxDiff = None

        self.mock_dynamodb2.start()
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
        self.mock_dynamodb2.stop()
        pass

    def test_get_users(self):
        guild1 = "guild1"
        user_id1 = "123"
        set_db(
            self.dynamodb_client,
            {
                guild1: [
                    {
                        "SK": f"USER#{user_id1}",
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
                    NominatedFilmID=None,
                    VoteID=None,
                    AttendanceVoteID=None,
                )
            },
        )

        user_id2 = "user2"
        film_id = "fake-film-id"
        film_id2 = "fake-film-id2"
        guild2 = "second-guild"
        set_db(
            self.dynamodb_client,
            {
                guild1: [
                    {
                        "SK": f"USER#{user_id1}",
                        "NominatedFilmID": film_id,
                        "VoteID": film_id2,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"USER#{user_id2}",
                        "NominatedFilmID": None,
                        "VoteID": film_id,
                        "AttendanceVoteID": film_id2,
                    },
                ],
                guild2: [
                    {
                        "SK": f"USER#{user_id1}",
                        "NominatedFilmID": None,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    }
                ],
            },
        )

        self.assertEqual(
            filmbot.get_users(),
            {
                user_id1: User(
                    DiscordUserID=user_id1,
                    NominatedFilmID=film_id,
                    VoteID=film_id2,
                    AttendanceVoteID=None,
                ),
                user_id2: User(
                    DiscordUserID=user_id2,
                    NominatedFilmID=None,
                    VoteID=film_id,
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
                "DiscordUserID": "UserA",
                "CastVotes": 0,
                "AttendanceVotes": 7,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film2",
                "FilmName": "FilmName2",
                "DiscordUserID": "UserB",
                "CastVotes": 3,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film3",
                "FilmName": "FilmName3",
                "DiscordUserID": "UserC",
                "CastVotes": 2,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film4",
                "FilmName": "FilmName4",
                "DiscordUserID": "UserD",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film5",
                "FilmName": "FilmName5",
                "DiscordUserID": "UserE",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": None,
                "DateNominated": (d + timedelta(seconds=1)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{d.isoformat()}#film6",
                "FilmName": "FilmName6",
                "DiscordUserID": "UserF",
                "CastVotes": 10,
                "AttendanceVotes": 9,
                "UsersAttended": set(["A", "B", "C"]),
                "DateNominated": d.isoformat(),
            },
        ]

        expected = [
            Film(
                FilmID="film1",
                FilmName="FilmName1",
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
            set_db(self.dynamodb_client, {guild: input})

            self.assertEqual(filmbot.get_nominations(), expected)
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
                "CastVotes": 0,
                "AttendanceVotes": 7,
                "UsersAttended": None,
                "DateNominated": d.isoformat(),
            },
            {
                "SK": "FILM#NOMINATED#film2",
                "FilmName": "FilmName2",
                "DiscordUserID": "UserB",
                "CastVotes": 3,
                "AttendanceVotes": 3,
                "UsersAttended": None,
                "DateNominated": (d + timedelta(seconds=2)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{(d + timedelta(seconds=1)).isoformat()}#film3",
                "FilmName": "FilmName3",
                "DiscordUserID": "UserC",
                "CastVotes": 2,
                "AttendanceVotes": 4,
                "UsersAttended": set(["A", "B", "C"]),
                "DateNominated": (d + timedelta(seconds=1)).isoformat(),
            },
            {
                "SK": f"FILM#WATCHED#{d.isoformat()}#film4",
                "FilmName": "FilmName4",
                "DiscordUserID": "UserD",
                "CastVotes": 10,
                "AttendanceVotes": 9,
                "UsersAttended": set(["A"]),
                "DateNominated": (d + timedelta(seconds=3)).isoformat(),
            },
        ]

        expected = [
            Film(
                FilmID="film1",
                FilmName="FilmName1",
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
            set_db(self.dynamodb_client, {guild: input})

            self.assertEqual(filmbot.get_all_films(), expected)
            count += 1

        assert count == factorial(len(input_films))

    def test_nominate_film(self):
        user_id1 = "user1"
        film_id1 = str(uuid1())
        film_name1 = "My Film"
        time1 = datetime(2001, 1, 2, 3, 4, 5, 123)

        guild1 = "GUILD1"
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild1)

        filmbot.nominate_film(
            DiscordUserID=user_id1,
            FilmName=film_name1,
            NewFilmID=film_id1,
            DateTime=time1,
        )

        self.assertEqual(
            grab_db(self.dynamodb_client),
            {
                guild1: [
                    {
                        "SK": f"FILM#NOMINATED#{film_id1}",
                        "FilmName": film_name1,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": time1.isoformat(),
                    },
                    {
                        "SK": f"USER#{user_id1}",
                        "NominatedFilmID": film_id1,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                ]
            },
        )

        # Check nominating fails when you already have a nomination
        film_name2 = "My Film 2: The Sequel"
        with self.assertRaises(UserError):
            filmbot.nominate_film(
                DiscordUserID=user_id1,
                FilmName=film_name2,
                NewFilmID=film_id1,
                DateTime=time1,
            )

        user_id2 = "user2"
        film_id2 = str(uuid1())
        time2 = datetime(2002, 1, 2, 3, 4, 5, 678)
        filmbot.nominate_film(
            DiscordUserID=user_id2,
            FilmName=film_name2,
            NewFilmID=film_id2,
            DateTime=time2,
        )

        expected = {
            guild1: [
                {
                    "SK": f"FILM#NOMINATED#{film_id1}",
                    "FilmName": film_name1,
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": time1.isoformat(),
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id2}",
                    "FilmName": film_name2,
                    "DiscordUserID": user_id2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": time2.isoformat(),
                },
                {
                    "SK": f"USER#{user_id1}",
                    "NominatedFilmID": film_id1,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
                {
                    "SK": f"USER#{user_id2}",
                    "NominatedFilmID": film_id2,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
            ]
        }
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't reuse film IDs
        user_id3 = "user3"
        film_name3 = "My Film 3: The Return of The Unit Test"
        with self.assertRaises(UserError):
            filmbot.nominate_film(
                DiscordUserID=user_id3,
                FilmName=film_name3,
                NewFilmID=film_id1,
                DateTime=time1,
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        guild2 = "guild2"
        filmbot2 = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild2)
        # Nominate exactly the same as we did for guild1 and check that it's
        # fine and all added under the other guild PK
        filmbot2.nominate_film(
            DiscordUserID=user_id1,
            FilmName=film_name1,
            NewFilmID=film_id1,
            DateTime=time1,
        )

        self.assertEqual(
            grab_db(self.dynamodb_client),
            {
                guild1: [
                    {
                        "SK": f"FILM#NOMINATED#{film_id1}",
                        "FilmName": film_name1,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": time1.isoformat(),
                    },
                    {
                        "SK": f"FILM#NOMINATED#{film_id2}",
                        "FilmName": film_name2,
                        "DiscordUserID": user_id2,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": time2.isoformat(),
                    },
                    {
                        "SK": f"USER#{user_id1}",
                        "NominatedFilmID": film_id1,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                    {
                        "SK": f"USER#{user_id2}",
                        "NominatedFilmID": film_id2,
                        "VoteID": None,
                        "AttendanceVoteID": None,
                    },
                ],
                guild2: [
                    {
                        "SK": f"FILM#NOMINATED#{film_id1}",
                        "FilmName": film_name1,
                        "DiscordUserID": user_id1,
                        "CastVotes": 0,
                        "AttendanceVotes": 0,
                        "UsersAttended": None,
                        "DateNominated": time1.isoformat(),
                    },
                    {
                        "SK": f"USER#{user_id1}",
                        "NominatedFilmID": film_id1,
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
        film_watched = "Film4"
        d = datetime(2010, 1, 2, 3, 4, 5, 678)
        ages_ago = d - timedelta(days=100)
        expected = {
            guild1: [
                {
                    "SK": f"FILM#NOMINATED#{film_id1}",
                    "FilmName": "My Film 1",
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id2}",
                    "FilmName": "My Film 2",
                    "DiscordUserID": user_id2,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#NOMINATED#{film_id3}",
                    "FilmName": "My Film 3",
                    "DiscordUserID": "dummy",
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"FILM#WATCHED#{ages_ago.isoformat()}#Super-old-film",
                    "FilmName": "My Film 4 (Watched)",
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": ages_ago.isoformat(),
                },
                {
                    "SK": f"FILM#WATCHED#{d.isoformat()}#{film_watched}",
                    "FilmName": "My Film 5 (Watched)",
                    "DiscordUserID": user_id1,
                    "CastVotes": 0,
                    "AttendanceVotes": 0,
                    "UsersAttended": None,
                    "DateNominated": d.isoformat(),
                },
                {
                    "SK": f"USER#{user_id1}",
                    "NominatedFilmID": film_id1,
                    "VoteID": None,
                    "AttendanceVoteID": "dummy",
                },
                {
                    "SK": f"USER#{user_id2}",
                    "NominatedFilmID": film_id2,
                    "VoteID": None,
                    "AttendanceVoteID": "dummy2",
                },
                {
                    "SK": f"USER#{user_id3}",
                    "NominatedFilmID": None,
                    "VoteID": None,
                    "AttendanceVoteID": None,
                },
            ]
        }

        # Create indices into `expected` that we can use later on
        FILM_1 = 0
        FILM_2 = 1
        FILM_3 = 2
        USER_1 = 5
        USER_2 = 6
        USER_3 = 7

        # Set up the database
        set_db(self.dynamodb_client, expected)
        self.assertEqual(grab_db(self.dynamodb_client), expected)
        filmbot = FilmBot(DynamoDBClient=self.dynamodb_client, GuildID=guild1)

        # Check we can't vote if we're not registered
        with self.assertRaises(UserError):
            filmbot.cast_preference_vote(
                DiscordUserID="not registered", FilmID=film_id1
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't vote for an already watched film
        with self.assertRaises(
            self.dynamodb_client.exceptions.TransactionCanceledException
        ):
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, FilmID=film_watched
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't vote for a non-existent film
        with self.assertRaises(
            self.dynamodb_client.exceptions.TransactionCanceledException
        ):
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, FilmID="not existent"
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can vote with no previous vote
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, FilmID=film_id2
            ),
            VotingStatus.UNCOMPLETE,
        )

        expected[guild1][FILM_2]["CastVotes"] += 1
        expected[guild1][USER_1]["VoteID"] = film_id2
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can vote for the same film as it's a shortcut path in the code
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, FilmID=film_id2
            ),
            VotingStatus.UNCOMPLETE,
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can change our vote
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, FilmID=film_id3
            ),
            VotingStatus.UNCOMPLETE,
        )
        expected[guild1][FILM_2]["CastVotes"] -= 1
        expected[guild1][FILM_3]["CastVotes"] += 1
        expected[guild1][USER_1]["VoteID"] = film_id3
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't vote for our nomination
        with self.assertRaises(UserError):
            filmbot.cast_preference_vote(
                DiscordUserID=user_id1, FilmID=film_id1
            ),
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check that we know when voting is finished
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id2, FilmID=film_id1
            ),
            VotingStatus.UNCOMPLETE,
        )
        expected[guild1][FILM_1]["CastVotes"] += 1
        expected[guild1][USER_2]["VoteID"] = film_id1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id3, FilmID=film_id1
            ),
            VotingStatus.COMPLETE,
        )
        expected[guild1][FILM_1]["CastVotes"] += 1
        expected[guild1][USER_3]["VoteID"] = film_id1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check that we can change votes when voting is finished
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id3, FilmID=film_id2
            ),
            VotingStatus.COMPLETE,
        )
        expected[guild1][FILM_1]["CastVotes"] -= 1
        expected[guild1][FILM_2]["CastVotes"] += 1
        expected[guild1][USER_3]["VoteID"] = film_id2
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can vote for the same film once voting is finished
        self.assertEqual(
            filmbot.cast_preference_vote(
                DiscordUserID=user_id3, FilmID=film_id2
            ),
            VotingStatus.COMPLETE,
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        good_time = d + timedelta(hours=24)
        bad_time = good_time - timedelta(seconds=1)

        # Check that we can't watch a film that doesn't exist
        with self.assertRaises(UserError):
            filmbot.start_watching_film(
                FilmID="non existent",
                DateTime=good_time,
                PresentUserIDs=[user_id1],
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check that we can't watch a film that has already been watched
        with self.assertRaises(UserError):
            filmbot.start_watching_film(
                FilmID=film_watched,
                DateTime=good_time,
                PresentUserIDs=[user_id1],
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can't watch another film within 24 hours
        with self.assertRaises(UserError):
            filmbot.start_watching_film(
                FilmID=film_id1, DateTime=bad_time, PresentUserIDs=[user_id1]
            )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can watch a film with multiple users initially present
        with snapshot(self.dynamodb_client) as exp:
            self.assertEqual(
                filmbot.start_watching_film(
                    FilmID=film_id1,
                    DateTime=good_time,
                    PresentUserIDs=[user_id1, user_id2, user_id3],
                ),
                "My Film 1",
            )

            # Update our users
            exp[guild1][USER_1]["NominatedFilmID"] = None
            exp[guild1][USER_1]["VoteID"] = None
            exp[guild1][USER_1]["AttendanceVoteID"] = film_id1
            exp[guild1][USER_2]["VoteID"] = None
            exp[guild1][USER_2]["AttendanceVoteID"] = film_id1
            exp[guild1][USER_3]["VoteID"] = None
            exp[guild1][USER_3]["AttendanceVoteID"] = film_id1

            # Update our nomination votes for user2 (user1 nominated the watched
            # film and user3 has no nomination)
            exp[guild1][FILM_2]["AttendanceVotes"] += 1

            # Move the film to the `WATCHED` section
            watched_film = exp[guild1].pop(FILM_1)
            watched_film[
                "SK"
            ] = f"FILM#WATCHED#{good_time.isoformat()}#{film_id1}"
            watched_film["UsersAttended"] = set([user_id1, user_id2, user_id3])
            exp[guild1].insert(4, watched_film)
            self.assertEqual(grab_db(self.dynamodb_client), exp)

        # Check we can watch a film with just one user
        self.assertEqual(
            filmbot.start_watching_film(
                FilmID=film_id1, DateTime=good_time, PresentUserIDs=[user_id1]
            ),
            "My Film 1",
        )

        # Update our users
        expected[guild1][USER_1]["NominatedFilmID"] = None
        expected[guild1][USER_1]["VoteID"] = None
        expected[guild1][USER_1]["AttendanceVoteID"] = film_id1
        expected[guild1][USER_2]["VoteID"] = None
        expected[guild1][USER_2]["AttendanceVoteID"] = None
        expected[guild1][USER_3]["VoteID"] = None

        # Move the film to the `WATCHED` section
        watched_film = expected[guild1].pop(FILM_1)
        watched_film["SK"] = f"FILM#WATCHED#{good_time.isoformat()}#{film_id1}"
        watched_film["UsersAttended"] = set([user_id1])
        expected[guild1].insert(4, watched_film)
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Fixup the indices
        FILM_1 = 4
        FILM_2 = 0
        FILM_3 = 1
        USER_1 = 5
        USER_2 = 6
        USER_3 = 7
        self.assertEqual(grab_db(self.dynamodb_client), expected)

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

        # Check we recording attendance for the nominator is a noop
        self.assertEqual(
            filmbot.record_attendance_vote(
                DiscordUserID=user_id1, DateTime=good_time
            ),
            AttendanceStatus.ALREADY_REGISTERED,
        )
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can record attendance on the cut off with a user
        # who has a nominated film
        just_in_time = good_time + timedelta(hours=4)
        self.assertEqual(
            filmbot.record_attendance_vote(
                DiscordUserID=user_id2, DateTime=just_in_time
            ),
            AttendanceStatus.REGISTERED,
        )
        expected[guild1][USER_2]["AttendanceVoteID"] = film_id1
        expected[guild1][FILM_1]["UsersAttended"].add(user_id2)
        expected[guild1][FILM_2]["AttendanceVotes"] += 1
        self.assertEqual(grab_db(self.dynamodb_client), expected)

        # Check we can record attendance for a user with no nominated film
        self.assertEqual(
            filmbot.record_attendance_vote(
                DiscordUserID=user_id3, DateTime=just_in_time
            ),
            AttendanceStatus.REGISTERED,
        )
        expected[guild1][USER_3]["AttendanceVoteID"] = film_id1
        expected[guild1][FILM_1]["UsersAttended"].add(user_id3)
        self.assertEqual(grab_db(self.dynamodb_client), expected)


if __name__ == "__main__":
    unittest.main()
