import unittest
import boto3
from moto import mock_dynamodb2
from filmbot import FilmBot, MEMBERS_TABLE, NOMINATIONS_TABLE, VOTES_TABLE
from datetime import datetime, timedelta
from uuid import uuid1
from UserError import UserError

AWS_REGION = "eu-west-2"


class TestFilmBot(unittest.TestCase):
    mock_dynamodb2 = mock_dynamodb2()

    def setUp(self):
        """
        Mock `dynamodb2` and create the tables we expect.
        """
        self.mock_dynamodb2.start()
        boto3.setup_default_session()
        self.dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
        self.dynamodb_client = boto3.client("dynamodb", region_name=AWS_REGION)

        # Create our 3 expected tables
        self.dynamodb.create_table(
            TableName=MEMBERS_TABLE,
            KeySchema=[{"AttributeName": "discord-user-id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "discord-user-id", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        self.dynamodb.create_table(
            TableName=NOMINATIONS_TABLE,
            KeySchema=[{"AttributeName": "film-id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "film-id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self.dynamodb.create_table(
            TableName=VOTES_TABLE,
            KeySchema=[{"AttributeName": "vote-id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "vote-id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Check all tables have been created
        self.assertEqual(
            self.dynamodb_client.list_tables()["TableNames"],
            [MEMBERS_TABLE, NOMINATIONS_TABLE, VOTES_TABLE],
        )

        # Create our `FilmBot`
        self.filmbot = FilmBot(self.dynamodb, self.dynamodb_client)
        pass

    def tearDown(self):
        """
        Unmock `dynamodb2`.
        """
        self.mock_dynamodb2.stop()
        pass

    def test_register_user(self):
        """
        Test `register_user`.
        """

        # Check we can register users successfully
        dummy_user_id = "12345"
        time_registered = datetime(2000, 12, 31, 23, 59, 59, 999999)
        self.filmbot.register_user(dummy_user_id, time_registered)
        self.assertEqual(
            self.dynamodb_client.describe_table(TableName=MEMBERS_TABLE)["Table"][
                "ItemCount"
            ],
            1,
        )

        table = self.dynamodb.Table(MEMBERS_TABLE)
        record = table.get_item(Key={"discord-user-id": dummy_user_id})
        self.assertEqual(
            record["Item"],
            {
                "discord-user-id": dummy_user_id,
                "date-joined": time_registered.isoformat(),
                "nominated-film-id": None,
                "vote-id": None,
                "attendance-vote-id": None,
            },
        )

        # Check registering duplicate Discord IDs throw
        with self.assertRaises(UserError):
            self.filmbot.register_user(
                dummy_user_id, time_registered + timedelta(days=1)
            )
        pass

    def test_nominate_film(self):
        dummy_user_id = "12345"
        time_registered = datetime(2000, 12, 31, 23, 59, 59, 999999)
        self.filmbot.register_user(dummy_user_id, time_registered)

        film_id = str(uuid1())
        film_name = "My Film"
        nominated = datetime(2001, 1, 2, 3, 4, 5)

        self.filmbot.nominate_film(
            DiscordUserID=dummy_user_id,
            FilmName=film_name,
            NewFilmID=film_id,
            DateTime=nominated,
        )

        members_table = self.dynamodb.Table(MEMBERS_TABLE)
        members_record = members_table.get_item(Key={"discord-user-id": dummy_user_id})
        self.assertEqual(
            members_record["Item"],
            {
                "discord-user-id": dummy_user_id,
                "date-joined": time_registered.isoformat(),
                "nominated-film-id": film_id,
                "vote-id": None,
                "attendance-vote-id": None,
            },
        )

        nominations_table = self.dynamodb.Table(NOMINATIONS_TABLE)
        nominations_record = nominations_table.get_item(Key={"film-id": film_id})
        self.assertEqual(
            nominations_record["Item"],
            {
                "film-id": film_id,
                "imdb-film-id": None,
                "film-title": film_name,
                "film-genre": None,
                "discord-user-id": dummy_user_id,
                "cast-votes": 0,
                "attendance-votes": 0,
                "date-nominated": nominated.isoformat(),
                "date-watched": None,
            },
        )

        # Check nominating fails when you already have a nomination
        with self.assertRaises(UserError):
            self.filmbot.nominate_film(
                DiscordUserID=dummy_user_id,
                FilmName="My New Film",
                NewFilmID=str(uuid1()),
                DateTime=nominated,
            )

        # Check nominating fails for a non-registered user
        with self.assertRaises(UserError):
            self.filmbot.nominate_film(
                DiscordUserID="non-existent user",
                FilmName="My New Film",
                NewFilmID=str(uuid1()),
                DateTime=nominated,
            )


if __name__ == "__main__":
    unittest.main()
