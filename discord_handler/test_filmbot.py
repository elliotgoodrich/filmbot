import unittest
import boto3
from botocore.exceptions import ClientError
from moto import mock_dynamodb2
from filmbot import FilmBot, MEMBERS_TABLE, NOMINATIONS_TABLE, VOTES_TABLE
from datetime import datetime, timedelta

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
        self.filmbot = FilmBot(self.dynamodb)
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
        self.assertRaises(
            ClientError,
            self.filmbot.register_user,
            dummy_user_id,
            time_registered + timedelta(days=1),
        )
        pass


if __name__ == "__main__":
    unittest.main()
