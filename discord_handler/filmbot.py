import boto3
from UserError import UserError

MEMBERS_TABLE = "filmbot-members"
# filmbot-members
# ---------------
# This table is a list of all users who are members of the Film Club.  It is keyed by
# each user's Discord user ID.
#
#   +---------------------+---------+----------------------------+
#   | field name          | type    | notes                      |
#   +---------------------+---------+----------------------------+
#   | discord-user-id*    | string  |                            |
#   | date-joined         | date    |                            |
#   | nominated-film-id   | ?string | key of filmbot-nominations |
#   | vote-id             | ?string | key of filmbot-votes       |
#   | attendance-vote-id  | ?string | key of filmbot-votes       |
#   +---------------------+---------+----------------------------+
#

NOMINATIONS_TABLE = "filmbot-nominations"
# filmbot-nominations
# -------------------
# This table is a list of all nominated films, their details, and the number of current votes.
#
#   +------------------+---------+
#   | field name       | type    |
#   +------------------+---------+
#   | film-id*         | string  |
#   | imdb-film-id     | ?string |
#   | film-title       | string  |
#   | film-genre       | ?string |
#   | discord-user-id  | string  |
#   | cast-votes       | integer |
#   | attendance-votes | integer |
#   | date-nominated   | date    |
#   | date-watched     | ?date   |
#   +------------------+---------+
#

VOTES_TABLE = "filmbot-votes"
# filmbot-votes
# -------------
# This film is a list of all votes cast by users, either explicitly or by attendance.
#
#   +------------------+---------+----------------------------+
#   | field name       | type    | notes                      |
#   +------------------+---------+----------------------------+
#   | vote-id*         | string  | UUID                       |
#   | film-id          | string  | key of filmbot-nominations |
#   | discord-user-id  | string  |                            |
#   | type             | integer | 0=vote 1=attendance        |
#   | date-cast        | date    |                            |
#   +------------------+---------+----------------------------+
#


class FilmBot:
    def __init__(self, dynamodb, dynamodb_client):
        self._members_table = dynamodb.Table(MEMBERS_TABLE)
        self._nominations_table = dynamodb.Table(NOMINATIONS_TABLE)
        self._vote_table = dynamodb.Table(VOTES_TABLE)
        self._dynamodb_client = dynamodb_client

    @property
    def members_table(self):
        return self._members_table

    @property
    def nominations_table(self):
        return self._nominations_table

    @property
    def votes_table(self):
        return self._votes_table

    @property
    def client(self):
        return self._dynamodb_client

    def register_user(self, discord_user_id, datetime):
        """Attempt to register the specified `discord_user_id` at the specified `datetime`.
        If `discord_user_id` is already registered, then throw an exception."""
        try:
            self.members_table.put_item(
                Item={
                    "discord-user-id": discord_user_id,
                    "date-joined": datetime.isoformat(),
                    "nominated-film-id": None,
                    "vote-id": None,
                    "attendance-vote-id": None,
                },
                ConditionExpression="attribute_not_exists(discord-user-id)",
            )
        except self.client.exceptions.ConditionalCheckFailedException as e:
            raise UserError("User is already registered")

    def nominate_film(self, *, DiscordUserID, FilmName, NewFilmID, DateTime):
        """Attempt to nominate the specified `FilmName` as the film choice for the specified `DiscordUserID`.
        If `DiscordUserID` is not a registered user, or `DiscordUserID` already has a nomination then throw an exception."""

        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": MEMBERS_TABLE,
                            "Key": {"discord-user-id": {"S": DiscordUserID}},
                            "ExpressionAttributeValues": {
                                ":new_film_id": {"S": NewFilmID},
                                ":null": {"NULL": True},
                            },
                            "ExpressionAttributeNames": {
                                "#nominated": "nominated-film-id",
                                "#discord-user-id": "discord-user-id",
                            },
                            "ConditionExpression": "attribute_exists(#discord-user-id) AND #nominated = :null",
                            "UpdateExpression": "SET #nominated = :new_film_id",
                        }
                    },
                    {
                        "Put": {
                            "TableName": NOMINATIONS_TABLE,
                            "Item": {
                                "film-id": {"S": NewFilmID},
                                "imdb-film-id": {"NULL": True},
                                "film-title": {"S": FilmName},
                                "film-genre": {"NULL": True},
                                "discord-user-id": {"S": DiscordUserID},
                                "cast-votes": {"N": "0"},
                                "attendance-votes": {"N": "0"},
                                "date-nominated": {"S": DateTime.isoformat()},
                                "date-watched": {"NULL": True},
                            },
                        }
                    },
                ]
            )
        except self.client.exceptions.TransactionCanceledException as e:
            # Note that we can hit this if the user is not registered too, but all
            # users in the Discord channel should be registered.  Ideally the
            # `TransactionCanceledException` would return the existing value in the
            # database (when setting) `ReturnValuesOnConditionCheckFailure`, but it
            # only works in the Java SDK.
            raise UserError(
                "Unable to nominate a film as you have already nominated one"
            )

    def cast_preference_vote(self, discord_user_id, imdb_film_id):
        # TODO
        assert False

    def record_attendance_vote(self, discord_user_id):
        # TODO
        assert False

    def start_watching_film(self, imdb_film_id):
        # TODO
        assert False

    def stop_recording_attendance(self):
        # TODO
        assert False
