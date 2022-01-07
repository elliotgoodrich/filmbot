import boto3
from enum import Enum
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
def keyed(v):
    """Convert the specified `v` into a dict keyed by the type, that will be accepted by DynamoDB"""
    if isinstance(v, bool):
        return {"BOOL": v}
    elif isinstance(v, int):
        return {"N": str(v)}
    elif isinstance(v, str):
        return {"S": v}
    elif v is None:
        return {"NULL": True}
    else:
        assert False, "'" + v + "' is not an accepted input for 'keyed'"


def unkeyed(v):
    """Convert the specified `v` from DynamoDB's dict keyed by the type to
    a primitive Python type."""
    for typeName in v:
        value = v[typeName]
        if typeName == "BOOL":
            return value
        if typeName == "S":
            return value
        elif typeName == "N":
            return int(value)
        elif typeName == "NULL":
            return None
        else:
            assert False, "'" + typeName + "' is not an understood type for 'unkeyed'"


def liftOutTypes(map):
    """Unkey the value for every element of the specified `map`."""
    result = {}
    for key in map:
        result[key] = unkeyed(map[key])
    return result


class VotingStatus(Enum):
    UNCOMPLETE = 0
    COMPLETE = 1


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

    def get_users(self):
        """Return a dictionary keyed by users against their votes, nominations, etc."""
        scan_kwargs = {
            "Select": "ALL_ATTRIBUTES",
            "ReturnConsumedCapacity": "NONE",
        }

        members_table = self.members_table
        done = False
        start_key = None
        users = {}
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = members_table.scan(**scan_kwargs)
            for user in response.get("Items"):
                users[user["discord-user-id"]] = user
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

        return users

    def get_nominations(self):
        """Return an array of currently nominated films in the order that they should
        be watched based on their vote tally."""

        film_ids = list(
            map(
                lambda n: {"film-id": {"S": n["nominated-film-id"]}},
                filter(lambda n: n["nominated-film-id"], self.get_users().values()),
            )
        )

        response = self.client.batch_get_item(
            RequestItems={
                NOMINATIONS_TABLE: {"Keys": film_ids, "ConsistentRead": True}
            },
            ReturnConsumedCapacity="NONE",
        )

        cleaned = map(liftOutTypes, response["Responses"][NOMINATIONS_TABLE])
        return sorted(
            cleaned, reverse=True, key=lambda n: n["cast-votes"] + n["attendance-votes"]
        )

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

    def cast_preference_vote(self, *, DiscordUserID, FilmID):
        """Attempt to cast a vote by the specified `DiscordUserID` for the specified `FilmID` (changing any previously cast vote)
        If `DiscordUserID` is not a registered user, or `FilmID` refers to your nominated film, or `DiscordUserID` already has a nomination then throw an exception.
        Return whether the voting is complete"""

        users = self.get_users()
        if DiscordUserID not in users:
            raise UserError("User is not registered")

        our_user = users[DiscordUserID]
        previous_vote = our_user["vote-id"]

        # Disallow voting for your own nomination
        if FilmID == our_user["nominated-film-id"]:
            raise UserError("Cannot vote for your own film")

        # Check everyone has nominated
        user_list = users.values()
        everyone_has_nominated = any(u for u in user_list if u["nominated-film-id"])
        if not everyone_has_nominated:
            raise UserError("Cannot vote unless all users have nominated")

        # Record if this is the last user to vote
        user_voted_count = sum(user["vote-id"] is not None for user in user_list)
        our_user_hasnt_voted = our_user["vote-id"] is None
        is_last_vote = user_voted_count + int(our_user_hasnt_voted) == len(user_list)

        # Do nothing if user votes for the same thing
        if previous_vote == FilmID:
            return (
                VotingStatus.COMPLETE
                if user_voted_count == len(user_list)
                else VotingStatus.UNCOMPLETE
            )

        items = [
            # Change vote-id and make sure it matches the one we previously read
            # i.e. there haven't been any changes between our read and this write
            {
                "Update": {
                    "TableName": MEMBERS_TABLE,
                    "Key": {"discord-user-id": {"S": DiscordUserID}},
                    "ExpressionAttributeValues": {
                        ":new_vote_id": {"S": FilmID},
                        ":previous_vote_id": keyed(previous_vote),
                    },
                    "ExpressionAttributeNames": {
                        "#vote": "vote-id",
                        "#discord-user-id": "discord-user-id",
                    },
                    "ConditionExpression": "attribute_exists(#discord-user-id) AND #vote = :previous_vote_id",
                    "UpdateExpression": "SET #vote = :new_vote_id",
                }
            },
            # Increment vote count in nominations for new film (also check it exists)
            {
                "Update": {
                    "TableName": NOMINATIONS_TABLE,
                    "Key": {"film-id": {"S": FilmID}},
                    "ExpressionAttributeValues": {
                        ":inc": {"N": "1"},
                    },
                    "ExpressionAttributeNames": {
                        "#cast_votes": "cast-votes",
                        "#film-id": "film-id",
                    },
                    "ConditionExpression": "attribute_exists(#film-id)",
                    "UpdateExpression": "SET #cast_votes = #cast_votes + :inc",
                }
            },
        ]

        if previous_vote is not None:
            # Decrement vote count for previous film
            items.append(
                {
                    "Update": {
                        "TableName": NOMINATIONS_TABLE,
                        "Key": {"film-id": {"S": previous_vote}},
                        "ExpressionAttributeValues": {
                            ":dec": {"N": "-1"},
                        },
                        "ExpressionAttributeNames": {
                            "#cast_votes": "cast-votes",
                        },
                        # We don't need a ConditionExpression as we should never be updating
                        # something that wasn't in the table
                        "UpdateExpression": "SET #cast_votes = #cast_votes + :dec",
                    }
                }
            )

        # The transactions below really shouldn't throw.  The only way this throws is if we:
        #   1. aren't registered,
        #   2. vote for a `FilmID` that doesn't exists, or
        #   3. the user votes (very) quickly in succession so updating our
        #      users table fails because the current film we voted on doesn't
        #      match what we previously extracted.
        #
        # If any of these actually happen in practice then a more descriptive
        # error message can be returned, or hopefully the issue can be fixed.
        self.client.transact_write_items(TransactItems=items)
        return (
            VotingStatus.COMPLETE
            if user_voted_count + int(our_user_hasnt_voted) == len(user_list)
            else VotingStatus.UNCOMPLETE
        )

    def start_watching_film(self, imdb_film_id):
        # TODO
        assert False

    def record_attendance_vote(self, discord_user_id):
        # TODO
        assert False

    def stop_recording_attendance(self):
        # TODO
        assert False
