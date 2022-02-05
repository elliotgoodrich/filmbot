from pprint import pprint
from enum import Enum
from UserError import UserError
from datetime import date, timedelta, datetime

TABLE_NAME = "FilmBotTable"

USER_PK = "PK"
USER_SK = "SK"
USER_NominatedFilmID = "NominatedFilmID"
USER_VoteID = "VoteID"
USER_AttendanceVoteID = "AttendanceVoteID"


FILM_PK = "PK"
FILM_SK = "SK"
FILM_FilmName = "FilmName"
FILM_IMDbID = "IMDbID"
FILM_DiscordUserID = "DiscordUserID"
FILM_CastVotes = "CastVotes"
FILM_AttendanceVotes = "AttendanceVotes"
FILM_UsersAttended = "UsersAttended"
FILM_DateNominated = "DateNominated"


class User:
    def __init__(
        self,
        *,
        DiscordUserID,
        NominatedFilmID,
        VoteID,
        AttendanceVoteID,
    ):
        self.DiscordUserID = DiscordUserID
        self.NominatedFilmID = NominatedFilmID
        self.VoteID = VoteID
        self.AttendanceVoteID = AttendanceVoteID

    @property
    def SK(self):
        return f"USER#{self.DiscordUserID}"

    def __eq__(self, other):
        return (
            self.DiscordUserID == other.DiscordUserID
            and self.NominatedFilmID == other.NominatedFilmID
            and self.VoteID == other.VoteID
            and self.AttendanceVoteID == other.AttendanceVoteID
        )

    def toDict(self, *, GuildID):
        return {
            "PK": {"S": GuildID},
            "SK": {"S": self.SK},
            "NominatedFilmID": keyed(self.NominatedFilmID),
            "VoteID": keyed(self.VoteID),
            "AttendanceVoteID": keyed(self.AttendanceVoteID),
        }

    @staticmethod
    def fromDict(dict):
        return User(
            DiscordUserID=dict[USER_SK]["S"].split("#")[-1],
            NominatedFilmID=unkeyed(dict[USER_NominatedFilmID]),
            VoteID=unkeyed(dict[USER_VoteID]),
            AttendanceVoteID=unkeyed(dict[USER_AttendanceVoteID]),
        )


class Film:
    def __init__(
        self,
        *,
        FilmID,
        FilmName,
        IMDbID,
        DiscordUserID,
        CastVotes,
        AttendanceVotes,
        UsersAttended,
        DateNominated,
        DateWatched,
    ):
        self.FilmID = FilmID
        self.FilmName = FilmName
        self.IMDbID = IMDbID
        self.DiscordUserID = DiscordUserID
        self.CastVotes = CastVotes
        self.AttendanceVotes = AttendanceVotes
        self.UsersAttended = UsersAttended
        self.DateNominated = DateNominated
        self.DateWatched = DateWatched

    @property
    def SK(self):
        return (
            f"FILM#NOMINATED#{self.FilmID}"
            if self.DateWatched is None
            else f"FILM#WATCHED#{datetime.isoformat(self.DateWatched)}#{self.FilmID}"
        )

    def __eq__(self, other):
        return (
            self.FilmID == other.FilmID
            and self.FilmName == other.FilmName
            and self.IMDbID == other.IMDbID
            and self.DiscordUserID == other.DiscordUserID
            and self.CastVotes == other.CastVotes
            and self.AttendanceVotes == other.AttendanceVotes
            and self.UsersAttended == other.UsersAttended
            and self.DateNominated == other.DateNominated
            and self.DateWatched == other.DateWatched
        )

    def __repr__(self):
        return (
            f"SK={self.SK}\n"
            f"FilmID={self.FilmID}\n"
            f"FilmName={self.FilmName}\n"
            f"IMDbID={self.IMDbID}\n"
            f"DiscordUserID={self.DiscordUserID}\n"
            f"CastVotes={self.CastVotes}\n"
            f"AttendanceVotes={self.AttendanceVotes}\n"
            f"UsersAttended={self.UsersAttended}\n"
            f"DateNominated={self.DateNominated}\n"
            f"DateWatched={self.DateWatched}"
        )

    def toDict(self, *, GuildID):
        return {
            "PK": {"S": GuildID},
            "SK": {"S": self.SK},
            "FilmName": {"S": self.FilmName},
            "IMDbID": keyed(self.IMDbID),
            "DiscordUserID": {"S": self.DiscordUserID},
            "CastVotes": {"N": str(self.CastVotes)},
            "AttendanceVotes": {"N": str(self.AttendanceVotes)},
            "UsersAttended": (
                {"NULL": True}
                if self.UsersAttended is None
                else {"SS": list(self.UsersAttended)}
            ),
            "DateNominated": {"S": datetime.isoformat(self.DateNominated)},
        }

    @staticmethod
    def fromDict(dict):
        sk_parts = dict[FILM_SK]["S"].split("#")
        assert len(sk_parts) >= 3
        assert sk_parts[0] == "FILM"
        return Film(
            FilmID=sk_parts[-1],
            FilmName=dict[FILM_FilmName]["S"],
            IMDbID=unkeyed(dict[FILM_IMDbID]),
            DiscordUserID=dict[FILM_DiscordUserID]["S"],
            CastVotes=int(dict[FILM_CastVotes]["N"]),
            AttendanceVotes=int(dict[FILM_AttendanceVotes]["N"]),
            UsersAttended=unkeyed(dict[FILM_UsersAttended]),
            DateNominated=datetime.fromisoformat(
                dict[FILM_DateNominated]["S"]
            ),
            DateWatched=(
                datetime.fromisoformat(sk_parts[2])
                if sk_parts[1] == "WATCHED"
                else None
            ),
        )


def extract_SK(sortKeyValue):
    return sortKeyValue.split("#")[-1]


def extract_watched(sortKeyValue):
    FILM, WATCHED, watch_time, film_id = sortKeyValue.split("#")
    assert FILM == "FILM"
    assert WATCHED == "WATCHED"
    return watch_time, film_id


def keyed(v):
    """Convert the specified `v` into a dict keyed by the type, that will be accepted by DynamoDB"""
    if isinstance(v, bool):
        return {"BOOL": v}
    elif isinstance(v, int):
        return {"N": str(v)}
    elif isinstance(v, str):
        return {"S": v}
    elif isinstance(v, set):
        return {"SS": list(v)}
    elif v is None:
        return {"NULL": True}
    else:
        assert False, f"'{v}' is not an accepted input for 'keyed'"


def key_map(map):
    """Unkey the value for every element of the specified `map`."""
    result = {}
    for key in map:
        result[key] = keyed(map[key])
    return result


def unkeyed(v):
    """Convert the specified `v` from DynamoDB's dict keyed by the type to
    a primitive Python type."""
    for type_name in v:
        value = v[type_name]
        if type_name == "BOOL":
            return value
        if type_name == "S":
            return value
        elif type_name == "N":
            return int(value)
        elif type_name == "SS":
            return set(value)
        elif type_name == "NULL":
            return None
        else:
            assert (
                False
            ), f"'{type_name}' is not an understood type for 'unkeyed'"


def unkey_map(map):
    """Unkey the value for every element of the specified `map`."""
    result = {}
    for key in map:
        result[key] = unkeyed(map[key])
    return result


class VotingStatus(Enum):
    UNCOMPLETE = 0
    COMPLETE = 1


class AttendanceStatus(Enum):
    REGISTERED = 0
    ALREADY_REGISTERED = 1


class FilmBot:
    def __init__(self, DynamoDBClient, GuildID):
        self._dynamodb_client = DynamoDBClient
        self._guildID = GuildID

    @property
    def client(self):
        return self._dynamodb_client

    @property
    def guildID(self):
        return self._guildID

    def __query(self, kwargs):
        """
        Run a DynamoDB query with the specified `kwargs` and return the result.
        """
        start_key = None
        results = []
        while True:
            if start_key:
                kwargs["ExclusiveStartKey"] = start_key
            response = self.client.query(**kwargs)
            results += response["Items"]
            start_key = response.get("LastEvaluatedKey", None)
            if start_key is None:
                return results

    def get_users(self):
        """
        Return a dictionary keyed by users against their votes and nomination.
        """

        users = map(
            User.fromDict,
            self.__query(
                {
                    "TableName": TABLE_NAME,
                    "ExpressionAttributeValues": {
                        ":GuildID": {"S": self.guildID},
                        ":UserPrefix": {"S": "USER#"},
                    },
                    "KeyConditionExpression": (
                        f"{USER_PK} = :GuildID AND "
                        f"begins_with({USER_SK}, :UserPrefix)"
                    ),
                }
            ),
        )

        return {user.DiscordUserID: user for user in users}

    def get_nominated_film(self, FilmID):
        """
        Return a `Film` object for the nominated film with the specified `FilmID`.
        Throw an exception if there isn't a nominated film associated with
        `FilmID`.
        """
        response = self.client.get_item(
            TableName=TABLE_NAME,
            Key={
                FILM_PK: {"S": self.guildID},
                FILM_SK: {"S": f"FILM#NOMINATED#{FilmID}"},
            },
        )

        if "Item" not in response:
            raise UserError(
                f"There is no nominated film with that ID ({FilmID})"
            )

        return Film.fromDict(response["Item"])

    def get_nominations(self):
        """Return an array of currently nominated films in the order that they should
        be watched based on their vote tally."""

        nominations = map(
            Film.fromDict,
            self.__query(
                {
                    "TableName": TABLE_NAME,
                    "ExpressionAttributeValues": {
                        ":GuildID": {"S": self.guildID},
                        ":FilmPrefix": {"S": "FILM#NOMINATED#"},
                    },
                    "KeyConditionExpression": (
                        f"{FILM_PK} = :GuildID AND "
                        f"begins_with({FILM_SK}, :FilmPrefix)"
                    ),
                }
            ),
        )

        # Sort by:
        #   - the highest number of votes
        #   - if that is the same, then tie break by highest cast votes
        #   - if that is the same, then tie break by earliest nominated
        return sorted(
            nominations,
            key=lambda n: [
                -n.CastVotes - n.AttendanceVotes,
                -n.CastVotes,
                n.DateNominated,
            ],
        )

    def get_all_films(self):
        """
        Return an array watched and unwatched films in the order that they were
        nominated.
        """
        films = map(
            Film.fromDict,
            self.__query(
                {
                    "TableName": TABLE_NAME,
                    "ExpressionAttributeValues": {
                        ":GuildID": {"S": self.guildID},
                        ":FilmPrefix": {"S": "FILM#"},
                    },
                    "KeyConditionExpression": (
                        f"{FILM_PK} = :GuildID AND "
                        f"begins_with({FILM_SK}, :FilmPrefix)"
                    ),
                }
            ),
        )

        return sorted(films, key=lambda n: n.DateNominated)

    def nominate_film(
        self,
        *,
        DiscordUserID,
        FilmName,
        NewFilmID,
        IMDbID,
        DateTime,
    ):
        """
        Attempt to nominate the specified `FilmName` as the film choice, with
        the specified `IMDbID` for the specified `DiscordUserID`.  If
        `DiscordUserID` is not a registered user then register them.  If
        `DiscordUserID` already has a nomination then throw an exception.
        """

        new_film = Film(
            FilmID=NewFilmID,
            FilmName=FilmName,
            IMDbID=IMDbID,
            DiscordUserID=DiscordUserID,
            CastVotes=0,
            AttendanceVotes=0,
            # Empty string sets are not allowed so we have to use None
            UsersAttended=None,
            DateNominated=DateTime,
            DateWatched=None,
        )

        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": {
                                USER_PK: {"S": self.guildID},
                                USER_SK: {"S": f"USER#{DiscordUserID}"},
                            },
                            "ExpressionAttributeValues": {
                                ":NewFilmID": {"S": NewFilmID},
                                ":Null": {"NULL": True},
                            },
                            "ConditionExpression": (
                                f"attribute_not_exists({USER_SK}) OR "
                                f"{USER_NominatedFilmID} = :Null"
                            ),
                            # Make sure to null out the other fields in case we didn't have a user yet
                            # These should both be Null at this point as we can only nominate after we
                            # watch a film and these are cleared
                            "UpdateExpression": (
                                f"SET {USER_NominatedFilmID} = :NewFilmID, "
                                f"{USER_VoteID} = :Null, "
                                f"{USER_AttendanceVoteID} = :Null"
                            ),
                        }
                    },
                    {
                        "Put": {
                            "TableName": TABLE_NAME,
                            "Item": new_film.toDict(GuildID=self.guildID),
                            # Make sure we haven't reused this film ID before
                            "ConditionExpression": f"attribute_not_exists({FILM_SK})",
                        }
                    },
                ]
            )
        except self.client.exceptions.TransactionCanceledException as e:
            # This can also occur if we pass in a reused FilmID, but that is
            # impossible if we're using a UUID properly.
            raise UserError(
                "Unable to nominate a film as you have already nominated one"
            )

    def cast_preference_vote(self, *, DiscordUserID, FilmID):
        """
        Attempt to cast a vote for `FilmID` by `DiscordUserID` and return
        whether voting is complete.  Throw an exception if either
        `DiscordUserID` is not a registered user, FilmID` refers to that
        user's nominated film, or `FilmID` doesn't point to a nominated film.
        """

        users = self.get_users()
        if DiscordUserID not in users:
            raise UserError("You can't vote until you have nominated a film")

        our_user = users[DiscordUserID]
        previous_vote = our_user.VoteID

        # Disallow voting for your own nomination
        if FilmID == our_user.NominatedFilmID:
            raise UserError("You can't vote for your own film")

        # Record if this is the last user to vote
        user_list = users.values()
        user_voted_count = sum(user.VoteID is not None for user in user_list)
        our_user_hasnt_voted = our_user.VoteID is None

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
                    "TableName": TABLE_NAME,
                    "Key": {
                        USER_PK: {"S": self.guildID},
                        USER_SK: {"S": f"USER#{DiscordUserID}"},
                    },
                    "ExpressionAttributeValues": {
                        ":NewFilmID": {"S": FilmID},
                        ":PreviousVoteID": keyed(previous_vote),
                    },
                    "ConditionExpression": (
                        f"attribute_exists({USER_SK}) AND "
                        f"{USER_VoteID} = :PreviousVoteID"
                    ),
                    "UpdateExpression": f"SET {USER_VoteID} = :NewFilmID",
                }
            },
            # Increment vote count in nominations for new film (also check it exists)
            {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        FILM_PK: {"S": self.guildID},
                        FILM_SK: {"S": f"FILM#NOMINATED#{FilmID}"},
                    },
                    "ExpressionAttributeValues": {
                        ":One": {"N": "1"},
                    },
                    "ConditionExpression": f"attribute_exists({FILM_SK})",
                    "UpdateExpression": f"SET {FILM_CastVotes} = {FILM_CastVotes} + :One",
                }
            },
        ]

        if previous_vote is not None:
            # Decrement vote count for previous film
            items.append(
                {
                    "Update": {
                        "TableName": TABLE_NAME,
                        "Key": {
                            FILM_PK: {"S": self.guildID},
                            FILM_SK: {"S": f"FILM#NOMINATED#{previous_vote}"},
                        },
                        "ExpressionAttributeValues": {
                            ":One": {"N": "1"},
                        },
                        # We don't need a ConditionExpression as we should never be updating
                        # something that wasn't in the table
                        "UpdateExpression": f"SET {FILM_CastVotes} = {FILM_CastVotes} - :One",
                    }
                }
            )

        # The transaction can also throw if the user votes quickly in
        # succession so updating our users table fails because the
        # current film we voted on doesn't match what we previously
        # extracted.  This is unlikely to occur as the only sensible way
        # to know the `FilmID` is to use the Discord autocomplete, which
        # takes time to load.
        try:
            self.client.transact_write_items(TransactItems=items)
        except self.client.exceptions.TransactionCanceledException:
            raise UserError(
                f"There is no nominated film with that ID ({FilmID})"
            )

        return (
            VotingStatus.COMPLETE
            if user_voted_count + int(our_user_hasnt_voted) == len(user_list)
            else VotingStatus.UNCOMPLETE
        )

    def start_watching_film(self, *, FilmID, PresentUserIDs, DateTime):
        """
        Attempt to record that we're watching the specified `FilmID` and
        record an attendance vote for each user in the `PresentUserIDs` array
        and return the a `Film` object.  Also clear out all cast votes
        from all users and clear out the user's nomination who had previously
        nominated `FilmID`.  Throw an exception if `FilmID` isn't correct,
        less than 24 hours has passed since watching the last film, or
        `PresentUserIDs` is empty.
        """

        # At least one user must be present to start watching a film"
        assert PresentUserIDs

        response = self.client.get_item(
            TableName=TABLE_NAME,
            Key={
                FILM_PK: {"S": self.guildID},
                FILM_SK: {"S": f"FILM#NOMINATED#{FilmID}"},
            },
        )

        if "Item" not in response:
            raise UserError(
                f"There is no nominated film with that ID ({FilmID})"
            )

        # Check to see all user IDs are valid
        all_users = self.get_users()
        for user in PresentUserIDs:
            assert user in all_users

        film = Film.fromDict(response["Item"])
        nominator_user_id = film.DiscordUserID

        # Get the last film watched and see if enough time has passed
        response = self.client.query(
            TableName=TABLE_NAME,
            ExpressionAttributeValues={
                ":GuildID": {"S": self.guildID},
                ":WatchedPrefix": {"S": "FILM#WATCHED#"},
            },
            KeyConditionExpression=(
                f"{FILM_PK} = :GuildID AND "
                f"begins_with({FILM_SK}, :WatchedPrefix)"
            ),
            ScanIndexForward=False,
            Limit=1,
        )

        if response["Items"]:
            latest_watched_film = Film.fromDict(response["Items"][0])
            if DateTime < latest_watched_film.DateWatched + timedelta(days=1):
                raise UserError(
                    "At least 24 hours must pass before watching films"
                )

        items = []

        for user_id in all_users:
            # Either reset our vote if we aren't present, or set it to
            # the current film ID if we are
            attendance_vote = (
                {"S": FilmID} if user_id in PresentUserIDs else {"NULL": True}
            )

            user = all_users[user_id]

            # Clear our out votes
            update_exprs = [
                f"{USER_VoteID} = :Null",
                f"{USER_AttendanceVoteID} = :AttendanceVote",
            ]

            # If this was our film, clear our nomination
            if user_id == nominator_user_id:
                update_exprs.append(f"{USER_NominatedFilmID} = :Null")

            items.append(
                {
                    "Update": {
                        "TableName": TABLE_NAME,
                        "Key": {
                            USER_PK: {"S": self.guildID},
                            USER_SK: {"S": f"USER#{user_id}"},
                        },
                        "ExpressionAttributeValues": {
                            ":Null": {"NULL": True},
                            ":PreviousNomination": keyed(user.NominatedFilmID),
                            ":AttendanceVote": attendance_vote,
                        },
                        # We have already checked that the user exists
                        "ConditionExpression": f"{USER_NominatedFilmID} = :PreviousNomination",
                        "UpdateExpression": "SET " + ", ".join(update_exprs),
                    }
                }
            )

            # Add an attendance vote for all present users as long
            # as we weren't the one who nominated the film being
            # watched and we also have a nominated film
            if (
                user_id in PresentUserIDs
                and user_id != nominator_user_id
                and user.NominatedFilmID is not None
            ):
                items.append(
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": {
                                FILM_PK: {"S": self.guildID},
                                FILM_SK: {
                                    "S": f"FILM#NOMINATED#{user.NominatedFilmID}"
                                },
                            },
                            "ExpressionAttributeValues": {
                                ":One": {"N": "1"},
                            },
                            "UpdateExpression": f"ADD {FILM_AttendanceVotes} :One",
                        }
                    }
                )

        film.DateWatched = DateTime
        film.UsersAttended = set(PresentUserIDs)
        items += [
            {
                "Delete": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        FILM_PK: {"S": self.guildID},
                        FILM_SK: {"S": f"FILM#NOMINATED#{FilmID}"},
                    },
                    # Make sure the film still exists to defend against this
                    # function being called multiple times in quick succession
                    "ConditionExpression": f"attribute_exists({FILM_SK})",
                }
            },
            {
                "Put": {
                    "TableName": TABLE_NAME,
                    "Item": film.toDict(GuildID=self.guildID),
                },
            },
        ]
        self.client.transact_write_items(TransactItems=items)

        return film

    def record_attendance_vote(self, *, DiscordUserID, DateTime):
        """
        Attempt to record that the `DiscordUserID` is present and watching
        the film at the specified `DateTime`.  Throw an exception if the
        user is not registered or there is no film currently being watched.
        """
        response = self.client.get_item(
            TableName=TABLE_NAME,
            Key={
                USER_PK: {"S": self.guildID},
                USER_SK: {"S": f"USER#{DiscordUserID}"},
            },
        )
        if "Item" not in response:
            raise UserError(
                "You cannot register attendance until you have nominated"
            )

        user = User.fromDict(response["Item"])

        # Do nothing if the user has already recorded their attendance
        if user.AttendanceVoteID is not None:
            return AttendanceStatus.ALREADY_REGISTERED

        # Get the last film watched and see if we fall within the correct
        # time frame
        response = self.client.query(
            TableName=TABLE_NAME,
            ExpressionAttributeValues={
                ":GuildID": {"S": self.guildID},
                ":WatchedPrefix": {"S": "FILM#WATCHED#"},
            },
            KeyConditionExpression=(
                f"{FILM_PK} = :GuildID AND "
                f"begins_with({FILM_SK}, :WatchedPrefix)"
            ),
            ScanIndexForward=False,
            Limit=1,
        )

        if not response["Items"]:
            raise UserError("There are no films that have been watched")

        # We shouldn't be recording attendance before we started watching a
        # film, but check for this anyway.
        latest_watched_film = Film.fromDict(response["Items"][0])
        if DateTime < latest_watched_film.DateWatched:
            raise UserError(
                "Cannot record attendance for a film that hasn't yet started"
            )

        # TODO: Get runtime from IMDB and use this
        # Note that this must never be greater than the watch cooldown
        # period (currently 24 hours) otherwise it would be possible to
        # have several films watched concurrently
        end_time = latest_watched_film.DateWatched + timedelta(hours=4)
        if DateTime > end_time:
            raise UserError(
                f"The cutoff for registering attendance was {end_time}"
            )

        items = [
            {
                # Record that the user has an attendance vote
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        USER_PK: {"S": self.guildID},
                        USER_SK: {"S": user.SK},
                    },
                    "ExpressionAttributeValues": {
                        ":Null": {"NULL": True},
                        ":AttendanceVote": {"S": latest_watched_film.FilmID},
                    },
                    # Check that we haven't recorded an attendance in the meantime
                    "ConditionExpression": f"{USER_AttendanceVoteID} = :Null",
                    "UpdateExpression": f"SET {USER_AttendanceVoteID} = :AttendanceVote",
                }
            },
            {
                # Add our user to the set of those who attended
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        FILM_PK: {"S": self.guildID},
                        FILM_SK: {"S": latest_watched_film.SK},
                    },
                    "ExpressionAttributeValues": {
                        ":User": {"SS": [DiscordUserID]},
                    },
                    "UpdateExpression": f"ADD {FILM_UsersAttended} :User",
                }
            },
        ]

        # Add an attendance vote if we weren't the user who nominated the film
        # and we have a nomination.  We must check both as we could either nominate
        # straight after we start watching our film or we could have failed to nominate
        # a new film before a second film has started being watched
        if (
            latest_watched_film.DiscordUserID != user.DiscordUserID
            and user.NominatedFilmID is not None
        ):
            items.append(
                {
                    "Update": {
                        "TableName": TABLE_NAME,
                        "Key": {
                            FILM_PK: {"S": self.guildID},
                            FILM_SK: {
                                "S": f"FILM#NOMINATED#{user.NominatedFilmID}"
                            },
                        },
                        "ExpressionAttributeValues": {
                            ":One": {"N": "1"},
                        },
                        "UpdateExpression": f"ADD {FILM_AttendanceVotes} :One",
                    }
                }
            )
        self.client.transact_write_items(TransactItems=items)
        return AttendanceStatus.REGISTERED
