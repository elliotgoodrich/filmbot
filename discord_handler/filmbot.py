from pprint import pprint
from enum import Enum
from UserError import UserError
from datetime import timedelta, datetime

TABLE_NAME = "FilmBotTable"

USER_PK = "PK"
USER_SK = "SK"
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
        VoteID,
        AttendanceVoteID,
        FilmID=None,
        FilmName=None,
        IMDbID=None,
        CastVotes=None,
        AttendanceVotes=None,
        DateNominated=None,
    ):
        self.DiscordUserID = DiscordUserID
        self.VoteID = VoteID
        self.AttendanceVoteID = AttendanceVoteID
        self.FilmID = FilmID
        self.FilmName = FilmName
        self.IMDbID = IMDbID
        self.CastVotes = CastVotes
        self.AttendanceVotes = AttendanceVotes
        self.DateNominated = DateNominated

    @property
    def has_nomination(self):
        return self.FilmName is not None

    @property
    def SK(self):
        return f"DISCORDUSER#{self.DiscordUserID}"

    def __eq__(self, other):
        return (
            self.DiscordUserID == other.DiscordUserID
            and self.VoteID == other.VoteID
            and self.AttendanceVoteID == other.AttendanceVoteID
            and self.FilmID == other.FilmID
            and self.FilmName == other.FilmName
            and self.IMDbID == other.IMDbID
            and self.CastVotes == other.CastVotes
            and self.AttendanceVotes == other.AttendanceVotes
            and self.DateNominated == other.DateNominated
        )

    def __hash__(self):
        return hash(self.DiscordUserID)

    def toDict(self, *, GuildID):
        d = {
            "PK": {"S": GuildID},
            "SK": {"S": self.SK},
            "VoteID": keyed(self.VoteID),
            "AttendanceVoteID": keyed(self.AttendanceVoteID),
        }
        if self.has_nomination:
            d["FilmID"] = {"S": self.FilmID}
            d["FilmName"] = {"S": self.FilmName}
            d["IMDbID"] = keyed(self.IMDbID)
            d["CastVotes"] = {"N": str(self.CastVotes)}
            d["AttendanceVotes"] = {"N": str(self.AttendanceVotes)}
            d["DateNominated"] = {"S": datetime.isoformat(self.DateNominated)}
        return d

    @staticmethod
    def fromDict(item):
        film_name = unkeyed(item["FilmName"]) if "FilmName" in item else None
        return User(
            DiscordUserID=item[USER_SK]["S"].split("#")[-1],
            VoteID=unkeyed(item[USER_VoteID]),
            AttendanceVoteID=unkeyed(item[USER_AttendanceVoteID]),
            FilmID=unkeyed(item["FilmID"]) if "FilmID" in item else None,
            FilmName=film_name,
            IMDbID=unkeyed(item["IMDbID"]) if "IMDbID" in item else None,
            CastVotes=(
                unkeyed(item["CastVotes"]) if "CastVotes" in item else None
            ),
            AttendanceVotes=(
                unkeyed(item["AttendanceVotes"])
                if "AttendanceVotes" in item
                else None
            ),
            DateNominated=(
                datetime.fromisoformat(unkeyed(item["DateNominated"]))
                if "DateNominated" in item
                else None
            ),
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

    @staticmethod
    def sortKey(film):
        # Sort by:
        #   - the highest number of votes
        #   - if that is the same, then tie break by highest cast votes
        #   - if that is the same, then tie break by earliest nominated
        #   - if by some miracle the nominations have the same timestamp, use the user's discord ID
        return (
            -film.CastVotes - film.AttendanceVotes,
            -film.CastVotes,
            film.DateNominated,
            film.DiscordUserID,
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


def _user_to_film(user):
    """Construct a nominated Film object from a User record."""
    return Film(
        FilmID=user.FilmID,
        FilmName=user.FilmName,
        IMDbID=user.IMDbID,
        DiscordUserID=user.DiscordUserID,
        CastVotes=user.CastVotes,
        AttendanceVotes=user.AttendanceVotes,
        UsersAttended=None,
        DateNominated=user.DateNominated,
        DateWatched=None,
    )


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
                        ":UserPrefix": {"S": "DISCORDUSER#"},
                    },
                    "KeyConditionExpression": (
                        f"{USER_PK} = :GuildID AND "
                        f"begins_with({USER_SK}, :UserPrefix)"
                    ),
                }
            ),
        )

        return {user.DiscordUserID: user for user in users}

    def get_nominations(self):
        """Return an array of currently nominated films in the order that they should
        be watched based on their vote tally."""

        users = self.get_users()
        nominations = [
            _user_to_film(user)
            for user in users.values()
            if user.has_nomination
        ]
        return sorted(nominations, key=Film.sortKey)

    def get_users_by_nomination(self):
        """Return an array of users with details of their (optionally) nominated films.
        This array is in the order that they should be watched based on their vote tally.
        If a user has not nominated then they will be put at the end of the array.
        """

        users = self.get_users()
        result = [
            {
                "User": user,
                "Film": _user_to_film(user) if user.has_nomination else None,
            }
            for user in users.values()
        ]

        return sorted(
            result,
            key=lambda u: (
                (0, Film.sortKey(u["Film"]))
                if u["Film"] is not None
                else (1, u["User"].DiscordUserID)
            ),
        )

    def get_watched_films(self):
        """
        Return an array of watched films ordered by most recently watched.
        """
        return list(
            map(
                Film.fromDict,
                self.__query(
                    {
                        "TableName": TABLE_NAME,
                        "ExpressionAttributeValues": {
                            ":GuildID": {"S": self.guildID},
                            ":FilmPrefix": {"S": "FILM#WATCHED#"},
                        },
                        "KeyConditionExpression": (
                            f"{FILM_PK} = :GuildID AND "
                            f"begins_with({FILM_SK}, :FilmPrefix)"
                        ),
                        "ScanIndexForward": False,
                    }
                ),
            )
        )

    def get_watched_films_after(self, Limit, ExclusiveStartKey=None):
        """
        Return an tuple where the first element is an array of maximum `Limit` items of
        watched films ordered by most recently watched, and the second element is a
        string representing the `ExclusiveStartKey` parameter to pass into the next
        call to get the next batch of filmes.  If there are no more films then the
        second parameter is `None`.
        """
        query = {
            "TableName": TABLE_NAME,
            "ExpressionAttributeValues": {
                ":GuildID": {"S": self.guildID},
                ":FilmPrefix": {"S": "FILM#WATCHED#"},
            },
            "KeyConditionExpression": (
                f"{FILM_PK} = :GuildID AND "
                f"begins_with({FILM_SK}, :FilmPrefix)"
            ),
            "ScanIndexForward": False,
            "Limit": Limit,
        }
        if ExclusiveStartKey:
            query["ExclusiveStartKey"] = {
                "PK": {"S": self.guildID},
                "SK": {"S": ExclusiveStartKey},
            }

        response = self.client.query(**query)

        # Simplify the `LastEvaluateKey` to just the sort key value
        LastEvaluateKey = response.get("LastEvaluatedKey", None)
        if LastEvaluateKey:
            LastEvaluateKey = LastEvaluateKey[FILM_SK]["S"]
        return (list(map(Film.fromDict, response["Items"])), LastEvaluateKey)

    def get_all_films(self):
        """
        Return an array watched and unwatched films in the order that they were
        nominated.
        """
        users = self.get_users()
        films = [
            _user_to_film(user)
            for user in users.values()
            if user.has_nomination
        ]
        films += self.get_watched_films()
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

        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Update": {
                            "TableName": TABLE_NAME,
                            "Key": {
                                USER_PK: {"S": self.guildID},
                                USER_SK: {"S": f"DISCORDUSER#{DiscordUserID}"},
                            },
                            "ExpressionAttributeValues": {
                                ":NewFilmID": {"S": NewFilmID},
                                ":FilmName": {"S": FilmName},
                                ":IMDbID": keyed(IMDbID),
                                ":Zero": {"N": "0"},
                                ":DateNominated": {
                                    "S": datetime.isoformat(DateTime)
                                },
                                ":Null": {"NULL": True},
                            },
                            "ConditionExpression": "attribute_not_exists(FilmName)",
                            # Make sure to null out VoteID and AttendanceVoteID in case we
                            # didn't have a user yet. These should both be Null at this point
                            # as we can only nominate after we watch a film and these are cleared.
                            "UpdateExpression": (
                                "SET FilmID = :NewFilmID, "
                                "FilmName = :FilmName, "
                                "IMDbID = :IMDbID, "
                                "CastVotes = :Zero, "
                                "AttendanceVotes = :Zero, "
                                "DateNominated = :DateNominated, "
                                f"{USER_VoteID} = :Null, "
                                f"{USER_AttendanceVoteID} = :Null"
                            ),
                        }
                    },
                ]
            )
        except self.client.exceptions.TransactionCanceledException as e:
            raise UserError(
                "Unable to nominate a film as you have already nominated one"
            )

    def cast_preference_vote(self, *, DiscordUserID, NominatorUserID):
        """
        Attempt to cast a vote for the film nominated by `NominatorUserID` on behalf
        of `DiscordUserID` and return a tuple of (VotingStatus, Film).  Throw an
        exception if `DiscordUserID` is not a registered user, `NominatorUserID`
        refers to that user themselves, or `NominatorUserID` doesn't have an active
        nomination.
        """

        users = self.get_users()
        if DiscordUserID not in users:
            raise UserError("You can't vote until you have nominated a film")

        our_user = users[DiscordUserID]
        previous_vote = (
            our_user.VoteID
        )  # NominatorUserID of previously voted film

        # Disallow voting for your own nomination
        if NominatorUserID == DiscordUserID:
            raise UserError("You can't vote for your own film")

        # Record if this is the last user to vote
        user_list = users.values()
        user_voted_count = sum(user.VoteID is not None for user in user_list)
        our_user_hasnt_voted = our_user.VoteID is None

        # Do nothing if user votes for the same thing
        if previous_vote == NominatorUserID:
            return (
                VotingStatus.COMPLETE
                if user_voted_count == len(user_list)
                else VotingStatus.UNCOMPLETE
            ), _user_to_film(users[NominatorUserID])

        items = [
            # Change vote-id and make sure it matches the one we previously read
            # i.e. there haven't been any changes between our read and this write
            {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        USER_PK: {"S": self.guildID},
                        USER_SK: {"S": f"DISCORDUSER#{DiscordUserID}"},
                    },
                    "ExpressionAttributeValues": {
                        ":NewVoteID": {"S": NominatorUserID},
                        ":PreviousVoteID": keyed(previous_vote),
                    },
                    "ConditionExpression": (
                        f"attribute_exists({USER_SK}) AND "
                        f"{USER_VoteID} = :PreviousVoteID"
                    ),
                    "UpdateExpression": f"SET {USER_VoteID} = :NewVoteID",
                }
            },
            # Increment vote count on the nominator's user record (also check they have a nomination)
            {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        USER_PK: {"S": self.guildID},
                        USER_SK: {"S": f"DISCORDUSER#{NominatorUserID}"},
                    },
                    "ExpressionAttributeValues": {
                        ":One": {"N": "1"},
                    },
                    "ConditionExpression": "attribute_exists(FilmName)",
                    "UpdateExpression": "SET CastVotes = CastVotes + :One",
                }
            },
        ]

        if previous_vote is not None:
            # Decrement vote count on previous nominator's record
            items.append(
                {
                    "Update": {
                        "TableName": TABLE_NAME,
                        "Key": {
                            USER_PK: {"S": self.guildID},
                            USER_SK: {"S": f"DISCORDUSER#{previous_vote}"},
                        },
                        "ExpressionAttributeValues": {
                            ":One": {"N": "1"},
                        },
                        "ConditionExpression": "attribute_exists(FilmName)",
                        "UpdateExpression": "SET CastVotes = CastVotes - :One",
                    }
                }
            )

        try:
            self.client.transact_write_items(TransactItems=items)
        except self.client.exceptions.TransactionCanceledException as e:
            reasons = e.response.get("CancellationReasons", [])
            failed = {
                i
                for i, r in enumerate(reasons)
                if r.get("Code") == "ConditionalCheckFailed"
            }
            if 0 in failed:
                raise UserError(
                    "Your vote could not be recorded due to a conflict, please try again"
                )
            if 1 in failed:
                raise UserError(
                    f"There is no film nominated by <@{NominatorUserID}>"
                )
            if 2 in failed:
                raise UserError(
                    "The film you were changing your vote away from has just been watched - please vote again"
                )
            raise UserError(
                "Unknown error occurred while recording your vote, please try again"
            )

        return (
            VotingStatus.COMPLETE
            if user_voted_count + int(our_user_hasnt_voted) == len(user_list)
            else VotingStatus.UNCOMPLETE
        ), _user_to_film(users[NominatorUserID])

    def start_watching_film(
        self, *, NominatorUserID, PresentUserIDs, DateTime
    ):
        """
        Attempt to record that we're watching the film nominated by `NominatorUserID`
        and record an attendance vote for each user in the `PresentUserIDs` array
        and return the a `Film` object.  Also clear out all cast votes from all users
        and clear the nomination from the user who nominated the film.  Throw an
        exception if `NominatorUserID` doesn't have an active nomination, less than
        24 hours has passed since watching the last film, or `PresentUserIDs` is empty.
        """

        # At least one user must be present to start watching a film
        assert PresentUserIDs

        response = self.client.get_item(
            TableName=TABLE_NAME,
            Key={
                USER_PK: {"S": self.guildID},
                USER_SK: {"S": f"DISCORDUSER#{NominatorUserID}"},
            },
        )

        if "Item" not in response:
            raise UserError(
                f"There is no film nominated by <@{NominatorUserID}>"
            )

        nominator_user = User.fromDict(response["Item"])
        if not nominator_user.has_nomination:
            raise UserError(
                f"There is no film nominated by <@{NominatorUserID}>"
            )

        # Check to see all user IDs are valid
        all_users = self.get_users()
        for user in PresentUserIDs:
            assert user in all_users

        film = _user_to_film(nominator_user)

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
                {"S": film.FilmID}
                if user_id in PresentUserIDs
                else {"NULL": True}
            )

            user = all_users[user_id]

            set_exprs = [
                f"{USER_VoteID} = :Null",
                f"{USER_AttendanceVoteID} = :AttendanceVote",
            ]
            remove_exprs = []
            add_exprs = []

            if user_id == NominatorUserID:
                # Clear embedded film fields from the nominator's record
                remove_exprs += [
                    "FilmID",
                    "FilmName",
                    "IMDbID",
                    "CastVotes",
                    "AttendanceVotes",
                    "DateNominated",
                ]

            if (
                user_id in PresentUserIDs
                and user_id != NominatorUserID
                and user.has_nomination
            ):
                add_exprs.append("AttendanceVotes :One")

            update_expr = "SET " + ", ".join(set_exprs)
            if remove_exprs:
                update_expr += " REMOVE " + ", ".join(remove_exprs)

            expr_attr_values = {
                ":Null": {"NULL": True},
                ":AttendanceVote": attendance_vote,
            }
            if add_exprs:
                update_expr += " ADD " + ", ".join(add_exprs)
                expr_attr_values[":One"] = {"N": "1"}

            if user.has_nomination:
                condition = "FilmID = :PreviousFilmID"
                expr_attr_values[":PreviousFilmID"] = {"S": user.FilmID}
            else:
                condition = "attribute_not_exists(FilmName)"

            items.append(
                {
                    "Update": {
                        "TableName": TABLE_NAME,
                        "Key": {
                            USER_PK: {"S": self.guildID},
                            USER_SK: {"S": f"DISCORDUSER#{user_id}"},
                        },
                        "ExpressionAttributeValues": expr_attr_values,
                        "ConditionExpression": condition,
                        "UpdateExpression": update_expr,
                    }
                }
            )

        film.DateWatched = DateTime
        film.UsersAttended = set(PresentUserIDs)
        items.append(
            {
                "Put": {
                    "TableName": TABLE_NAME,
                    "Item": film.toDict(GuildID=self.guildID),
                },
            }
        )
        self.client.transact_write_items(TransactItems=items)

        return film

    def _check_retire(self, *, DiscordUserID):
        """
        Returns a ``(reason, nominated_film)`` tuple where:
          - ``reason`` is ``None`` when the user is eligible, or a human-readable
            string (suitable for passing to `UserError`) explaining why they are not.
          - ``nominated_film`` is the ``Film`` record for their current nomination, or ``None`` if they
            have no nomination.
        """
        response = self.client.get_item(
            TableName=TABLE_NAME,
            Key={
                USER_PK: {"S": self.guildID},
                USER_SK: {"S": f"DISCORDUSER#{DiscordUserID}"},
            },
        )
        if "Item" not in response:
            raise UserError(f"<@{DiscordUserID}> is not a registered user")

        user = User.fromDict(response["Item"])

        if user.VoteID is not None:
            return (
                f"<@{DiscordUserID}> cannot be retired as they have an outstanding vote",
                None,
            )

        nominated_film = None
        if user.has_nomination:
            nominated_film = _user_to_film(user)
            all_users = self.get_users()
            for other_user_id, other_user in all_users.items():
                if (
                    other_user_id != DiscordUserID
                    and other_user.VoteID == DiscordUserID
                ):
                    return (
                        f"<@{DiscordUserID}> cannot be retired as other users have voted for their film",
                        None,
                    )

        # Watched film records are immutable once written, so this read is
        # not subject to a race condition.
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
            Limit=5,
        )
        for item in response["Items"]:
            film = Film.fromDict(item)
            if (
                film.UsersAttended is not None
                and DiscordUserID in film.UsersAttended
            ):
                return (
                    f"<@{DiscordUserID}> cannot be retired as they attended one of the last 5 films",
                    None,
                )

        return (None, nominated_film)

    def can_retire(self, *, DiscordUserID):
        """
        Return ``None`` if the specified `DiscordUserID` is eligible to be
        retired, or a human-readable reason string if they are not.  All of
        the following must hold for eligibility:
          - They have not attended any of the last 5 watched films.
          - They have not cast a preference vote.
          - If they have a nomination, no other user has voted for it.
        """
        reason, _ = self._check_retire(DiscordUserID=DiscordUserID)
        return reason

    def retire_user(self, *, DiscordUserID):
        """
        Attempt to retire the specified `DiscordUserID` by removing their
        user record (which also removes their nomination if they have one).
        Throw an exception if the user is not registered or any of the
        following hold:
          - They attended any of the last 5 watched films.
          - They have cast a preference vote.
          - Another user currently has a vote for their nomination.
        """
        reason, nominated_film = self._check_retire(
            DiscordUserID=DiscordUserID
        )
        if reason is not None:
            raise UserError(reason)

        condition = f"{USER_VoteID} = :Null"
        expr_attr_values = {":Null": {"NULL": True}}

        if nominated_film is not None:
            # Guard against any user voting for this film between our read
            # and this commit. Since we have already checked no one voted
            # for this film, the only thing that can change CastVotes is
            # someone voting for it.
            condition += " AND CastVotes = :ReadCastVotes"
            expr_attr_values[":ReadCastVotes"] = {
                "N": str(nominated_film.CastVotes)
            }

        try:
            self.client.transact_write_items(
                TransactItems=[
                    {
                        "Delete": {
                            "TableName": TABLE_NAME,
                            "Key": {
                                USER_PK: {"S": self.guildID},
                                USER_SK: {"S": f"DISCORDUSER#{DiscordUserID}"},
                            },
                            "ConditionExpression": condition,
                            "ExpressionAttributeValues": expr_attr_values,
                        }
                    }
                ]
            )
        except self.client.exceptions.TransactionCanceledException:
            raise UserError(
                f"Unable to retire <@{DiscordUserID}>. Please try again."
            )

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
                USER_SK: {"S": f"DISCORDUSER#{DiscordUserID}"},
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

        # Build the update expression for the user's record, optionally
        # incrementing their nomination's AttendanceVotes in the same write.
        update_expr = f"SET {USER_AttendanceVoteID} = :AttendanceVote"
        expr_attr_values = {
            ":Null": {"NULL": True},
            ":AttendanceVote": {"S": latest_watched_film.FilmID},
        }

        # Add an attendance vote if we weren't the user who nominated the film
        # and we have a nomination.  We must check both as we could either nominate
        # straight after we start watching our film or we could have failed to nominate
        # a new film before a second film has started being watched.
        if (
            latest_watched_film.DiscordUserID != user.DiscordUserID
            and user.has_nomination
        ):
            update_expr += " ADD AttendanceVotes :One"
            expr_attr_values[":One"] = {"N": "1"}

        items = [
            {
                # Record that the user has an attendance vote, and optionally
                # increment their film's attendance votes.
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        USER_PK: {"S": self.guildID},
                        USER_SK: {"S": user.SK},
                    },
                    "ExpressionAttributeValues": expr_attr_values,
                    # Check that we haven't recorded an attendance in the meantime
                    "ConditionExpression": f"{USER_AttendanceVoteID} = :Null",
                    "UpdateExpression": update_expr,
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

        self.client.transact_write_items(TransactItems=items)
        return AttendanceStatus.REGISTERED
