import boto3

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
    def __init__(self, dynamodb):
        self._members_table = dynamodb.Table(MEMBERS_TABLE)
        self._nominations_table = dynamodb.Table(NOMINATIONS_TABLE)
        self._vote_table = dynamodb.Table(VOTES_TABLE)

    @property
    def members_table(self):
        return self._members_table

    @property
    def nominations_table(self):
        return self._nominations_table

    @property
    def votes_table(self):
        return self._votes_table

    def register_user(self, discord_user_id, datetime):
        """Attempt to register the specified `discord_user_id` at the specified `datetime`.
        If `discord_user_id` is already registered, then throw an exception."""
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

    def nominate_film(self, discord_user_id, film_name):
        # TODO
        assert False

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
