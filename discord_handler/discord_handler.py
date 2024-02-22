from filmbot import FilmBot, VotingStatus, AttendanceStatus, Film
from UserError import UserError
import datetime as dt
from itertools import islice
from uuid import uuid1
from imdb import IMDb

MAX_MESSAGE_SIZE = 2000

# To avoid querying for more data than we can show in a single message for `/history`,
# we limit the query size.  This gives us 2000/80 = 25 bytes per row to show and it's
# unlikely that we have 80 films with small enough titles to fit in 2KB.  In those
# rare occassions it doesn't really matter as we display a button to display the
# next set.  Realistically this should be small enough to fill 2KB but less a multiple
# of the DynamoDB block size (4KB) in order to minimize cost.
HISTORY_LIMIT = 80


class DiscordRequest:
    PING = 1
    APPLICATION_COMMAND = 2
    MESSAGE_COMPONENT = 3
    APPLICATION_COMMAND_AUTOCOMPLETE = 4


class DiscordResponse:
    PONG = 1
    CHANNEL_MESSAGE_WITH_SOURCE = 4
    DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE = 5
    DEFERRED_UPDATE_MESSAGE = 6
    UPDATE_MESSAGE = 7
    APPLICATION_COMMAND_AUTOCOMPLETE_RESULT = 8


class DiscordFlag:
    EPHEMERAL_FLAG = 64


class DiscordMessageComponent:
    ACTION_ROW = 1
    BUTTON = 2
    SELECT_MENU = 3


class DiscordStyle:
    PRIMARY = 1
    SECONDARY = 2
    SUCCESS = 3
    DANGER = 4
    LINK = 5


class MessageComponentID:
    ATTENDANCE = "register_attendance"
    SHAME = "shame"
    MORE_HISTORY = "more_history#"


def films_to_choices(films):
    return list(
        map(
            lambda n: {
                "name": n.FilmName,
                "value": n.FilmID,
            },
            films,
        )
    )


def encode_IMDB(imdb_id, film_name):
    return f"IMDB:{imdb_id}:{film_name}"


def decode_film(film_name_or_id):
    if film_name_or_id.startswith("IMDB:"):
        parts = film_name_or_id.split(":", 3)
        assert parts[0] == "IMDB"
        imdb_id = parts[1]
        film_name = parts[2]
        return film_name, imdb_id
    else:
        return film_name_or_id, None


def display_nomination(nomination):
    position = nomination[0] + 1
    n = nomination[1]
    vote_count = n.CastVotes + n.AttendanceVotes
    s = "" if vote_count == 1 else "s"
    # Surround links with <> to avoid Discord previewing the links
    imdb = (
        f" [IMDB](<https://imdb.com/title/tt{n.IMDbID}>)"
        if n.IMDbID is not None
        else ""
    )
    return f"{position}. <@{n.DiscordUserID}> {n.FilmName} ({vote_count} vote{s}){imdb}"


def display_users_by_nomination(users):
    position = users[0] + 1
    userAndFilm = users[1]
    discordUserID = userAndFilm["User"].DiscordUserID
    if userAndFilm["Film"] is not None:
        film = userAndFilm["Film"]
        vote_count = film.CastVotes + film.AttendanceVotes
        # Surround links with <> to avoid Discord previewing the links
        film = (
            f"[{film.FilmName}](<https://imdb.com/title/tt{film.IMDbID}>)"
            if film.IMDbID is not None
            else film.FilmName
        )
        return f"{position}. {film} ({vote_count} 🗳) <@{discordUserID}>"
    else:
        return f"{position}. [No nomination] <@{discordUserID}>"


def display_watched(f: Film):
    return f"- <t:{int(f.DateWatched.timestamp())}:d> {f.FilmName} - <@{f.DiscordUserID}>"


def get_history(filmbot: FilmBot, nextKey=None, MessagePrefix=""):
    (films, nextKey) = filmbot.get_watched_films_after(
        Limit=HISTORY_LIMIT, ExclusiveStartKey=nextKey
    )
    if films:
        message = MessagePrefix

        lastFilmKey = None
        for film in films:
            line = display_watched(film) + "\n"
            if len(message) + len(line) > MAX_MESSAGE_SIZE:
                nextKey = lastFilmKey
                break

            lastFilmKey = film.SK
            message += line
    else:
        message = "No films have yet been watched."

    result = {
        "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
        "data": {
            "content": message,
            "flags": DiscordFlag.EPHEMERAL_FLAG,
        },
    }
    if nextKey:
        result["data"]["components"] = [
            {
                "type": DiscordMessageComponent.ACTION_ROW,
                "components": [
                    {
                        "type": DiscordMessageComponent.BUTTON,
                        "label": "More History",
                        "style": DiscordStyle.PRIMARY,
                        "custom_id": MessageComponentID.MORE_HISTORY + nextKey,
                    }
                ],
            }
        ]
    return result


def register_attendance(*, FilmBot, DiscordUserID, DateTime):
    status = FilmBot.record_attendance_vote(
        DiscordUserID=DiscordUserID, DateTime=DateTime
    )
    response = {
        "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
        "data": {
            "content": (
                f"<@{DiscordUserID}> has attended"
                if status == AttendanceStatus.REGISTERED
                else f"Your attendance has already been recorded"
            )
        },
    }
    # Don't allow users to flood the chat with `/here` commands
    if status == AttendanceStatus.ALREADY_REGISTERED:
        response["data"]["flags"] = DiscordFlag.EPHEMERAL_FLAG
    return response


def handle_application_command(event, client):
    """
    Handle the 6 application commands that we support:
      * /nominate [FilmName]
      * /vote [FilmID]
      * /peek
      * /watch [FilmID]
      * /here
    """
    now = dt.datetime.now(dt.timezone.utc)
    body = event["body-json"]
    command = body["data"]["name"]
    guild_id = body["guild_id"]
    filmbot = FilmBot(DynamoDBClient=client, GuildID=guild_id)
    user_id = body["member"]["user"]["id"]
    if command == "nominate":
        film_name_or_imdb = body["data"]["options"][0]["value"]
        film_name, imdb_id = decode_film(film_name_or_imdb)

        film_id = str(uuid1())
        filmbot.nominate_film(
            DiscordUserID=user_id,
            FilmName=film_name,
            IMDbID=imdb_id,
            NewFilmID=film_id,
            DateTime=now,
        )
        return {
            "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    f"<@{user_id}> has successfully nominated {film_name}.\n\n"
                    + "The current list of nominations are:\n"
                    + "\n".join(
                        map(
                            display_nomination,
                            enumerate(filmbot.get_nominations()),
                        )
                    )
                )
            },
        }
    elif command == "vote":
        film_id = body["data"]["options"][0]["value"]
        status = filmbot.cast_preference_vote(
            DiscordUserID=user_id, FilmID=film_id
        )
        film_name = filmbot.get_nominated_film(film_id).FilmName
        if status == VotingStatus.COMPLETE:
            return {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": (
                        f"<@{user_id}> has voted for {film_name}.\n\n"
                        "This was the final vote and the standings are:\n"
                        + "\n".join(
                            map(
                                display_nomination,
                                enumerate(filmbot.get_nominations()),
                            )
                        )
                    )
                },
            }
        else:
            return {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": f"<@{user_id}> has voted for {film_name}",
                },
            }

    elif command == "peek":
        users = filmbot.get_users_by_nomination()
        content = ""
        components = None
        if not users:
            content += "There are no current nominations. Each user can nominate with the `/nominate` command."
        else:
            content += "The current list of nominations are:\n" + "\n".join(
                map(
                    display_users_by_nomination,
                    enumerate(users),
                )
            )

            toNominate = list(
                filter(lambda u: u["User"].NominatedFilmID is None, users)
            )
            toVote = list(filter(lambda u: u["User"].VoteID is None, users))

            if toVote:
                content += "\n\nand these users need to vote:\n"

                def print_user(u):
                    return "- <@" + u["User"].DiscordUserID + ">"

                content += "\n".join(map(print_user, toVote))
            else:
                content += "All users have voted."

            if toNominate or toVote:
                components = [
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
                ]

        result = {
            "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": content,
                "flags": DiscordFlag.EPHEMERAL_FLAG,
            },
        }
        if components:
            result["data"]["components"] = components
        return result

    elif command == "watch":
        film_id = body["data"]["options"][0]["value"]
        film = filmbot.start_watching_film(
            FilmID=film_id, DateTime=now, PresentUserIDs=[user_id]
        )
        return {
            "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    f"Started watching {film.FilmName}!\n\n"
                    + f"Everyone other than <@{user_id}> should record their attendance below or using `/here`.\n\n"
                    + f"<@{film.DiscordUserID}> can now nominated their next suggestion with `/nominate`.\n"
                ),
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
                    }
                ],
            },
        }
    elif command == "here":
        return register_attendance(
            FilmBot=filmbot, DiscordUserID=user_id, DateTime=now
        )
    elif command == "history":
        return get_history(
            filmbot,
            MessagePrefix="Here are the films that have been watched:\n",
        )
    else:
        raise Exception(f"Unknown application command (/{command})")


def handle_autocomplete(event, client):
    """
    Handle the autocomplete for 3 of the application commands that we support:
      * /nominate [FilmName]
      * /vote [FilmID]
      * /watch [FilmID]
    """
    body = event["body-json"]
    command = body["data"]["name"]
    guild_id = body["guild_id"]
    if command == "nominate":
        ia = IMDb()

        partial_film_name = body["data"]["options"][0]["value"]

        # Get 2x the number of results we expect as `search_movie` also
        # finds TV shows etc. and we will trim it down to `MAX_RESULTS`
        # afterwards.
        MAX_RESULTS = 5
        results = ia.search_movie(partial_film_name, results=MAX_RESULTS * 2)
        return {
            "type": DiscordResponse.APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
            "data": {
                "choices": list(
                    map(
                        lambda r: {
                            "name": f"{r['title']} ({r['year']})",
                            "value": encode_IMDB(
                                r.movieID, f"{r['title']} ({r['year']})"
                            ),
                        },
                        islice(
                            filter(lambda r: r["kind"] == "movie", results),
                            MAX_RESULTS,
                        ),
                    )
                )
            },
        }

    elif command == "vote":
        user_id = body["member"]["user"]["id"]
        filmbot = FilmBot(DynamoDBClient=client, GuildID=guild_id)
        nominations = filmbot.get_nominations()

        # Reorder to have the oldest film show up first and filter out
        # our nomination as we can't vote for it.
        final_nominations = sorted(
            filter(lambda f: f.DiscordUserID != user_id, nominations),
            key=lambda f: f.DateNominated,
            reverse=True,
        )
        return {
            "type": DiscordResponse.APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
            "data": {
                "choices": films_to_choices(final_nominations),
            },
        }
    elif command == "watch":
        filmbot = FilmBot(DynamoDBClient=client, GuildID=guild_id)

        # Keep the films ordered with the highest nominated film at the top
        # as this is most likely the one we are going to watch
        nominations = filmbot.get_nominations()
        return {
            "type": DiscordResponse.APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
            "data": {
                "choices": films_to_choices(nominations),
            },
        }
    else:
        raise Exception(f"Autocomplete not supported for /{command}")


def handle_message_component(event, client):
    body = event["body-json"]
    now = dt.datetime.now(dt.timezone.utc)
    component_type = body["data"]["component_type"]
    if component_type != DiscordMessageComponent.BUTTON:
        raise Exception(f"Unknown message component ({component_type})!")

    custom_id = body["data"]["custom_id"]
    if custom_id == MessageComponentID.ATTENDANCE:
        filmbot = FilmBot(DynamoDBClient=client, GuildID=body["guild_id"])
        user_id = body["member"]["user"]["id"]
        return register_attendance(
            FilmBot=filmbot, DiscordUserID=user_id, DateTime=now
        )
    elif custom_id == MessageComponentID.SHAME:
        filmbot = FilmBot(DynamoDBClient=client, GuildID=body["guild_id"])
        users = filmbot.get_users().values()
        message = []
        toNominate = list(filter(lambda u: u.NominatedFilmID is None, users))
        toVote = list(filter(lambda u: u.VoteID is None, users))

        def display_user(user):
            return f"- <@{user.DiscordUserID}>"

        if toNominate:
            message.append("These users need to nominate:")
            message += map(display_user, toNominate)
            if toVote:
                # Add a separating newline
                message.append("")

        if toVote:
            message.append("These users need to vote:")
            message += map(display_user, toVote)
        elif not toNominate:
            message = ["There are no outstanding tasks."]

        return {
            "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {"content": "\n".join(message)},
        }
    elif custom_id.startswith(MessageComponentID.MORE_HISTORY):
        filmbot = FilmBot(DynamoDBClient=client, GuildID=body["guild_id"])
        return get_history(
            filmbot,
            nextKey=custom_id.removeprefix(MessageComponentID.MORE_HISTORY),
        )
    else:
        raise Exception(
            f"Unknown 'custom_id' for button component ({custom_id})!"
        )


def handle_discord(event, client):
    body = event["body-json"]
    type = body["type"]
    if type == DiscordRequest.PING:
        return {"type": DiscordResponse.PONG}
    elif type == DiscordRequest.APPLICATION_COMMAND:
        try:
            return handle_application_command(event, client)
        except UserError as e:
            # If we get a `UserError` it's something we can display to the user
            return {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": str(e),
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            }
    elif type == DiscordRequest.MESSAGE_COMPONENT:
        try:
            return handle_message_component(event, client)
        except UserError as e:
            # If we get a `UserError` it's something we can display to the user
            return {
                "type": DiscordResponse.CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": str(e),
                    "flags": DiscordFlag.EPHEMERAL_FLAG,
                },
            }
    elif type == DiscordRequest.APPLICATION_COMMAND_AUTOCOMPLETE:
        return handle_autocomplete(event, client)
    else:
        raise Exception(f"Unknown type ({type})!")
