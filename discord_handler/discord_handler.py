from filmbot import FilmBot, VotingStatus, AttendanceStatus, Film
from UserError import UserError
from datetime import datetime
from itertools import islice
from uuid import uuid1
from imdb import IMDb

MAX_MESSAGE_SIZE = 2000

PING = 1
APPLICATION_COMMAND = 2
MESSAGE_COMPONENT = 3
APPLICATION_COMMAND_AUTOCOMPLETE = 4

PONG = 1
CHANNEL_MESSAGE_WITH_SOURCE = 4
DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE = 5
DEFERRED_UPDATE_MESSAGE = 6
UPDATE_MESSAGE = 7
APPLICATION_COMMAND_AUTOCOMPLETE_RESULT = 8

EPHEMERAL_FLAG = 64

ACTION_ROW = 1
BUTTON = 2
SELECT_MENU = 3

PRIMARY_STYLE = 1
SECONDARY_STYLE = 2
SUCCESS_STYLE = 3
DANGER_STYLE = 4
LINK_STYLE = 5

MSG_COMPONENT_ATTENDANCE_ID = "register_attendance"
MSG_COMPONENT_SHAME_ID = "shame"


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
    return f"  {position}. <@{n.DiscordUserID}> {n.FilmName} ({vote_count} vote{s}){imdb}"


def display_user(user):
    return f"  - <@{user.DiscordUserID}>"


def display_watched(f: Film):
    # Surround links with <> to avoid Discord previewing the links
    imdb = (
        f" [IMDB](<https://imdb.com/title/tt{f.IMDbID}>)"
        if f.IMDbID is not None
        else ""
    )
    return f"  • {f.DateWatched.strftime('%Y-%m-%d')} {f.FilmName}{imdb} - <@{f.DiscordUserID}>"


def naughty_message(filmbot):
    users = filmbot.get_users().values()
    message = []
    toNominate = set(filter(lambda u: u.NominatedFilmID is None, users))
    toVote = set(filter(lambda u: u.VoteID is None, users))
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
        "content": "\n".join(message),
        "any_tasks": toNominate or toVote,
    }


def register_attendance(*, FilmBot, DiscordUserID, DateTime):
    status = FilmBot.record_attendance_vote(
        DiscordUserID=DiscordUserID, DateTime=DateTime
    )
    response = {
        "type": CHANNEL_MESSAGE_WITH_SOURCE,
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
        response["data"]["flags"] = EPHEMERAL_FLAG
    return response


def handle_application_command(event, client):
    """
    Handle the 6 application commands that we support:
      * /nominate [FilmName]
      * /vote [FilmID]
      * /peek
      * /watch [FilmID]
      * /here
      * /naughty
    """
    now = datetime.utcnow()
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
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
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
                "type": CHANNEL_MESSAGE_WITH_SOURCE,
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
                "type": CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": f"<@{user_id}> has voted for {film_name}",
                },
            }

    elif command == "peek":
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    "The current list of nominations are:\n"
                    + "\n".join(
                        map(
                            display_nomination,
                            enumerate(filmbot.get_nominations()),
                        )
                    )
                ),
                "flags": EPHEMERAL_FLAG,
            },
        }

    elif command == "watch":
        film_id = body["data"]["options"][0]["value"]
        film = filmbot.start_watching_film(
            FilmID=film_id, DateTime=now, PresentUserIDs=[user_id]
        )
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    f"Started watching {film.FilmName}!\n\n"
                    + f"Everyone other than <@{user_id}> should record their attendance below or using `/here`.\n\n"
                    + f"<@{film.DiscordUserID}> can now nominated their next suggestion with `/nominate`.\n"
                ),
                "components": [
                    {
                        "type": ACTION_ROW,
                        "components": [
                            {
                                "type": BUTTON,
                                "label": "Register Attendance",
                                "style": PRIMARY_STYLE,
                                "custom_id": MSG_COMPONENT_ATTENDANCE_ID,
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
    elif command == "naughty":
        naughty = naughty_message(filmbot)
        result = {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": naughty["content"],
                "flags": EPHEMERAL_FLAG,
            },
        }

        # Display a "shame" button only if there are tasks
        if naughty["any_tasks"]:
            result["data"]["components"] = [
                {
                    "type": ACTION_ROW,
                    "components": [
                        {
                            "type": BUTTON,
                            "label": "Publically Shame",
                            "style": DANGER_STYLE,
                            "custom_id": MSG_COMPONENT_SHAME_ID,
                        }
                    ],
                }
            ]
        return result
    elif command == "history":
        message = "Here are the films that have been watched:\n"
        films = filmbot.get_watched_films()
        for film in films:
            line = display_watched(film) + "\n"
            if len(message) + len(line) > MAX_MESSAGE_SIZE:
                break

            message += line

        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": message,
                "flags": EPHEMERAL_FLAG,
            },
        }
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
            "type": APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
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
            "type": APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
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
            "type": APPLICATION_COMMAND_AUTOCOMPLETE_RESULT,
            "data": {
                "choices": films_to_choices(nominations),
            },
        }
    else:
        raise Exception(f"Autocomplete not supported for /{command}")


def handle_message_component(event, client):
    body = event["body-json"]
    now = datetime.utcnow()
    component_type = body["data"]["component_type"]
    if component_type != BUTTON:
        raise Exception(f"Unknown message component ({component_type})!")

    custom_id = body["data"]["custom_id"]
    if custom_id == MSG_COMPONENT_ATTENDANCE_ID:
        filmbot = FilmBot(DynamoDBClient=client, GuildID=body["guild_id"])
        user_id = body["member"]["user"]["id"]
        return register_attendance(
            FilmBot=filmbot, DiscordUserID=user_id, DateTime=now
        )
    elif custom_id == MSG_COMPONENT_SHAME_ID:
        filmbot = FilmBot(DynamoDBClient=client, GuildID=body["guild_id"])
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {"content": naughty_message(filmbot)["content"]},
        }
    else:
        raise Exception(
            f"Unknown 'custom_id' for button component ({custom_id})!"
        )


def handle_discord(event, client):
    body = event["body-json"]
    type = body["type"]
    if type == PING:
        return {"type": PONG}
    elif type == APPLICATION_COMMAND:
        try:
            return handle_application_command(event, client)
        except UserError as e:
            # If we get a `UserError` it's something we can display to the user
            return {
                "type": CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": str(e),
                    "flags": EPHEMERAL_FLAG,
                },
            }
    elif type == MESSAGE_COMPONENT:
        try:
            return handle_message_component(event, client)
        except UserError as e:
            # If we get a `UserError` it's something we can display to the user
            return {
                "type": CHANNEL_MESSAGE_WITH_SOURCE,
                "data": {
                    "content": str(e),
                    "flags": EPHEMERAL_FLAG,
                },
            }
    elif type == APPLICATION_COMMAND_AUTOCOMPLETE:
        return handle_autocomplete(event, client)
    else:
        raise Exception(f"Unknown type ({type})!")
