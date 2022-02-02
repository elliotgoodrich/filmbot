import boto3
from filmbot import FilmBot, VotingStatus, AttendanceStatus
from UserError import UserError
from datetime import datetime
from uuid import uuid1

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


def make_client(region_name):
    return boto3.client("dynamodb", region_name=region_name)


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


def display_nomination(nomination):
    position = nomination[0] + 1
    n = nomination[1]
    vote_count = n.CastVotes + n.AttendanceVotes
    s = "" if vote_count == 1 else "s"
    return f"  {position}. <@{n.DiscordUserID}> {n.FilmName} ({vote_count} vote{s})"


def handle_application_command(event, region_name):
    """
    Handle the 4 application commands that we support:
      * /nominate [FilmName]
      * /vote [FilmID]
      * /peek
      * /watch [FilmID]
      * /here
    """
    now = datetime.utcnow()
    body = event["body-json"]
    command = body["data"]["name"]
    guild_id = body["guild_id"]
    filmbot = FilmBot(
        DynamoDBClient=make_client(region_name), GuildID=guild_id
    )
    user_id = body["member"]["user"]["id"]
    if command == "nominate":
        film_name = body["data"]["options"][0]["value"]
        film_id = str(uuid1())
        filmbot.nominate_film(
            DiscordUserID=user_id,
            FilmName=film_name,
            NewFilmID=film_id,
            DateTime=now,
        )
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    f"<@{user_id}> has successfully nominated {film_name}. "
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
                    + f"Everyone other than <@{user_id}> should record their attendance using `/here`.\n\n"
                    + f"<@{film.DiscordUserID}> can now nominated their next suggestion with `/nominate`"
                )
            },
        }
    elif command == "here":
        status = filmbot.record_attendance_vote(
            DiscordUserID=user_id, DateTime=now
        )
        response = {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    f"<@{user_id}>'s has attended"
                    if status == AttendanceStatus.REGISTERED
                    else f"Your attendance has already been recorded"
                )
            },
        }

        # Don't allow users to flood the chat with `/here` commands
        if status == AttendanceStatus.ALREADY_REGISTERED:
            response["data"]["flags"] = EPHEMERAL_FLAG
        return response
    else:
        raise Exception(f"Unknown application command (/{command})")


def handle_autocomplete(event, region_name):
    """
    Handle the autocomplete for 2 of the application commands that we support:
      * /vote [FilmID]
      * /watch [FilmID]
    """
    body = event["body-json"]
    command = body["data"]["name"]
    guild_id = body["guild_id"]
    if command == "vote":
        user_id = body["member"]["user"]["id"]
        filmbot = FilmBot(
            DynamoDBClient=make_client(region_name), GuildID=guild_id
        )
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
        filmbot = FilmBot(
            DynamoDBClient=make_client(region_name), GuildID=guild_id
        )

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


def handle_discord(event, region_name):
    body = event["body-json"]
    type = body["type"]
    if type == PING:
        return {"type": PONG}
    elif type == APPLICATION_COMMAND:
        try:
            return handle_application_command(event, region_name)
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
        raise Exception(f"Unknown type ({type})!")
    elif type == APPLICATION_COMMAND_AUTOCOMPLETE:
        return handle_autocomplete(event, region_name)
    else:
        raise Exception(f"Unknown type ({type})!")
