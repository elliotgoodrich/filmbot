import boto3
from filmbot import FilmBot, VotingStatus, AttendanceStatus
from UserError import UserError
from datetime import datetime
from uuid import uuid1
from enum import Enum

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


def make_client(region_name):
    return boto3.client("dynamodb", region_name=region_name)


def films_to_choices(films):
    return list(
        map(
            lambda n: {
                "name": n["FilmName"],
                "value": n["FilmID"],
            },
            films,
        )
    )


def handle_application_command(event, region_name):
    """
    Handle the 4 application commands that we support:
      * /nominate [FilmName]
      * /vote [FilmID]
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
                "content": f"You have successfully nominated {film_name}"
            },
        }
    elif command == "vote":
        film_id = body["data"]["options"][0]["value"]
        filmbot.cast_preference_vote(DiscordUserID=user_id, FilmID=film_id)
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": "Vote successfully recorded",
            },
        }

    elif command == "watch":
        film_id = body["data"]["options"][0]["value"]
        filmbot.start_watching_film(
            FilmID=film_id, DateTime=now, PresentUserIDs=[user_id]
        )
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {"content": "Successfully started watching"},
        }
    elif command == "here":
        status = filmbot.record_attendance_vote(
            DiscordUserID=user_id, DateTime=now
        )
        return {
            "type": CHANNEL_MESSAGE_WITH_SOURCE,
            "data": {
                "content": (
                    "Your attendance has been recorded"
                    if status == AttendanceStatus.REGISTERED
                    else "Your attendance has already been recorded"
                )
            },
        }
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
            filter(lambda f: f["DiscordUserID"] != user_id, nominations),
            key=lambda f: f["DateNominated"],
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
                "data": {"content": f"ERROR! {str(e)}"},
            }
    elif type == MESSAGE_COMPONENT:
        raise Exception(f"Unknown type ({type})!")
    elif type == APPLICATION_COMMAND_AUTOCOMPLETE:
        return handle_autocomplete(event, region_name)
    else:
        raise Exception(f"Unknown type ({type})!")
