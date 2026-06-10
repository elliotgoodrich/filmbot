#!/usr/bin/env python3
"""
Migration: embeds FILM#NOMINATED data into DISCORDUSER records.

Each DISCORDUSER record with a NominatedFilmID gets the corresponding film's
data (FilmID, FilmName, IMDbID, CastVotes, AttendanceVotes, DateNominated)
embedded directly.  VoteID (previously a bare film ID) is replaced by the
nominator's Discord user ID.  After migrating, FILM#NOMINATED records and
NominatedFilmID fields are removed.

Run with --dry-run to preview changes without modifying the database.
"""

import argparse
import boto3

TABLE_NAME = "FilmBotTable"


def migrate(client, dry_run):
    # Scan the full table (paginating in case of large datasets)
    items = []
    kwargs = {"TableName": TABLE_NAME}
    while True:
        response = client.scan(**kwargs)
        items.extend(response["Items"])
        if "LastEvaluatedKey" not in response:
            break
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    # Group by GuildID (PK), separating user records from nominated film records
    guilds = {}
    for item in items:
        guild_id = item["PK"]["S"]
        if guild_id not in guilds:
            guilds[guild_id] = {"users": {}, "nominated": {}}
        sk = item["SK"]["S"]
        if sk.startswith("DISCORDUSER#"):
            guilds[guild_id]["users"][sk.removeprefix("DISCORDUSER#")] = item
        elif sk.startswith("FILM#NOMINATED#"):
            guilds[guild_id]["nominated"][
                sk.removeprefix("FILM#NOMINATED#")
            ] = item

    for guild_id, data in guilds.items():
        users = data["users"]
        nominated = data["nominated"]

        if not nominated:
            continue

        # Build film_id → nominator_user_id mapping from the nominated film records
        film_to_nominator = {
            film_id: film["DiscordUserID"]["S"]
            for film_id, film in nominated.items()
        }

        transact_items = []

        for user_id, user in users.items():
            set_exprs = []
            remove_exprs = []
            expr_attr_values = {}

            # Embed film data if this user has an active nomination
            nominated_film_id = user.get("NominatedFilmID", {}).get("S")
            if nominated_film_id:
                film = nominated.get(nominated_film_id)
                if film:
                    set_exprs += [
                        "FilmID = :FilmID",
                        "FilmName = :FilmName",
                        "IMDbID = :IMDbID",
                        "CastVotes = :CastVotes",
                        "AttendanceVotes = :AttendanceVotes",
                        "DateNominated = :DateNominated",
                    ]
                    expr_attr_values.update(
                        {
                            ":FilmID": {"S": nominated_film_id},
                            ":FilmName": film["FilmName"],
                            ":IMDbID": film.get("IMDbID", {"NULL": True}),
                            ":CastVotes": film["CastVotes"],
                            ":AttendanceVotes": film["AttendanceVotes"],
                            ":DateNominated": film["DateNominated"],
                        }
                    )
                else:
                    print(
                        f"[{guild_id}] WARNING: user {user_id} references "
                        f"film {nominated_film_id} which has no FILM#NOMINATED# record"
                    )

            # Remove NominatedFilmID (replaced by the embedded fields above)
            if "NominatedFilmID" in user:
                remove_exprs.append("NominatedFilmID")

            # Update VoteID from a bare film ID to the nominator's DiscordUserID
            vote_film_id = user.get("VoteID", {}).get("S")
            if vote_film_id:
                nominator_id = film_to_nominator.get(vote_film_id)
                if nominator_id:
                    set_exprs.append("VoteID = :VoteID")
                    expr_attr_values[":VoteID"] = {"S": nominator_id}
                else:
                    print(
                        f"[{guild_id}] WARNING: user {user_id} voted for film "
                        f"{vote_film_id} which has no nominator — clearing vote"
                    )
                    set_exprs.append("VoteID = :Null")
                    expr_attr_values[":Null"] = {"NULL": True}

            if not set_exprs and not remove_exprs:
                continue

            update_expr = ""
            if set_exprs:
                update_expr += "SET " + ", ".join(set_exprs)
            if remove_exprs:
                update_expr += (
                    (" " if update_expr else "")
                    + "REMOVE "
                    + ", ".join(remove_exprs)
                )

            op = {
                "Update": {
                    "TableName": TABLE_NAME,
                    "Key": {
                        "PK": {"S": guild_id},
                        "SK": {"S": f"DISCORDUSER#{user_id}"},
                    },
                    "UpdateExpression": update_expr,
                }
            }
            if expr_attr_values:
                op["Update"]["ExpressionAttributeValues"] = expr_attr_values
            transact_items.append(op)

        # Delete all FILM#NOMINATED# records
        for film_id in nominated:
            transact_items.append(
                {
                    "Delete": {
                        "TableName": TABLE_NAME,
                        "Key": {
                            "PK": {"S": guild_id},
                            "SK": {"S": f"FILM#NOMINATED#{film_id}"},
                        },
                    }
                }
            )

        if dry_run:
            print(
                f"[{guild_id}] Dry run - {len(transact_items)} operation(s):"
            )
            for op in transact_items:
                kind = next(iter(op))
                key = op[kind]["Key"]["SK"]["S"]
                print(f"  {kind}: {key}")
        else:
            # transact_write_items supports up to 100 items per call; split if needed
            for i in range(0, len(transact_items), 100):
                client.transact_write_items(
                    TransactItems=transact_items[i : i + 100]
                )


def main():
    parser = argparse.ArgumentParser(
        description="Migrate FilmBot DynamoDB table to embed nominations in user records."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing to the database",
    )
    parser.add_argument(
        "--endpoint-url",
        help="DynamoDB endpoint URL (e.g. http://localhost:8000 for local testing)",
    )
    args = parser.parse_args()

    kwargs = {}
    if args.endpoint_url:
        kwargs["endpoint_url"] = args.endpoint_url

    client = boto3.client("dynamodb", **kwargs)
    migrate(client, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
