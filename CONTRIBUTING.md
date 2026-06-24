# Contributing

## Building and running tests

  1. `cd discord_handler`
  2. `pip install -r dev_requirements.txt -r requirements.txt`
  3. `python -m unittest discover -v`

## Formatting

  1. `python -m black .`

## Deploying

All commits to `main` that pass the unit tests will be deployed automatically.

## Architecture

  * [`lambda_function.py`](discord_handler/lambda_function.py) is the AWS Lambda entry point, which calls into
  * [`discord_handler.py`](discord_handler/discord_handler.py), which handles the application commands, autocomplete, and button clicks from Discord. This calls into
  * [`filmbot.py`](discord_handler/filmbot.py), where most of the application logic resides

Additionally on deployment,

  * [`sync_application_commands.py`](sync_application_commands/sync_application_commands.py), deploys the Discord applications commands in [`commands.json`](sync_application_commands/commands.json)

## Table Schema

There is one DynamoDB table needed by FilmBot called "filmbot-table".  It has a partition key
called "PK" and a sort key called "SK".

The partition key will be Discord Guild ID.

The sort key will take one of the following forms:
  1. `"DISCORDUSER#" + DiscordUserID`
  2. `"FILM#WATCHED#" + DateTimeStarted + "#" + FilmID`

Where:
  * `DiscordUserID` is the user's Discord ID (supplied by Discord)
  * `FilmID` is a UUID that we generate per film
  * `DateTimeStarted` is an ISO 8601 formatted string of the UTC datetime that
     film was started being watched

For example:
  1. `"DISCORDUSER#16393729388392"`
  2. `"FILM#WATCHED#2022-01-19T21:35:58.000000#76988c8a-a15d-48a9-8805-5c7f1723e298"`

### "DISCORDUSER#*" Record Format

The records with sort key starting with `"DISCORDUSER#"` contain the following fields:
  * `VoteID` is a string matching another user's `DiscordUserID` that represents the user this person has voted for, or `NULL` if this user has not voted yet in this round
  * `AttendanceVoteID` is a string matching a `FilmID` that represents the last film this user recorded attendance for, or `NULL` if this user has not recorded attendance for the latest watched film

When a user has an active nomination, the following additional fields are present:
  * `FilmID` is a UUID identifying the nominated film (also used as part of the `"FILM#WATCHED#"` sort key when the film is later watched)
  * `FilmName` is a string representation of the film's name
  * `IMDbID` is `NULL` or an IMDb ID (e.g. `"0113375"`)
  * `CastVotes` is a non-negative integer representing the number of preference votes cast for this film
  * `AttendanceVotes` is a non-negative integer representing the number of attendance votes accumulated by this user's nominations over time
  * `DateNominated` is an ISO 8601 formatted string of the UTC datetime this film was nominated

### "FILM#WATCHED#*" Record Format

The records with sort key starting with `"FILM#WATCHED#"` contain the following fields:
  * `FilmName` is a string representation of the film's name
  * `DiscordUserID` is the Discord ID of the user who nominated this film
  * `IMDbID` is `NULL` or an IMDb ID (e.g. `"0113375"`)
  * `CastVotes` is a non-negative integer representing the number of preference votes the film received
  * `AttendanceVotes` is a non-negative integer representing the number of attendance votes for the user who nominated this film
  * `UsersAttended` is `NULL` or a non-empty set of Discord user IDs of those who attended (DynamoDB does not support empty string sets)
  * `DateNominated` is an ISO 8601 formatted string of the UTC datetime this film was nominated