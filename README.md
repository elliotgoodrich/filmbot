# A FilmBot for Discord

Discord Bot to manage our Film Club.

## Scripts

There are 2 separate scripts:
  * [`register_application_commands.py`](register_application_commands/register_application_commands.py) needs to be run any time the [Discord application commands](https://discord.com/developers/docs/interactions/application-commands) changes
  * [`lambda_function.py`](discord_handler/lambda_function.py) is run any time an application command is run

## Table Schema

There is one DynamoDB table needed by FilmBot called "filmbot-table".  It has a partition key 
called "PK" and a sort key called "SK".

The partition key will be Discord Guild ID.

The sort key will take one of the following forms:
  1. `"USER." + DiscordUserID`
  2. `"FILM.NOMINATED." + FilmID`
  3. `"FILM.WATCHED." + DateTimeStarted + "." + FilmID`

Where:
  * `DiscordUserID` is the user's Discord ID (supplied by Discord)
  * `FilmID` is a UUID that we generate per film
  * `DateStarted` is an ISO 8601 formatted string of the UTC datetime that
     film was started being watched

For example:
  1. `"USER.16393729388392"`
  2. `"FILM.NOMINATED.76988c8a-a15d-48a9-8805-5c7f1723e298"`
  3. `"FILM.WATCHED.2022-01-19T21:35:58Z.76988c8a-a15d-48a9-8805-5c7f1723e298"`

### "USER.*" Record Format

The records with sort key starting with `"USER.*"` contains the following
fields:
  * `NominatedFilmID` is a string matching a `"FILM.NOMINATED.*"` sort key that represents this users nominated film, or `NULL` if this user has no currently nominated film
  * `VoteID` is a string matching a `"FILM.NOMINATED.*"` sort key that represents this user's voted film, or `NULL` if this user has not voted yet in this round
  * `AttendanceVoteID` is a string matching a `"FILM.WATCHED.*.*"` sort key that represents this user's attendance vote for the last watched film, or `NULL` if this user did not watch the latest film

### "FILM.*" Record Format

The records with sort key starting with `"FILM.*"` contains the following fields:
  * `FilmName` is a string representation of the film's name
  * `DiscordUserID` is a string matching the users's Discord ID who nominated this film
  * `CastVotes` is a non-negative integer representing the number of votes cast for this film
  * `AttendanceVotes` is a non-negative integer representing the number of attendance votes for the user who nominated this film
  * `UsersAttended` is a set containing the user's Discord IDs of those who have attended
  * `DateNominated` is an ISO 8601 formatting string of the UTC datetime this film was nominated
