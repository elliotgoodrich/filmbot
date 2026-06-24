[![Deploy](https://github.com/elliotgoodrich/filmbot/actions/workflows/deploy.yml/badge.svg)](https://github.com/elliotgoodrich/filmbot/actions/workflows/deploy.yml)
[![Test](https://github.com/elliotgoodrich/filmbot/actions/workflows/test.yml/badge.svg)](https://github.com/elliotgoodrich/filmbot/actions/workflows/test.yml)

# FilmBot

A Discord bot for running a film club. Members nominate films, vote on what to watch next, and record their attendance - all from within Discord.

## How it works

1. All members who haven't nominated a film can do so with `/nominate`
2. Everyone votes for their favourite nomination with `/vote`
3. A film is watched by the group and recorded using `/watch`
4. Member's attendence is automatically recorded; latecomers can register with `/here`.
5. The cycle repeats

Use `/peek` at any time to see the current standings and who still needs to nominate or vote.

## Nominations

Each member can have one active nomination at a time. Use `/nominate <film>` to nominate — the bot searches [The Movie Database](https://www.themoviedb.org/) as you type and links to the IMDb page where available. Once your film has been watched, you can nominate again immediately.

## Voting

Use `/vote <film>` to cast your preference vote for another member's nomination. You cannot vote for your own film. You can change your vote at any time before watching starts. Once every member has voted, the full rankings are displayed.

### How films are ranked

Films are ranked by their **total votes**, which is the sum of two components:

- **Preference votes** - the cumulative votes cast by members across all rounds for this film
- **Attendance votes** - a member gains one attendance vote on their current nomination each time they attend a film that someone else nominated

Ties are broken first by preference votes alone, then by earliest nomination date.

Preference votes and attendance votes do not reset between rounds. Nominations of members who regularly show up will eventually rise to the top even if no one votes for them.

## Watching a film

Use `/watch <film>` to start watching a film. This does not need to be the top ranked film (perhaps you skip a film due to the nominee's absense).  All members in the same voice channel as the member who types `/watch` will have their attendence recorded. Latecomers can record their attendence by typing `/here` within 4 hours of starting a film.

There is a 24-hour cooldown between films.

## Commands

| Command | Description |
|---------|-------------|
| `/nominate <film>` | Nominate a film (one active nomination per member) |
| `/vote <film>` | Vote for another member's nomination |
| `/peek` | View current nominations, vote counts, and who still needs to act |
| `/watch <film>` | Start watching the selected film |
| `/here` | Register your attendance for the current film |
| `/history` | Browse previously watched films |
| `/retire <user>` | Remove an inactive member who hasn't attended the last 5 films |

## Installation

This bot is not public, but if you would like to have this bot on your Discord server, please open an [issue](https://github.com/elliotgoodrich/filmbot/issues/new) with a way to contact yourself.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
