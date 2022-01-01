# filmbot for Discord

Discord Bot to manage our Film Club.

## Scripts

There are 2 separate scripts:

  * [`register_application_commands.py`](register_application_commands/register_application_commands.py) needs to be run any time the [Discord application commands](https://discord.com/developers/docs/interactions/application-commands) changes
  * [`lambda_function.py`](discord_handler/lambda_function.py) is run any time an application command is run
