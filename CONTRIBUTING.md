# Contributing

## Building and running tests

  1. `cd discord_handler`
  2. `pip install -r dev_requirements.txt`
  3. `pip install -r requirements.txt`
  4. `python test_filmbot.py`

## Formatting

  1. `black *.py`

## Creating AWS Layer

On a Linux x64 machine (e.g. an EC2 Amazon Linux instance)

  1. Within the `filmbot` directory
  2. `mkdir python`
  3. `cd python`
  4. `pip install -r ../discord_handler/requirements.txt -t .`
  5. `pip install pynacl -t .`
  6. `rm -r *dist-info __pycache__`
  7. `cd ..`
  7. `zip -r pynacl.zip python` (the zip contains 1 top level `python` folder)
  8. `rm -rf python`
  9. Repeat steps 3-8 with `pip install IMDbPY -t .` and `imdbpy.zip` instead