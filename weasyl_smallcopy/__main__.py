import json

from . import main


with open("config.json", "r") as f:
	config = json.load(f)

main(config)
