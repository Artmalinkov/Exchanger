import yaml

from classes.config import load_config

from src.classes.config import *

with open('config_database.yaml', 'r') as file:
    config = yaml.safe_load(file)
config

config = load_config()
