import yaml

from classes.config import load_config

from src.classes.config import *
config = load_config()
config

config['database_for_analytics']['DRIVER']


del config

config