import yaml
def load_config():
    with open('config_database.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config







