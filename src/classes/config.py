import yaml
def load_config():
    '''
    Загрузка файла конфигурации для подключения к БД
    :return:
    '''
    with open('config_database.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config








