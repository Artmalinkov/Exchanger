import pyodbc

def get_cursor(db_name):
    '''
    Возвращает объект курсора для дальнейшего взаимодействия с базой
    :param db_name: имя конкретной базы данных
    :return:
    '''
    conn_str = (
        f"DRIVER={config['database_for_analytics']['DRIVER']};"  # Драйвер MySQL  
        f"SERVER={config['database_for_analytics']['SERVER']};"  # Имя сервера  
        f"DATABASE={db_name};"  # Имя базы данных  
        f"UID={config['database_for_analytics']['UID']};"  # Имя пользователя  
        f"PWD={config['database_for_analytics']['PWD']};"  # Пароль
    )

    pyodbc.connect(conn_str)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return cursor

