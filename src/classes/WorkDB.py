import pyodbc
from src.classes.config import *


class WorkDB():
    '''
    Клас для работы с базой данных
    '''

    def get_cursor(db_name):

        '''
        Подключается к базе, возвращает объект курсора для дальнейшего взаимодействия с базой
        :param db_name: имя конкретной базы данных
        :return: corsor
        '''
        # Загрузка файла конйигурации
        config = load_config()

        conn_str = (
            f"DRIVER={config['database_for_analytics']['DRIVER']};"  # Драйвер MySQL  
            f"SERVER={config['database_for_analytics']['SERVER']};"  # Имя сервера  
            f"DATABASE={db_name};"  # Имя базы данных  
            f"UID={config['database_for_analytics']['UID']};"  # Имя пользователя  
            f"PWD={config['database_for_analytics']['PWD']};"  # Пароль
        )
        try:
            pyodbc.connect(conn_str)
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            return cursor

        except pyodbc.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

    def get_bd_names():
        '''
        Возвращает список имеющихся баз данных
        :return: db_names
        '''
        cursor_for_analytics = get_cursor('for_analytics')
        table_db_names = cursor_for_analytics.execute('SELECT * FROM for_analytics.exchanges;')
        db_names = []
        for db in table_db_names.fetchall():
            db_names.append(db[0])
        return db_names

    cursor_for_analytics = get_cursor('for_analytics')
    cursor_cerber = get_cursor('cerber')
    cursor_lemonchik = get_cursor('lemonchik')
    cursor_tochka = get_cursor('tochka')
    cursor_tmp = get_cursor('tmp')
