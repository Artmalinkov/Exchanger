import configparser
import mysql.connector
from mysql.connector import Error
from pathlib import Path

class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.config = configparser.ConfigParser()
        config_path = Path(__file__).parent.parent / "config" / "db_config.ini"
        self.config.read(config_path)

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.config['mysql']['host'],
                port=self.config['mysql'].getint('port'),
                user=self.config['mysql']['user'],
                password=self.config['mysql']['password'],
                database=self.config['mysql']['database']
            )
            return True
        except Error as e:
            print(f"Ошибка подключения: {e}")
            return False

    def get_tables(self):
        """Возвращает список таблиц в БД"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            return [table[0] for table in cursor.fetchall()]
        except Error as e:
            print(f"Ошибка получения таблиц: {e}")
            return []

    def get_table_data(self, table_name, limit=100):
        """Возвращает данные из таблицы"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            return cursor.fetchall(), cursor.column_names
        except Error as e:
            print(f"Ошибка запроса: {e}")
            return None, None

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()