'''
Основной функционал проекта согласно README
'''
import pandas as pd
import pyodbc
from src.classes.WorkDB import *

def config_pandas():
    '''
    Настройка конфигурации pandas
    :return: None
    '''
    # Настройка ширины вывода строки
    pd.set_option('display.max_colwidth', None)
    # Настройка ширины вывода заголовков
    pd.set_option('display.max_columns', None)
    # Настройка ширины вывода строк
    pd.set_option('display.width', 1000)

def main():
    # Настройка конфигурации pandas
    config_pandas()

    # Получение объектов курсоров
    dict_db_cursors = WorkDB.get_all_cursors()


if __name__ == '__main__':
    main()
