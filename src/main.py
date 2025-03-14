'''
Основной функционал проекта согласно README
'''
import pandas as pd
from src.classes.WorkDB import *

def get_start():
    '''
    Начало работы, подключение.
    :return: об
    '''

    cursor_for_analytics = WorkDB.get_cursor('for_analytics')
    cursor_cerber = WorkDB.get_cursor('cerber')
    cursor_lemonchik = WorkDB.get_cursor('lemonchik')
    cursor_tochka = WorkDB.get_cursor('tochka')
    cursor_tmp = WorkDB.get_cursor('tmp')
    return cursor_for_analytics, cursor_cerber, cursor_lemonchik, cursor_tochka, cursor_tmp


def main():
    # Получение объектов курсоров
    cursor_for_analytics, cursor_cerber, cursor_lemonchik, cursor_tochka, cursor_tmp = get_start()


if __name__ == '__main__':
    main()
