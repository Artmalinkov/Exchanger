# TODO Проверка по картам

'''
Проверка банковских карт по всем БД
'''
from src.classes.WorkDB import *
import pandas as pd


class Check():
    '''
    Общий класс по проверке реквизитов, который в последующем будет наследоваться и дополняться
    '''

    def __init__(self):
        self.dict_db_cursors = WorkDB.get_all_cursors()
        self.df = pd.DataFrame()


class CheckCard(Check):
    '''
    Класс по проверке криптокошельков по БД
    '''

    def __init__(self):
        # Вызов __init__ родительского класса
        super().__init__()
        self.columns = ['db_name', 'create_date', 'edit_date', 'account_give', 'account_get', 'user_email', 'exsum',
                        'to_account', 'from_account', 'user_ip']
        self.sql_card_lemonchik = '''
        select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
        from  lemonchik.arah_archive_exchange_bids
        where account_give in (select card from for_analytics.cards where need_check = 1) or
              account_get in (select card from for_analytics.cards where need_check = 1) or
              to_account  in (select card from for_analytics.cards where need_check = 1) or
              from_account in (select card from for_analytics.cards where need_check = 1);'''
        # TODO Добавить строки SQL по проверки по другим БД
    def check_universal(self, db_name, sql_string):
        '''
        Универсальный метод по проверке криптокошельков по всем БД
        :param db_name: Имя базы данных
        :param sql_string: SQL-запрос
        :return:
        '''
        try:
            # Переключение на целевую БД
            self.dict_db_cursors[db_name].execute(f'USE {db_name}')

            # Объект курсора
            table_tmp = self.dict_db_cursors[db_name].execute(sql_string)

            # Отбираем строки
            rows = table_tmp.fetchall()

            # Формирование временного df из строк
            tmp_df = pd.DataFrame.from_records(rows, columns=self.columns)

        except pyodbc.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

        # Проверка временный tmp_df на пустоту
        if not tmp_df.empty:
            # Добавляем полученную информацию к изначальному df
            self.df = pd.concat([self.df, tmp_df], ignore_index=True)


obj = CheckCard()
obj.columns

obj.check_universal('lemonchik', obj.sql_card_lemonchik)
obj.df
