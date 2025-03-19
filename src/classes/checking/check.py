'''
Проверка банковских карт по всем БД
'''
import pandas as pd

from src.classes.WorkDB import *


class Check():
    '''
    Общий класс по проверке реквизитов, который в последующем будет наследоваться и дополняться аотрибутами
    по типу реквизита
    '''

    def __init__(self):
        self.dict_db_cursors = WorkDB.get_all_cursors()
        self.df = pd.DataFrame()
        print('Вывод исходного DataFrame', self.df)

    def get_check(self, db_name, sql_string):
        '''
        Универсальный метод по проверке реквизита по одной БД
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

    def get_check_all_db(self):
        '''
        Универсальный метод по проверке реквизита по всем БД
        :return:
        '''
        # Запускаем цикл проверки sql_string по всем БД
        for db_name, sql_string in self.dict_sql_string.items():
            self.get_check(db_name, sql_string)

        # Вывод итогового df
        print('Результат\n', self.df)


class CheckCard(Check):
    '''
    Класс по проверке карт по БД
    '''

    def __init__(self):
        # Вызов __init__ родительского класса
        super().__init__()
        self.columns = ['db_name', 'create_date', 'edit_date', 'account_give', 'account_get', 'user_email', 'exsum',
                        'to_account', 'from_account', 'user_ip']
        self.dict_sql_string = {
            'lemonchik': '''select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, 
                                    to_account, from_account, user_ip
                            from  lemonchik.arah_archive_exchange_bids
                            where account_give in (select card from for_analytics.cards where need_check = 1) or
                                  account_get in (select card from for_analytics.cards where need_check = 1) or
                                  to_account  in (select card from for_analytics.cards where need_check = 1) or
                                  from_account in (select card from for_analytics.cards where need_check = 1);''',
            'cerber': '''select 'cerber', create_date, edit_date, account_give, account_get, user_email, exsum, 
                         to_account, from_account, user_ip
                         from  cerber.1iyp_archive_exchange_bids
                         where account_give in (select card from for_analytics.cards where need_check = 1) or
                                  account_get in (select card from for_analytics.cards where need_check = 1) or
                                  to_account  in (select card from for_analytics.cards where need_check = 1) or
                                  from_account in (select card from for_analytics.cards where need_check = 1);''',
            'tochka': '''select 'tochka', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
                        from  tochka.tnp8_archive_exchange_bids
                        where account_give in (select card from for_analytics.cards where need_check = 1) or
                                  account_get in (select card from for_analytics.cards where need_check = 1) or
                                  to_account  in (select card from for_analytics.cards where need_check = 1) or
                                  from_account in (select card from for_analytics.cards where need_check = 1);'''
        }


class CheckCrypto(Check):
    '''
    Класс по проверке криптокошельков по БД
    '''

    def __init__(self):
        super().__init__()
        self.columns = ['db_name', 'create_date', 'edit_date', 'account_give', 'account_get', 'user_email', 'exsum',
                        'to_account', 'from_account', 'user_ip']
        self.dict_sql_string = {'lemonchik': '''select 'lemonchik', create_date, edit_date, account_give, account_get, 
                                                        user_email, exsum, to_account, from_account, user_ip
                                 from  lemonchik.arah_archive_exchange_bids
                                 where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1);''',
                                'cerber': '''select 'cerber', create_date, edit_date, account_give, account_get, user_email, 
                                                     exsum, to_account, from_account, user_ip
                                 from  cerber.1iyp_archive_exchange_bids
                                 where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1);''',
                                'tochka': '''select 'tochka', create_date, edit_date, account_give, account_get, 
                                                     user_email, exsum, to_account, from_account, user_ip
                                 from  tochka.tnp8_archive_exchange_bids
                                 where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                       account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                       to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                                       from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1);'''
                                }


class EmailUsers(Check):
    '''
    Класс по проверке email по юзерам по БД
    '''

    def __init__(self):
        # Вызов __init__ родительского класса
        super().__init__()
        self.columns = ['db_name', 'user_login', 'user_email', 'display_name', 'user_browser', 'user_ip']
        self.dict_sql_string = {
            'lemonchik': '''select 'lemonchik', user_login, user_email, display_name, user_browser, user_ip
                            from  lemonchik.arah_users
                            where user_email in (select e_mail from for_analytics.e_mail where need_check = 1);''',
            'cerber': '''select 'cerber', user_login, user_email, display_name, user_browser, user_ip
                         from  cerber.1iyp_users
                         where user_email in (select e_mail from for_analytics.e_mail where need_check = 1);''',
            'tochka': '''select 'tochka', user_login, user_email, display_name, user_browser, user_ip
                         from  tochka.tnp8_users
                         where user_email in (select e_mail from for_analytics.e_mail where need_check = 1);'''
        }


class EmailTransactions(Check):
    '''
    Класс по проверке email по транзакциям по БД
    '''

    def __init__(self):
        # Вызов __init__ родительского класса
        super().__init__()
        self.columns = ['db_name', 'create_date', 'edit_date', 'account_give', 'account_get', 'user_email', 'exsum',
                        'to_account', 'from_account', 'user_ip']
        self.dict_sql_string = {
            'lemonchik': '''select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
                            from  lemonchik.arah_archive_exchange_bids
                            where user_email in (select e_mail from for_analytics.e_mail where need_check = 1);''',
            'cerber': '''select 'cerber', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
                            from  cerber.1iyp_archive_exchange_bids
                            where user_email in (select e_mail from for_analytics.e_mail where need_check = 1);''',
            'tochka': '''select 'tochka', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
                            from  tochka.tnp8_archive_exchange_bids
                            where user_email in (select e_mail from for_analytics.e_mail where need_check = 1);'''
        }


class IPUsers(Check):
    '''
    Класс по проверке IP по юзерам по БД
    '''

    def __init__(self):
        # Вызов __init__ родительского класса
        super().__init__()
        self.columns = ['db_name', 'user_login', 'user_email', 'display_name', 'user_browser', 'user_ip']
        self.dict_sql_string = {
            'lemonchik': '''select 'lemonchik', user_login, user_email, display_name, user_browser, user_ip
                            from  lemonchik.arah_users
                            where user_ip in (select IP_address from for_analytics.ip where need_check = 1);''',
            'cerber': '''select 'cerber', user_login, user_email, display_name, user_browser, user_ip
                         from  cerber.1iyp_users
                         where user_ip in (select IP_address from for_analytics.ip where need_check = 1);''',
            'tochka': '''select 'tochka', user_login, user_email, display_name, user_browser, user_ip
                         from  tochka.tnp8_users
                         where user_ip in (select IP_address from for_analytics.ip where need_check = 1);'''
        }
