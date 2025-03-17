'''
Проверка криптокошельков по всем БД
'''
from src.classes.WorkDB import *

class CheckCrypto():
    '''
    Класс по проверке криптокошельков по БД
    '''

    def __init__(self):
        self.dict_db_cursors = WorkDB.get_all_cursors()
        self.df = pd.DataFrame()
        self.sql_crypto_lemonchik = '''
            select DATABASE() as db_name, create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
            from  lemonchik.arah_archive_exchange_bids
            where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1);'''
        self.sql_crypto_tochka = '''
            select 'tochka', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
            from  tochka.tnp8_archive_exchange_bids
            where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1);'''
        self.sql_crypto_cerber = '''
            select 'cerber', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
            from  cerber.1iyp_archive_exchange_bids
            where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1);'''
        self.columns = ['db_name', 'create_date', 'edit_date', 'account_give', 'account_get', 'user_email', 'exsum',
                        'to_account', 'from_account', 'user_ip']

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
