from src.classes.checking.crypto import *

# TODO Проверка криптокошельков по точке

class CheckCrypto():
    def __init__(self):
        self.dict_db_cursors = WorkDB.get_all_cursors()
        self.df = pd.DataFrame()
        self.sql_crypto_lemonchik = '''
            select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
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



    # TODO продумать насчет универсальной функции по проверки криптокошельков
    def check_universal(self, db_name, sql_string):
        try:
            # Объект курсора
            table_tmp = self.dict_db_cursors[db_name].execute(sql_string)

            # Отбираем строки
            rows = table_tmp.fetchall()

            # Определение заголовков
            columns = [column[0] for column in self.dict_db_cursors[db_name].description]

            # Формирование временного df
            tmp_df = pd.DataFrame.from_records(rows, columns=columns)

            # Добавляем полученную информацию к изначальному df
            self.df = pd.concat([self.df, tmp_df], ignore_index=True)

          except pyodbc.Error as e:
                print(f"Ошибка при выполнении запроса: {e}")














          pass


    def check_lemonchik(self):
        '''
        Проверка криптокошельков по Лемончику
        :return: None
        '''
        sql_crypto_lemonchik = '''select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
                        from  lemonchik.arah_archive_exchange_bids
                        where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                              account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                              to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
                              from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
                              ;'''
        try:
            table_tmp = self.dict_db_cursors['lemonchik'].execute(sql_crypto_lemonchik)
            rows = table_tmp.fetchall()

            # Определение строки заголовков
            columns = [column[0] for column in self.dict_db_cursors['lemonchik'].description]
            # Формирование временного
            tmp_df = pd.DataFrame.from_records(rows, columns=columns)

            # Добавляем полученную информацию к изначальному df
            self.df = pd.concat([self.df, tmp_df], ignore_index=True)

        except pyodbc.Error as e:
            print(f"Ошибка при выполнении запроса: {e}")

    def check_tochka(self):





        pass

    def chech_cerber(self):
        pass




















try:

      table_tmp = self.dict_db_cursors['lemonchik'].execute(sql_crypto_lemonchik)
      rows = table_tmp.fetchall()

      # Определение строки заголовков
      columns = [column[0] for column in self.dict_db_cursors['lemonchik'].description]
      # Формирование временного
      tmp_df = pd.DataFrame.from_records(rows, columns=columns)

      # Добавляем полученную информацию к изначальному df
      self.df = pd.concat([self.df, tmp_df], ignore_index=True)

except pyodbc.Error as e:
      print(f"Ошибка при выполнении запроса: {e}")





