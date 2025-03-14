'''
Проверка криптокошельков по всем БД
'''
class CheckCrypto():
    dict_db_cursors = WorkDB.get_all_cursors()
    def check_lemonchik():
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
              table_tmp = dict_db_cursors['lemonchik'].execute(sql_crypto_lemonchik)
        except pyodbc.Error as e:
              print(f"Ошибка при выполнении запроса: {e}")

        rows = table_tmp.fetchall()
        columns = [column[0] for column in dict_db_cursors['lemonchik'].description]
        df = pd.DataFrame.from_records(rows, columns=columns)
        print(df)

