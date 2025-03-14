import pandas as pd
import pyodbc

from src.classes.WorkDB import *
from src.main import *

# TODO настройка типовых запросов
    # TODO Настройка запросов по криптокошельку

sql_string = '''select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  lemonchik.arah_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
      ;'''

dict_db_cursors = WorkDB.get_all_cursors()

try:
      table_tmp = dict_db_cursors['lemonchik'].execute(sql_string)
      rows = table_tmp.fetchall()
except pyodbc.Error as e:
      print(f"Ошибка при выполнении запроса: {e}")


rows

columns = [column[0] for column in dict_db_cursors['lemonchik'].description]
columns


df = pd.DataFrame.from_records(rows, columns=columns)
df


table_tmp
df = pd.DataFrame(rows)
df




# Объединение двух df
df = pd.concat([df, df2], ignore_index=True)

# Проверка криптокошельков по всем БД

# Отдельно по Точке
sql_crypto_tochka = '''

select 'tochka', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  tochka.tnp8_archive_exchange_bids

where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
'''

'''   
UNION
select 'cerber', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  cerber.1iyp_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
UNION
select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  lemonchik.arah_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
      ;


'''
db_name = 'lemonchik'

cursor_for_analytics = WorkDB.get_cursor(db_name)

sql_str = '''
# Проверка криптокошельков по имеющимся базам

select 'tochka', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  tochka.tnp8_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
      
      
UNION
select 'cerber', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  cerber.1iyp_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
UNION
select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  lemonchik.arah_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
      ;
'''
cursor_lemonchik.execute(sql_str)











select 'tochka', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  tochka.tnp8_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
'''


'''   
UNION
select 'cerber', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  cerber.1iyp_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
UNION
'''
sql_str = '''





