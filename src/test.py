import pandas as pd
from src.classes.WorkDB import *

# TODO настройка подключение к нужным базам






schemas = cursor_for_analytics.execute("SHOW DATABASES")
schemas
df = pd.DataFrame(schemas)
df

schemas.fetchone()

df = pd.DataFrame(schemas)
df[0]

schemas.fetchall()
schemas.fetchall()

df.iat[0, 0]



sql_string = '''
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
select 'lemonchik', create_date, edit_date, account_give, account_get, user_email, exsum, to_account, from_account, user_ip
from  lemonchik.arah_archive_exchange_bids
where account_give in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      account_get in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      to_account  in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1) or
      from_account in (select crypto_wallet from for_analytics.crypto_wallet where need_to_check = 1)
      ;'''

cursor_lemonchik = get_cursor('lemonchik')
cursor_lemonchik.execute(sql_str)



pd.set_option('display.max_colwidth', None)
df = pd.DataFrame(cursor_lemonchik.execute(sql_str))
df
df.loc[0]



