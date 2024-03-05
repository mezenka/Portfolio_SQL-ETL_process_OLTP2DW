#!/usr/bin/python

import pandas
import jaydebeapi
import time

conn = jaydebeapi.connect( 
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:demipt2/peregrintook@de-oracle.chronosavant.ru:1521/deoracle',
['demipt2','peregrintook'],
'/home/demipt2/ojdbc8.jar'
)
time.sleep(1)

conn.jconn.setAutoCommit(False)
time.sleep(1)

curs = conn.cursor()
time.sleep(1)

curs.execute ("delete from demipt2.meze_dwh_dim_clients_hist")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_clients")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_clients_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_dwh_dim_accounts_hist")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_accounts")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_accounts_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_dwh_dim_cards_hist")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_cards")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_cards_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_dwh_dim_terminals_hist")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_terminals")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_terminals_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_dwh_fact_psspblcklst")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_psspblcklst")
time.sleep(1)
curs.execute ("delete from demipt2.meze_dwh_fact_transactions")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_transactions")
time.sleep(1)
curs.execute ("delete from demipt2.meze_rep_fraud")
time.sleep(1)

conn.commit()
conn.close()
