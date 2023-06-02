#!/usr/bin/python

import pandas
import jaydebeapi
import time
import os

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


# Incremental loading

# 1.1. Clear stagings for BANK database
curs.execute ("delete from demipt2.meze_stg_clients")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_clients_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_accounts")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_accounts_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_cards")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_cards_del")
time.sleep(1)

# 1.2. Capture data to stagings (all but deletes)
curs.execute ("""insert into demipt2.meze_stg_clients (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt)
select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, current_date from bank.clients
where update_dt > (select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD'))
    from demipt2.meze_meta_project where table_db = 'demipt2' and table_name = 'transactions')
    or create_dt = to_date('1900-01-01', 'YYYY-MM-DD') and client_id not in (select client_id from demipt2.meze_dwh_dim_clients_hist)""")
time.sleep(1)

curs.execute ("""insert into demipt2.meze_stg_accounts (account_num, valid_to, client, create_dt, update_dt) select account, valid_to, client, create_dt, current_date from bank.accounts
where update_dt > (select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD'))
    from demipt2.meze_meta_project where table_db = 'demipt2' and table_name = 'transactions')
    or create_dt = to_date('1900-01-01', 'YYYY-MM-DD') and account not in (select account_num from demipt2.meze_dwh_dim_accounts_hist)""")
time.sleep(1)

curs.execute ("""insert into demipt2.meze_stg_cards (card_num, account_num, create_dt, update_dt) select card_num, account, create_dt, current_date from bank.cards
where update_dt > (select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD'))
    from demipt2.meze_meta_project where table_db = 'demipt2' and table_name = 'transactions')
    or create_dt = to_date('1900-01-01', 'YYYY-MM-DD') and card_num not in (select card_num from demipt2.meze_dwh_dim_cards_hist)""")

time.sleep(1)

# 1.3. Capture keys for deletes
curs.execute ("insert into demipt2.meze_stg_clients_del (client_id) select client_id from bank.clients")
time.sleep(1)
curs.execute ("insert into demipt2.meze_stg_accounts_del (account_num) select account from bank.accounts")
time.sleep(1)
curs.execute ("insert into demipt2.meze_stg_cards_del (card_num) select card_num from bank.cards")
time.sleep(1)

# 1.4. Capture "inserts" and put into target tables
curs.execute ("""merge into demipt2.meze_dwh_dim_clients_hist tgt
using (
    select 
        s.client_id,
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.client_id order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_clients s
    left join demipt2.meze_dwh_dim_clients_hist t
    on s.client_id = t.client_id
    where 
        t.client_id is null or 
        (t.client_id is not null and ( 1=0
            or s.last_name <> t.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
            or s.first_name <> t.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
            or s.patronymic <> t.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
            or s.date_of_birth <> t.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
            or s.passport_num <> t.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
            or s.passport_valid_to <> t.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
            or s.phone <> t.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
        )
        )
) stg
on (tgt.client_id = stg.client_id)
when not matched then insert (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, deleted_flg, effective_from, effective_to)
values (stg.client_id, stg.last_name, stg.first_name, stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, 0, stg.effective_from, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_accounts_hist tgt
using (
    select 
        s.account_num,
        s.valid_to,
        s.client,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.account_num order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_accounts s
    left join demipt2.meze_dwh_dim_accounts_hist t
    on s.account_num = t.account_num
    where 
        t.account_num is null or 
        (t.account_num is not null and (1=0
            or s.valid_to <> t.valid_to or (s.valid_to is null and t.valid_to is not null ) or ( s.valid_to is not null and t.valid_to is null)
            or s.client <> t.client or (s.client is null and t.client is not null ) or ( s.client is not null and t.client is null)
        )
        )
) stg
on (tgt.account_num = stg.account_num)
when not matched then insert (account_num, valid_to, client, deleted_flg, effective_from, effective_to)
values (stg.account_num, stg.valid_to, stg.client, 0, stg.effective_from, stg.effective_to)
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_cards_hist tgt
using (
    select 
        s.card_num,
        s.account_num,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.card_num order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_cards s
    left join demipt2.meze_dwh_dim_cards_hist t
    on s.card_num = t.card_num
    where 
        t.card_num is null or 
        ( t.card_num is not null and ( 1=0
            or s.account_num <> t.account_num or ( s.account_num is null and t.account_num is not null ) or ( s.account_num is not null and t.account_num is null )
        )
        )
) stg
on ( tgt.card_num = stg.card_num )
when not matched then insert (card_num, account_num, deleted_flg, effective_from, effective_to) values ( stg.card_num, stg.account_num, 0, stg.effective_from, stg.effective_to)
""")
time.sleep(1)

# 1.5. Capture "updates" and put into target tables 
        # assumption: client may realistically change last name (marriage/divorse), passport number and validity date, phone number
curs.execute ("""merge into demipt2.meze_dwh_dim_clients_hist tgt
using (
    select 
        s.client_id,
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.client_id order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_clients s
    left join demipt2.meze_dwh_dim_clients_hist t
    on s.client_id = t.client_id
    where 
       t.client_id is null or 
        (t.client_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.last_name <> t.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
            or s.first_name <> t.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
            or s.patronymic <> t.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
            or s.date_of_birth <> t.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
            or s.passport_num <> t.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
            or s.passport_valid_to <> t.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
            or s.phone <> t.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
        )
        )
) stg
on (tgt.last_name = stg.last_name)
when not matched then insert (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, deleted_flg, effective_from, effective_to)
values (stg.client_id, stg.last_name, stg.first_name, stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_clients_hist tgt
using (
    select 
        s.client_id,
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.update_dt) over (partition by s.client_id order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_clients s
    left join demipt2.meze_dwh_dim_clients_hist t
    on s.client_id = t.client_id
    where 
       t.client_id is null or 
        (t.client_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.last_name <> t.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
            or s.first_name <> t.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
            or s.patronymic <> t.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
            or s.date_of_birth <> t.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
            or s.passport_num <> t.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
            or s.passport_valid_to <> t.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
            or s.phone <> t.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
        )
        )
) stg
on (tgt.passport_num = stg.passport_num)
when not matched then insert (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, deleted_flg, effective_from, effective_to)
values (stg.client_id, stg.last_name, stg.first_name, stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_clients_hist tgt
using (
    select 
        s.client_id,
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.update_dt) over (partition by s.client_id order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_clients s
    left join demipt2.meze_dwh_dim_clients_hist t
    on s.client_id = t.client_id
    where 
       t.client_id is null or 
        (t.client_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.last_name <> t.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
            or s.first_name <> t.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
            or s.patronymic <> t.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
            or s.date_of_birth <> t.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
            or s.passport_num <> t.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
            or s.passport_valid_to <> t.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
            or s.phone <> t.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
        )
        )
) stg
on (tgt.passport_valid_to = stg.passport_valid_to)
when not matched then insert (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, deleted_flg, effective_from, effective_to)
values (stg.client_id, stg.last_name, stg.first_name, stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_clients_hist tgt
using (
    select 
        s.client_id,
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.update_dt) over (partition by s.client_id order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_clients s
    left join demipt2.meze_dwh_dim_clients_hist t
    on s.client_id = t.client_id
    where 
       t.client_id is null or 
        (t.client_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.last_name <> t.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
            or s.first_name <> t.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
            or s.patronymic <> t.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
            or s.date_of_birth <> t.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
            or s.passport_num <> t.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
            or s.passport_valid_to <> t.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
            or s.phone <> t.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
        )
        )
) stg
on (tgt.phone = stg.phone)
when not matched then insert (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, deleted_flg, effective_from, effective_to)
values (stg.client_id, stg.last_name, stg.first_name, stg.patronymic, stg.date_of_birth, stg.passport_num, stg.passport_valid_to, stg.phone, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_accounts_hist tgt
using (
    select 
        s.account_num,
        s.valid_to,
        s.client,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.account_num order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_accounts s
    left join demipt2.meze_dwh_dim_accounts_hist t
    on s.account_num = t.account_num
    where 
       t.account_num is null or 
        (t.account_num is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.valid_to <> t.valid_to or ( s.valid_to is null and t.valid_to is not null ) or ( s.valid_to is not null and t.valid_to is null )
            or s.client <> t.client or ( s.client is null and t.client is not null ) or ( s.client is not null and t.client is null )
        )
        )
) stg
on (tgt.valid_to = stg.valid_to)
when not matched then insert (account_num, valid_to, client, deleted_flg, effective_from, effective_to)
values (stg.account_num, stg.valid_to, stg.client, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_accounts_hist tgt
using (
    select 
        s.account_num,
        s.valid_to,
        s.client,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.account_num order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_accounts s
    left join demipt2.meze_dwh_dim_accounts_hist t
    on s.account_num = t.account_num
    where 
       t.account_num is null or 
        (t.account_num is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.valid_to <> t.valid_to or ( s.valid_to is null and t.valid_to is not null ) or ( s.valid_to is not null and t.valid_to is null )
            or s.client <> t.client or ( s.client is null and t.client is not null ) or ( s.client is not null and t.client is null )
        )
        )
) stg
on (tgt.client = stg.client)
when not matched then insert (account_num, valid_to, client, deleted_flg, effective_from, effective_to)
values (stg.account_num, stg.valid_to, stg.client, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_cards_hist tgt
using (
    select 
        s.card_num,
        s.account_num,
        deleted_flg,
        s.create_dt as effective_from,
        coalesce (lead (s.create_dt) over (partition by s.card_num order by s.create_dt) - interval '1' day, to_date('9999-12-31', 'YYYY-MM-DD')) effective_to
    from demipt2.meze_stg_cards s
    left join demipt2.meze_dwh_dim_cards_hist t
    on s.card_num = t.card_num
    where 
        t.card_num is null or 
        ( t.card_num is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.account_num <> t.account_num or ( s.account_num is null and t.account_num is not null ) or ( s.account_num is not null and t.account_num is null )
        )
        )
) stg
on (tgt.card_num = stg.card_num)
when not matched then insert (card_num, account_num, deleted_flg, effective_from, effective_to) values ( stg.card_num, stg.account_num, 0, current_date, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

# 1.6. Update closing dates of previous versions of updated entries
curs.execute ("""update demipt2.meze_dwh_dim_clients_hist
set effective_to = current_date - interval '1' day
where 
        last_name in (
            select distinct
                t.last_name
                    from demipt2.meze_dwh_dim_clients_hist t
                    left join demipt2.meze_stg_clients s
                    on t.client_id = s.client_id
                    where effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and t.last_name <> s.last_name
                        or t.first_name <> s.first_name
                        or t.patronymic <> s.patronymic
                        or t.date_of_birth <> s.date_of_birth
                        or t.passport_num <> s.passport_num
                        or t.passport_valid_to <> s.passport_valid_to
                        or t.phone <> s.phone
)""")
time.sleep(1)

curs.execute ("""update demipt2.meze_dwh_dim_accounts_hist
set effective_to = current_date - interval '1' day
where 
        client
          in (
            select
                t.client
                    from demipt2.meze_dwh_dim_accounts_hist t
                    left join demipt2.meze_stg_accounts s
                    on t.account_num = s.account_num
                    where effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and t.valid_to <> s.valid_to
                        or t.client <> s.client
)""")
time.sleep(1)

curs.execute ("""update demipt2.meze_dwh_dim_cards_hist
set effective_to = current_date - interval '1' day
where account_num in (
    select
        t.account_num
            from demipt2.meze_dwh_dim_cards_hist t
            left join demipt2.meze_stg_cards s
            on t.card_num = s.card_num
            where effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and t.account_num <> s.account_num
)""")
time.sleep(1)

# 1.7. Put flags on deleted entries in target tables
curs.execute ("""update demipt2.meze_dwh_dim_clients_hist
set deleted_flg = 1, effective_from = current_date, effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
where last_name in (
    select
        t.last_name
    from demipt2.meze_dwh_dim_clients_hist t
    left join demipt2.meze_stg_clients_del s
    on t.client_id = s.client_id
        where s.client_id is null and deleted_flg = 0 and effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
)""")
time.sleep(1)

curs.execute ("""update demipt2.meze_dwh_dim_accounts_hist
set deleted_flg = 1, effective_from = current_date, effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
where client in (
    select
        t.client
    from demipt2.meze_dwh_dim_accounts_hist t
    left join demipt2.meze_stg_accounts_del s
    on t.account_num = s.account_num
    where s.account_num is null and deleted_flg = 0 and effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
)""")
time.sleep(1)

curs.execute ("""update demipt2.meze_dwh_dim_cards_hist
set deleted_flg = 1, effective_from = current_date, effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
where account_num in (
    select
        t.account_num
    from demipt2.meze_dwh_dim_cards_hist t
    left join demipt2.meze_stg_cards_del s
    on t.card_num = s.card_num
    where s.card_num is null and deleted_flg = 0 and effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
)""")
time.sleep(1)

# 1.8. Return previous versions of deleted entries into target tables and set dates of deletes
curs.execute ("""insert into demipt2.meze_dwh_dim_clients_hist (
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    deleted_flg,
    effective_from,
    effective_to)
        select distinct
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            0,
            effective_from,
            current_date - interval '1' day
            from demipt2.meze_dwh_dim_clients_hist
            where deleted_flg = 1 and client_id in (
            select
                client_id
                from
                    (select distinct
                        client_id,
                        count (client_id) over (partition by client_id order by effective_from) cnt_qty
                        from demipt2.meze_dwh_dim_clients_hist)
                        where cnt_qty < 2
)""")
time.sleep(1)

curs.execute ("""insert into demipt2.meze_dwh_dim_accounts_hist (
    account_num,
    valid_to,
    client,
    deleted_flg,
    effective_from,
    effective_to)
        select distinct
            account_num,
			valid_to,
			client,
            0,
            effective_from,
            current_date - interval '1' day
            from demipt2.meze_dwh_dim_accounts_hist
            where deleted_flg = 1 and account_num in (
            select
                account_num
                from
                    (select distinct
                        account_num,
                        count (account_num) over (partition by account_num order by effective_from) acc_qty
                        from demipt2.meze_dwh_dim_accounts_hist)
                        where acc_qty < 2
)""")
time.sleep(1)

curs.execute ("""insert into demipt2.meze_dwh_dim_cards_hist (
    card_num,
    account_num,
    deleted_flg,
    effective_from,
    effective_to)
        select distinct
            card_num,
            account_num,
            0,
            effective_from,
            current_date - interval '1' day
            from demipt2.meze_dwh_dim_cards_hist
            where deleted_flg = 1 and card_num in (
            select
                card_num
                from
                    (select distinct
                        card_num,
                        count (card_num) over (partition by card_num order by effective_from) crd_qty
                        from demipt2.meze_dwh_dim_cards_hist)
                        where crd_qty < 2
)""")
time.sleep(1)

# 2.1. Clear stagings for daily FILES
curs.execute ("delete from demipt2.meze_stg_terminals")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_terminals_del")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_psspblcklst")
time.sleep(1)
curs.execute ("delete from demipt2.meze_stg_transactions")
time.sleep(1)


# 2.2. Capture data from FILES to stagings (using PYTHON)
df1 = pandas.read_excel( '/home/demipt2/meze/terminals_03032021.xlsx', sheet_name='terminals', header=0, index_col=None )
curs.executemany ("insert into demipt2.meze_stg_terminals (terminal_id, terminal_type, terminal_city, terminal_address) values (?,?,?,?)", df1.values.tolist())
time.sleep(1)
os.rename ('/home/demipt2/meze/terminals_03032021.xlsx', '/home/demipt2/meze/terminals_03032021.xlsx' + '.backup')
os.replace ('/home/demipt2/meze/terminals_03032021.xlsx.backup', '/home/demipt2/meze/archive/terminals_03032021.xlsx.backup')
time.sleep(1)

df2 = pandas.read_excel( '/home/demipt2/meze/passport_blacklist_03032021.xlsx', sheet_name='blacklist', header=0, index_col=None )
df2['date'] = df2['date'].astype(str)
curs.executemany ("insert into demipt2.meze_stg_psspblcklst (entry_dt, passport_num) values( to_date(?, 'YYYY-MM-DD HH24:MI:SS'), ?)", df2.values.tolist())
time.sleep(1)
os.rename ('/home/demipt2/meze/passport_blacklist_03032021.xlsx', '/home/demipt2/meze/passport_blacklist_03032021.xlsx' + '.backup')
os.replace ('/home/demipt2/meze/passport_blacklist_03032021.xlsx.backup', '/home/demipt2/meze/archive/passport_blacklist_03032021.xlsx.backup')
time.sleep(1)

df3 = pandas.read_csv( '/home/demipt2/meze/transactions_03032021.txt', sep=';', header=0, index_col=None, decimal=",")
df3['transaction_date'] = df3['transaction_date'].astype(str)
curs.executemany( "insert into demipt2.meze_stg_transactions (trans_id,trans_date,amt,card_num,oper_type,oper_result,terminal) values(?, to_date(?,'YYYY-MM-DD HH24:MI:SS'), ?, ?, ?, ?, ?)", df3.values.tolist() )
time.sleep(1)
os.rename ('/home/demipt2/meze/transactions_03032021.txt', '/home/demipt2/meze/transactions_03032021.txt' + '.backup')
os.replace ('/home/demipt2/meze/transactions_03032021.txt.backup', '/home/demipt2/meze/archive/transactions_03032021.txt.backup')
time.sleep(1)


# 2.3. Capture keys for deletes (terminals only as passports and transaction are fact tables)
curs.execute ("insert into demipt2.meze_stg_terminals_del (terminal_id) select terminal_id from demipt2.meze_stg_terminals")
time.sleep(1)

# 2.4. Capture "inserts" and put into target table (terminals only)
curs.execute ("""merge into demipt2.meze_dwh_dim_terminals_hist tgt
using (
    select 
        s.terminal_id,
        s.terminal_type,
        s.terminal_city,
        s.terminal_address,
        deleted_flg,
        to_date('1900-01-01', 'YYYY-MM-DD') effective_from,
        to_date('9999-12-31', 'YYYY-MM-DD') effective_to
    from demipt2.meze_stg_terminals s
    left join demipt2.meze_dwh_dim_terminals_hist t
    on s.terminal_id = t.terminal_id
    where 
        t.terminal_id is null or 
        (t.terminal_id is not null and ( 1=0
            or s.terminal_type <> t.terminal_type or ( s.terminal_type is null and t.terminal_type is not null ) or ( s.terminal_type is not null and t.terminal_type is null )
            or s.terminal_city <> t.terminal_city or ( s.terminal_city is null and t.terminal_city is not null ) or ( s.terminal_city is not null and t.terminal_city is null )
            or s.terminal_address <> t.terminal_address or ( s.terminal_address is null and t.terminal_address is not null ) or ( s.terminal_address is not null and t.terminal_address is null )
        )
        )
) stg
on (tgt.terminal_id = stg.terminal_id)
when not matched then insert (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg, effective_from, effective_to)
values (stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, 0, stg.effective_from, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_fact_psspblcklst tgt
using (
    select 
        s.entry_dt,
        s.passport_num
    from demipt2.meze_stg_psspblcklst s
    left join demipt2.meze_dwh_fact_psspblcklst t
    on s.passport_num = t.passport_num
    where 
        t.passport_num is null or 
        (t.passport_num is not null and ( 1=0
            or s.entry_dt <> t.entry_dt or ( s.entry_dt is null and t.entry_dt is not null ) or ( s.entry_dt is not null and t.entry_dt is null )
        )
        )
) stg
on (tgt.passport_num = stg.passport_num)
when not matched then insert (entry_dt, passport_num)
values (stg.entry_dt, stg.passport_num)
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_fact_transactions tgt
using (
    select 
        s.trans_id,
        s.trans_date,
        s.amt,
        s.card_num,
        s.oper_type,
        s.oper_result,
        s.terminal
    from demipt2.meze_stg_transactions s
    left join demipt2.meze_dwh_fact_transactions t
    on s.trans_id = t.trans_id
    where 
        t.trans_id is null or 
        (t.trans_id is not null and ( 1=0
            or s.trans_date <> t.trans_date or ( s.trans_date is null and t.trans_date is not null ) or ( s.trans_date is not null and t.trans_date is null )
        )
        )
) stg
on (tgt.trans_id = stg.trans_id)
when not matched then insert (trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal)
values (stg.trans_id, stg.trans_date, stg.amt, stg.card_num, stg.oper_type, stg.oper_result, stg.terminal)
""")
time.sleep(1)

# 2.5. Capture "updates" and put into target tables (teminals only)
curs.execute ("""merge into demipt2.meze_dwh_dim_terminals_hist tgt
using (
    select 
        s.terminal_id,
        s.terminal_type,
        s.terminal_city,
        s.terminal_address,
        deleted_flg,
        (select max(trans_date) from demipt2.meze_stg_transactions) effective_from,
        to_date('9999-12-31', 'YYYY-MM-DD') effective_to
    from demipt2.meze_stg_terminals s
    left join demipt2.meze_dwh_dim_terminals_hist t
    on s.terminal_id = t.terminal_id
    where 
        t.terminal_id is null or 
        (t.terminal_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.terminal_type <> t.terminal_type or ( s.terminal_type is null and t.terminal_type is not null ) or ( s.terminal_type is not null and t.terminal_type is null )
            or s.terminal_city <> t.terminal_city or ( s.terminal_city is null and t.terminal_city is not null ) or ( s.terminal_city is not null and t.terminal_city is null )
            or s.terminal_address <> t.terminal_address or ( s.terminal_address is null and t.terminal_address is not null ) or ( s.terminal_address is not null and t.terminal_address is null )
        )
        )
) stg
on (tgt.terminal_type = stg.terminal_type)
when not matched then insert (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg, effective_from, effective_to)
values (stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, 0, stg.effective_from, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_terminals_hist tgt
using (
    select 
        s.terminal_id,
        s.terminal_type,
        s.terminal_city,
        s.terminal_address,
        deleted_flg,
        (select max(trans_date) from demipt2.meze_stg_transactions) effective_from,
        to_date('9999-12-31', 'YYYY-MM-DD') effective_to
    from demipt2.meze_stg_terminals s
    left join demipt2.meze_dwh_dim_terminals_hist t
    on s.terminal_id = t.terminal_id
    where 
        t.terminal_id is null or 
        (t.terminal_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.terminal_type <> t.terminal_type or ( s.terminal_type is null and t.terminal_type is not null ) or ( s.terminal_type is not null and t.terminal_type is null )
            or s.terminal_city <> t.terminal_city or ( s.terminal_city is null and t.terminal_city is not null ) or ( s.terminal_city is not null and t.terminal_city is null )
            or s.terminal_address <> t.terminal_address or ( s.terminal_address is null and t.terminal_address is not null ) or ( s.terminal_address is not null and t.terminal_address is null )
        )
        )
) stg
on (tgt.terminal_city = stg.terminal_city)
when not matched then insert (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg, effective_from, effective_to)
values (stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, 0, stg.effective_from, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

curs.execute ("""merge into demipt2.meze_dwh_dim_terminals_hist tgt
using (
    select 
        s.terminal_id,
        s.terminal_type,
        s.terminal_city,
        s.terminal_address,
        deleted_flg,
        (select max(trans_date) from demipt2.meze_stg_transactions) effective_from,
        to_date('9999-12-31', 'YYYY-MM-DD') effective_to
    from demipt2.meze_stg_terminals s
    left join 
        (select
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            deleted_flg,
            effective_from,
            effective_to
        from demipt2.meze_dwh_dim_terminals_hist) t
    on s.terminal_id = t.terminal_id
    where 
        t.terminal_id is null or 
        (t.terminal_id is not null and t.effective_to = to_date('9999-12-31', 'YYYY-MM-DD') and ( 1=0
            or s.terminal_type <> t.terminal_type or ( s.terminal_type is null and t.terminal_type is not null ) or ( s.terminal_type is not null and t.terminal_type is null )
            or s.terminal_city <> t.terminal_city or ( s.terminal_city is null and t.terminal_city is not null ) or ( s.terminal_city is not null and t.terminal_city is null )
            or s.terminal_address <> t.terminal_address or ( s.terminal_address is null and t.terminal_address is not null ) or ( s.terminal_address is not null and t.terminal_address is null )
        )
        )
) stg
on (tgt.terminal_address = stg.terminal_address)
when not matched then insert (terminal_id, terminal_type, terminal_city, terminal_address, deleted_flg, effective_from, effective_to)
values (stg.terminal_id, stg.terminal_type, stg.terminal_city, stg.terminal_address, 0, stg.effective_from, to_date('9999-12-31', 'YYYY-MM-DD'))
""")
time.sleep(1)

# 2.6. Update closing dates of previous versions of updated entries (teminals only)
curs.execute ("""update demipt2.meze_dwh_dim_terminals_hist
set effective_to = (select max(trans_date) from demipt2.meze_stg_transactions) - interval '1' day
where 
        terminal_address in (
            select
                t.terminal_address
                    from demipt2.meze_dwh_dim_terminals_hist t
                    left join demipt2.meze_stg_terminals s
                    on t.terminal_id = s.terminal_id
                    where t.terminal_type <> s.terminal_type
                        or t.terminal_city <> s.terminal_city
                        or t.terminal_address <> s.terminal_address
                        and effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
                    )""")
time.sleep(1)
 
# 2.7. Put flags on deleted entries in target tables (terminals only)
curs.execute ("""update demipt2.meze_dwh_dim_terminals_hist
set deleted_flg = 1, effective_to = (select max(trans_date) from demipt2.meze_stg_transactions) - interval '1' day where terminal_address in (
    select
        t.terminal_address
    from demipt2.meze_dwh_dim_terminals_hist t
    left join demipt2.meze_stg_terminals_del s
    on t.terminal_id = s.terminal_id
        where s.terminal_id is null and deleted_flg = 0 and effective_to = to_date('9999-12-31', 'YYYY-MM-DD')
)""")
time.sleep(1)

# 2.8. Return previous versions of deleted entries into target tables and set dates of deletes (terminals only)
curs.execute ("""insert into demipt2.meze_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    deleted_flg,
    effective_from,
    effective_to)
        select distinct
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            0,
            (select max(trans_date) from demipt2.meze_stg_transactions),
            to_date('9999-12-31', 'YYYY-MM-DD')
            from demipt2.meze_dwh_dim_terminals_hist
            where deleted_flg = 1 and terminal_id in (
            select
                terminal_id
                from
                    (select distinct
                        terminal_id,
                        count (terminal_id) over (partition by terminal_id order by effective_from) tmn_qty
                        from demipt2.meze_dwh_dim_terminals_hist)
                        where tmn_qty < 2
            )""")
time.sleep(1)

# 3. Fraud report
curs.execute ("""insert into demipt2.meze_rep_fraud (
	event_dt,
	passport,
	fio,
	phone,
	event_type,
	report_dt)
		select 
			event_dt,
			passport_num,
			last_name ||' '||first_name||' '||patronymic as fio,
			phone,
			event_type,
			report_dt
			from(
				select distinct
					max (trans_date) over (partition by passport_num) as event_dt,
					passport_num,
					last_name,
					first_name,
					patronymic,
					phone,
					case
						when passport_valid_to < trans_date or passport_num in (select (passport_num) from demipt2.meze_dwh_fact_psspblcklst) then 1
						when deleted_flg = 1 then 2
						when lead (terminal_city) over (partition by card_num order by trans_date) <> terminal_city and lead (trans_date) over (partition by card_num order by trans_date) - trans_date < 60 then 3
						else 0
					end event_type,
					(select max(trans_date) from demipt2.meze_stg_transactions) as report_dt
					from (
							select
								t.trans_id,
								t.trans_date,
								t.amt,
								t.card_num,
								t.oper_type,
								t.terminal,
								tm.terminal_city,
								c.client_id,
								c.last_name,
								c.first_name,
								c.patronymic,
								c.date_of_birth,
								c.passport_num,
								c.passport_valid_to,
								c.phone,
								c.deleted_flg,
								c.effective_from,
								c.effective_to
								from demipt2.meze_dwh_dim_terminals_hist tm
								left join demipt2.meze_dwh_fact_transactions t
								on tm.terminal_id = t.terminal
									left join demipt2.meze_dwh_dim_cards_hist cd
									on t.card_num = cd.card_num
										left join demipt2.meze_dwh_dim_accounts_hist a
										on cd.account_num = a.account_num
											left join demipt2.meze_dwh_dim_clients_hist c
											on a.client = c.client_id
							)
							) where event_type > 0
""")
time.sleep(1)

# Update meta
curs.execute ("""update demipt2.meze_meta_project
set last_update_dt = (select max(trans_date) from demipt2.meze_stg_transactions)
where table_db = 'demipt2' and table_name = 'transactions' and (select max(trans_date) from demipt2.meze_stg_transactions) is not null""")


conn.commit()
conn.close()
