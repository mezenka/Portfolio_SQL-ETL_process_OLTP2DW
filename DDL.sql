create table demipt2.meze_dwh_dim_clients_hist (
    client_id varchar2(20 byte),
	last_name varchar2(100 byte),
	first_name varchar2(100 byte),
	patronymic varchar2(100 byte),
	date_of_birth date,
	passport_num varchar2(15 byte),
	passport_valid_to date,
	phone varchar2(20 byte),
    deleted_flg char(1),
	effective_from date,
    effective_to date
);

create table demipt2.meze_dwh_dim_accounts_hist (
    account_num char(20 byte),
	valid_to date,
	client varchar2(20 byte),
	deleted_flg char(1),
	effective_from date,
    effective_to date  
);

create table demipt2.meze_dwh_dim_cards_hist (
    card_num char(20 byte),
    account_num char(20 byte),
    deleted_flg char(1),
    effective_from date,
    effective_to date  
);

create table demipt2.meze_dwh_dim_terminals_hist (
    terminal_id varchar2(20 byte),
    terminal_type varchar2(20 byte),
    terminal_city varchar2(50),
	terminal_address varchar2(200 byte),
	deleted_flg char(1),
	effective_from date,
    effective_to date
);

create table demipt2.meze_dwh_fact_psspblcklst (
    entry_dt date,
    passport_num varchar2(15 byte)
);

create table demipt2.meze_dwh_fact_transactions (
    trans_id varchar2(50),
    trans_date date,
    amt decimal (10,2),
    card_num char(20 byte),
    oper_type varchar2(50),
    oper_result varchar2(50),
    terminal varchar2(20 byte)
);

create table demipt2.meze_stg_clients (
    client_id varchar2(20 byte),
	last_name varchar2(100 byte),
	first_name varchar2(100 byte),
	patronymic varchar2(100 byte),
	date_of_birth date,
	passport_num varchar2(15 byte),
	passport_valid_to date,
	phone varchar2(20 byte),
    create_dt date,
    update_dt date
);

create table demipt2.meze_stg_accounts (
    account_num char(20 byte),
	valid_to date,
	client varchar2(20 byte),
    create_dt date,
    update_dt date
);

create table demipt2.meze_stg_cards (
    card_num char(20 byte),
    account_num char(20 byte),
    create_dt date,
    update_dt date
);

create table demipt2.meze_stg_terminals (
    terminal_id varchar2(20 byte),
    terminal_type varchar2(20 byte),
    terminal_city varchar2(50),
	terminal_address varchar2(200 byte)
);

create table demipt2.meze_stg_psspblcklst (
    entry_dt date,
    passport_num varchar2(15 byte)
);

create table demipt2.meze_stg_transactions (
    trans_id varchar2(50),
    trans_date date,
    amt decimal (10,2),
    card_num char(20 byte),
    oper_type varchar2(50),
    oper_result varchar2(50),
    terminal varchar2(20 byte)
);

create table demipt2.meze_stg_clients_del (
    client_id varchar2(20 byte)
);

create table demipt2.meze_stg_accounts_del (
    account_num char(20 byte)
);  

create table demipt2.meze_stg_cards_del (
    card_num char(20 byte)
);

create table demipt2.meze_stg_terminals_del (
    terminal_id varchar2(20 byte)
);

create table demipt2.meze_meta_project(
    table_db varchar2(30),
    table_name varchar2(30),
    last_update_dt date
);

create table demipt2.meze_rep_fraud(
    event_dt date,
	passport varchar2(15 byte),
	fio varchar2(300 byte),
	phone varchar2(20 byte),
	event_type char(1),
	report_dt date
);
