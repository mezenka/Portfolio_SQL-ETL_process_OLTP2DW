## Individual project
Denis Mezenko
May 2022, MIPT School of Applied Mathematics and Informatics

## Task:
===========================================================================
## Develop an ETL process that receives daily ATM operation data (provided for 3 days), uploads it to the data warehouse, and builds daily report.

Uploading data:
Every day the following three files are received from some OLTP systems:
1. List of transactions for the current day. Format - CSV.
2. List of terminals. Format - XLSX.
3. List of passports included in the "black list" - cumulative from the beginning of the month. Format - XLSX.

Information about cards, accounts, and customers is stored in the Oracle DBMS in the BANK schema.
You are provided with an upload for the last three days, it must be processed.

The data must be loaded into storage with the specified structure.
===========================================================================

main.py - main process

main.cron - scheduling the main.py process

Archive directory - for processed daily files (empty in the archive, become filled with processed files on the server whe the process is initiated)

Fraud report is in demipt2.meze_rep_fraud table

DDL.sql in sql_scripts directory - creation of all tables for the project 

clrtbs.py in the py_scripts directory - clearing all tables
