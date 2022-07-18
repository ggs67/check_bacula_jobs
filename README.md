# check_bacula_jobs.py

## Introduction

This is a Nagios / Icinga(2) plugin to monitor the status of 
completed backup jobs for a given client

This version of the script supports bacula installations with PostgreSQL or MySQL database

## IMPORTANT NOTES

### Early stage

While usable, this project is in a very early stage. Thus bugs are 
expected and changes in usage and behavior may change in the future. 

### Disclaimer

While the author make every effort to deliver high quality software, the author does not
guarantee that his software is free from defects. The software is provided “as-is," and you use the
software at your own risk.

The author makes no warranties as to performance, merchantability, fitness for a particular purpose, or any
other warranties whether expressed or implied.
No oral or written communication from or information provided by the author shall create a warranty.

Under no circumstances shall the author be liable for direct, indirect, special,
incidental, or consequential damages resulting from the use, misuse, or inability to use this software,
even if the author has been advised of the possibility of such damages.

## Install

This plugin requires the psychpg2 (PostgreSQL) or pymysql (MySQL) module. You may choose to install both or only the
required one. Note that the script assumes postgres database by default. User --mysql switch in order to connect
to a MySQL database.


```
pip3 install psycopg2-binary
```

## Command options
```
usage: check_bacula_jobs.py [-h] [-H HOST] [-p PORT] [-U DBUSER] [-P DBPASS] [-D DB] -C CLIENT [-d DAYS] [-j JOB] [-R]
                            [-w WARN WARN] [-c CRIT CRIT]

optional arguments:
  -h, --help            show this help message and exit
  -H HOST, --host HOST  Bacula database host name or address
  -p PORT, --port PORT  PostgreSQL port (default=5432)
  -U DBUSER, --dbuser DBUSER
                        Database user (default=potgres)
  -P DBPASS, --dbpass DBPASS
                        Database user password
  -D DB, --db DB
  -C CLIENT, --client CLIENT
                        Bacula client name.
  -d DAYS, --days DAYS  Number of days to consider when checking the job status for the given client
  -j JOB, --job JOB     Optional job name
  -R, --norunwarn       No run status gives warning
  -w <target> <thresh>, --warn <target> <thresh>
                        Warning threshold compatible to Nagios threshold range specifica tions. This option takes two
                        arguments, first the target data either '+' for the total successful job count or a job name,
                        and the threshold. ex. --warn + 3:
  -c <target> <thresh>, --crit CRIT CRIT<target> <thresh>
                        Critical threshold compatible to Nagios threshold range specific ations. This option takes two
                        arguments, first the target data either '+' for the total successful job count or a job name,
                        and the threshold. ex. --crit server-backup 3:
```