# check_bacula_jobs.py

## Introduction

This is a Nagios / Icinga(2) plugin to monitor the status of 
completed backup jobs for a given client

Currently only bacula installations with PostgreSQL database 
are supported

## Install

This plugin requires the psychpg2 module

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