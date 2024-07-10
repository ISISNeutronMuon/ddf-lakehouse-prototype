# Data Platform Proof-of-Concept

## Introduction

After requirements gathering meetings and a workshop as part of the
[accelerator operational data platform project](https://stfc365.sharepoint.com/sites/ISISProject-1432)
there is appetite for trialling a system to support long-term storage, complex
queries and analysis of data from a multitude of sources across the accelerator
data space.

This document describes the initial set of requirements along with an overview
of the design and implementation of a minimal system to prove
the [Data Lakehouse](https://stfc365-my.sharepoint.com/:w:/g/personal/martyn_gigg_stfc_ac_uk/ETTFvkI6FulFhQFWmrmfGosBRO1Syvqbiq6DhVwnqxhVbw?e=HNKUIl)
concept can be valuable to ISIS.

## Architecture

The lakehouse architecture has several layers as described above.
The diagram below shows the proof-of-concept implementation within the SCD cloud.

![SCD cloud implementation](./images/ISIS%20Data%20Lakehouse%20PoC.png)

See [./architecture-decisions/](./architecture-decisions/) for a full list of
architecture decisions.

## Data Sources

The proof-of-concept will ingest data from the following sources:

- Crew logs: Opralog DB
- Vista/EPICS control system 2s data

### Opralog

The crew logs are captured by Opralog and stored in a Microsoft SQL Server database.
Full backups of the database run every 24 hours and T-Logs are run hourly.

For the purposes of testing a full backup of the database was obtained from the
Infrastructure team in ISIS and copied to a [Manila share](https://openstack.stfc.ac.uk/project/shares/faae6094-4856-4322-bbc1-678e414e32dd/)
under the path `[mount_point]/staging/opralog/full-backups`

The [MSSQL documentation](https://learn.microsoft.com/en-us/sql/linux/tutorial-restore-backup-in-sql-server-container?view=sql-server-2017&tabs=cli)
describes how to restore this to a server instance running in a Linux container.
The following commands will restore the database to a new container with the data stored in a docker volume `sql1data`:

```sh
> docker run -e 'ACCEPT_EULA=Y' -e 'MSSQL_SA_PASSWORD=Strong@Passw0rd' \
  --name 'sql1' -p 1433:1433 \
  -v sql1data:/var/opt/mssql -v /mnt/isis_lakehouse/staging:/staging:ro \
  -d mcr.microsoft.com/mssql/server:2019-latest

> docker exec -it sql1 /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P 'Strong@Passw0rd' \
  -Q 'RESTORE DATABASE LOGBOOK FROM DISK = "/staging/opralog/full-backups/LOGBOOK_backup_2024_06_24_183655_2079164.bak" WITH MOVE "LOGBOOK_Data" TO "/var/opt/mssql/LOGBOOK_Data.mdf", MOVE "LOGBOOK_Log" TO "/var/opt/mssql/LOGBOOK_Log.ldf"'
```

### Control system 2s data

TODO after talking with Controls Team.
