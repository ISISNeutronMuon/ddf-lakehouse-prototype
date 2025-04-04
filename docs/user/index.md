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
- Vista/EPICS control system 2s data: InfluxDB
- Running schedule: ISIS status display [API](https://status.isis.stfc.ac.uk/api/schedule)

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

The 2s control system data is stored in an InfluxDB instance managed by the accelerator controls team.

For the purposes of testing a full backup of the database was obtained from the accelerator
controls team copied to a [Manila share](https://openstack.stfc.ac.uk/project/shares/faae6094-4856-4322-bbc1-678e414e32dd/)
under the path `[mount_point]/staging/influxdb/influxdbv2/data`

### Running schedule

The running schedule is available from the system supporting the physical
status display screens around ISIS buildings. A JSON document with the schedule
can be retrieved from <https://status.isis.stfc.ac.uk/api/schedule>.

It currently contains dates from 2019/1 onwards.

## Data Architecture

The data layout follows a layered structure where each layer adds transformations
taking the data the raw source data to modeled data with business rules applied,
ready for consumption by downstream users such as BI (business intelligence) tools,
ML tools etc.

![Lakehouse data architecture](./images/lakehouse-data-architecture-layers.jpg)

### Raw

The raw layer is a source-aligned layer that preserves the source structure where
possible. This layer does not consist of managed tables and is simply sets of files
containing data exported from source systems.

The files are stored in a object store on the SCD cloud in a bucket called
`staging-isis` and contains exported files in various formats such as JSON, Parquet
listed under directories named after the system from which the data originates, e.g:

```txt
staging-isis/
|-- [sourcename]
    |-- incoming/
        |-- full/
        |   |-- YYYY/
        |       |-- MM/
        |           |-- DD/
        |               |-- [tablename]_full_YYYYMMDD.parquet
        |-- incremental/
            |-- YYYY/
                |-- MM/
                    |-- DD/
                        |-- [tablename]_incremental_YYYYMMDD.parquet
```

The directory parts having the following meaning:

```markdown
TODO: DESCRIBE INGESTION directory structure
```

## Managed tables

The remaining layers are stored within a catalog of
[Apache Iceberg](https://iceberg.apache.org/) tables and apply structure through
SQL transformations. Iceberg catalogs support the concept of schemas, a grouping
of tables, to allow a structure to be imposed that aids with tasks such as
discoverability and access control.

TODO: DESCRIBE catalogs and schemas
