"""Uses SQLAlchemy to pull out entries from a logbook in Opralog"""

from collections import namedtuple
from datetime import datetime
from typing import List

from sqlalchemy import Column, DateTime, Numeric, ForeignKey, String, Table
from sqlalchemy.sql import and_, select
from sqlalchemy.engine import Engine, URL, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    WriteOnlyMapped,
    mapped_column,
    relationship,
)

##########
# Configuration types
DatabaseConfig = namedtuple(
    "DatabaseConfig", ["host", "port", "name", "schema", "user", "password"]
)
##########

##########
# Constants
DIALECT = "mssql"
DRIVER = "pymssql"
BACKUP_OPRALOG_DB = DatabaseConfig(
    host="172.16.105.169",
    port=1433,
    name="LOGBOOK",
    schema="dbo",
    user="SA",
    password="Strong@Passw0rd",
)


##########
# Utility functions
def database_config() -> DatabaseConfig:
    # TODO: Read this from a file...
    return BACKUP_OPRALOG_DB


def create_sqlaengine(config: DatabaseConfig) -> Engine:
    url = URL.create(
        f"{DIALECT}+{DRIVER}",
        username=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.name,
    )
    return create_engine(url)


##########
# ORM mappings:
# NOTE: We do not provide the full table schemas here as SQLAlchemy doesn't
#       require the full schema to perform queries.
# SQLAlchemy.orm.DeclarativeBase cannot be used directly
class DeclMappingBase(DeclarativeBase):
    pass


# Junction table for many-to-many mapping of Logbook -> Entry
logbook_entries = Table(
    "LOGBOOK_ENTRIES",
    DeclMappingBase.metadata,
    Column("LOGBOOK_ID", ForeignKey("LOGBOOKS.LOGBOOK_ID"), primary_key=True),
    Column("ENTRY_ID", ForeignKey("ENTRIES.ENTRY_ID"), primary_key=True),
)


class Logbook(DeclMappingBase):
    """Defines the properties of the available log books"""

    __tablename__ = "LOGBOOKS"

    LOGBOOK_ID: Mapped[int] = mapped_column(Numeric, primary_key=True)
    LOGBOOK_NAME: Mapped[str] = mapped_column(String(50))

    entries: WriteOnlyMapped["Entry"] = relationship(
        secondary=LOGBOOK_ENTRIES,
        order_by="Entry.ENTRY_TIMESTAMP",
    )


class Entry(DeclMappingBase):
    """Each row holds the data for an entry in a log book with matching
    (ENTRY_ID, LOGBOOK_ID) in the LogbookEntry table"""

    __tablename__ = "ENTRIES"

    ENTRY_ID: Mapped[int] = mapped_column(Numeric, primary_key=True)

    ENTRY_TIMESTAMP: Mapped[datetime] = mapped_column(DateTime)
    ENTRY_DESCRIPTION: Mapped[str] = mapped_column(String(1000))
    GROUP_ID: Mapped[int] = mapped_column(Numeric)

    logbooks: WriteOnlyMapped["Logbook"] = relationship(
        secondary=logbook_entries, back_populates="entries"
    )


# class MoreEntryColumns(DeclMappingBase):
#     """Each row holds additional column data for the entry with id=ENTRY_ID"""

#     __tablename__ = "MORE_ENTRY_COLUMNS"

#     ENTRY_ID: Mapped[int] = mapped_column(Numeric, primary_key=True)
#     COLUMN_NO: Mapped[int] = mapped_column(Numeric, primary_key=True)
#     ENTRY_TYPE_ID: Mapped[int] = mapped_column(Numeric, primary_key=True)


# class AdditionalColumns(DeclMappingBase):
#     """Each row holds metadata describing a piece of additional information such as Group, Lost Time, Equipment"""

#     __tablename__ = "ADDITIONAL_COLUMNS"

#     COLUMN_NO: Mapped[int] = mapped_column(Numeric, primary_key=True)
#     ENTRY_TYPE_ID: Mapped[int] = mapped_column(Numeric, primary_key=True)


##########
# Main
logbooks_for_extraction = ["MCR Running Log"]

engine = create_sqlaengine(database_config())
with Session(engine) as session:
    logbook_name = logbooks_for_extraction[0]
    logbook = session.scalar(
        select(Logbook).where(Logbook.LOGBOOK_NAME == logbook_name)
    )
    entries = session.scalars(
        logbook.entries.select().where(
            and_(
                Entry.ENTRY_TIMESTAMP >= "2017-10-01 00:00:00",
                Entry.ENTRY_TIMESTAMP < "2018-01-01 00:00:00",
            )
        )
    ).all()
    print(len(entries))
    for entry in entries:
        print(entry.ENTRY_TIMESTAMP, entry.ENTRY_DESCRIPTION)
