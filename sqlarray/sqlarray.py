#!/usr/bin/env python3
# (C) Daniel F. Smith, 2020, 2022
# SPDX-License-Identifier: LGPL-2.1
# Other licenses on request.

import os
import errno
import re
import sqlite3
from collections import UserDict


class SQLArray(UserDict):
    """
    A class that makes a SQLite3 database into an array-like object.

    Each key at the top level is the name of a database table.  Queries
    can be run against tables.  For example:

    from sqlarray import SQLArray
    table = SQLArray("my_filename")["mytable"]
    table["hello"] = "world"
    for key in table.search("or"):
        print(table[key])
    """

    def __init__(self, sqlfilename, create=True, convert=None, unconvert=None):
        """
        Open or create a SQLite3 database and return an array-like object
        of tables in the database.  Each subsequent table is a (key, value) store.

        Iterating over an SQLArray returns the tables.

        sqlfilename: string
            A filename to use as the SQLite database.  This will have '_sa.sqlite'
            appended to it if the file does not already exist.
        create: bool
            Create the DB if it does not exist.
        convert: function(object) -> str
            When writing a value to the database, it will be converted to a string
            with this function.
            E.g., json.dumps, lambda value: MyPickle(value, 1234)
        unconvert: function(str) -> object
            When reading a value from the database, it will be converted from a
            string with this function.
            E.g., json.loads.
        """
        self.allowedtablename = re.compile("[a-zA-Z_]+").search
        self.filename = sqlfilename
        self.convert = convert if convert else self.raw
        self.unconvert = unconvert if unconvert else self.raw

        if not os.path.isfile(self.filename):
            self.filename = f"{sqlfilename}_sa.sqlite"
        if not create and not os.path.isfile(self.filename):
            raise FileNotFoundError(errno.ENOENT, self.filename)
        # print(f"connecting to {self.filename}")
        self.sql = sqlite3.connect(self.filename)

    def raw(self, string):
        return string

    def __repr__(self):
        return f"SQLArray('{self.filename}')"

    class IterTables():
        def __init__(self, db):
            self.db = db
            self.cursor = self.db.sql.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")

        def __next__(self):
            tablename = self.cursor.__next__()[0]
            return self.db.Table(db, tablename)

    def __iter__(self):
        return self.IterTables(self)

    def __getitem__(self, tablename):
        return self.Table(self, tablename)

    def vacuum(self):
        """Compact the database by removing holes (deletions)."""
        self.sql.execute("VACUUM")

    class Table():
        """An associative array based on a table in an SQLite backed database."""

        def _sanitize_type(db_type):
            if not db_type:
                return None
            if isinstance(db_type, str):
                valid_types = ['NULL', 'INTEGER', 'REAL', 'TEXT', 'BLOB']
                if db_type in valid_types:
                    return db_type
                raise ValueError(
                    f"DB type must be one of {valid_types} (got {db_type})")
            if db_type==int or isinstance(db_type, int):
                return 'INTEGER'
            if db_type==bytes or db_type==bytearray or isinstance(db_type, (bytes, bytearray)):
                return 'BLOB'
            if db_type==float or isinstance(db_type, float):
                return 'REAL'
            raise ValueError(f"unknown DB type: {type(db_type)}")

        def __init__(self, db, name,
                     key_type=None, value_type=None,
                     convert=None, unconvert=None):
            """Use 'INTEGER', int, 'BLOB', bytes, 'TEXT', str, 'REAL', float, etc. for key/value_type."""
            self.db = db
            self.convert = convert if convert else db.convert
            self.unconvert = unconvert if unconvert else db.unconvert
            if not db.allowedtablename(name):
                raise KeyError
            self.name = name
            key_type = SQLArray.Table._sanitize_type(key_type)
            value_type = SQLArray.Table._sanitize_type(value_type)
            self.db.sql.execute(
                f"CREATE TABLE IF NOT EXISTS {self.name} ("
                f"key {key_type if key_type else ''} PRIMARY KEY,"
                f"value {value_type if value_type else ''}"
                f")")

        def __repr__(self):
            return self.db.__repr__() + f"['{self.name}']"

        def __str__(self):
            return self.name

        def row(self, key):
            return self.db.sql.execute(f"SELECT value FROM {self.name} WHERE key=?",
                                       (key,)).fetchone()

        def __len__(self):
            return self.db.sql.execute(f"SELECT count(*) FROM {self.name}").fetchone()[0]

        def __getitem__(self, key):
            row = self.row(key)
            if not row:
                raise KeyError
            return self.unconvert(row[0])

        def __setitem__(self, key, value):
            v = self.convert(value)
            self.db.sql.execute(f"REPLACE INTO {self.name} (key,value) VALUES (?,?)",
                                (key, v))
            self.db.sql.commit()

        def __delitem__(self, key):
            if not self.row(key):
                raise KeyError
            self.db.sql.execute(f"DELETE FROM {self.name} WHERE key=?",
                                (key,))

        class IterKey():
            def __init__(self, cursor):
                self.cursor = cursor

            def __iter__(self):
                return self

            def __next__(self):
                return self.cursor.__next__()[0]

        def __iter__(self):
            c = self.db.sql.cursor()
            return self.IterKey(c.execute(f"SELECT key FROM {self.name}"))

        def list(self):
            """Return a list of all keys."""
            return [row for row in self]

        def like(self, search):
            """Case insensitive SQL-like search over raw values."""
            c = self.db.sql.cursor()
            c.execute(f"SELECT key FROM {self.name} WHERE value LIKE ? ESCAPE '\\'",
                      (search,))
            return self.IterKey(c)

        def glob(self, search):
            """Case sensitive glob-like search over raw values."""
            c = self.db.sql.cursor()
            c.execute(f"SELECT key FROM {self.name} WHERE value GLOB ?",
                      (search,))
            return self.IterKey(c)

        def search(self, search):
            """Search for sub-string over raw values.  (Uses glob.)"""
            search.replace("*", "\\*").replace("?",
                                               "\\?").replace("[", "\\[").replace("]", "\\]")
            return self.glob("*" + search + "*")

        def equal(self, search):
            """Search for exact match in values."""
            c = self.db.sql.cursor()
            c.execute(f"SELECT key FROM {self.name} WHERE value == ?",
                      (search,))
            return self.IterKey(c)


if __name__ == "__main__":
    import sys
    for file in sys.argv[1:]:
        db = SQLArray(file, False)
        for table in db:
            for key in table:
                print(f"{file}['{table}']['{key}']: '{table[key]}'")
