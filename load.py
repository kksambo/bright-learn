"""
Handles PostgreSQL connection and loading data into the database.
"""

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


class PostgreSQLLoader:

    def __init__(
        self,
        host="localhost",
        port=5434,
        database="postgres",
        username="postgres",
        password="postgres"
    ):

        self.connection_string = (
            f"postgresql+psycopg2://"
            f"{username}:{password}@"
            f"{host}:{port}/{database}"
        )

        self.engine = create_engine(
            self.connection_string,
            echo=False
        )

    def test_connection(self):
        """
        Tests the PostgreSQL connection.
        """

        try:
            with self.engine.connect():
                print("Connected to PostgreSQL successfully.")

        except Exception as e:
            print("Database connection failed.")
            raise e

    def load_table(self, dataframe, table_name):
        """
        Loads a dataframe into PostgreSQL.
        """

        try:

            dataframe.to_sql(
                table_name,
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000
            )

            print(f"Loaded {len(dataframe)} rows into {table_name}")

        except SQLAlchemyError as e:

            print(f"Error loading {table_name}")

            raise e

    def close(self):
        """
        Dispose engine.
        """

        self.engine.dispose()

        print("Database connection closed.")


# load.py
import sqlite3
import pandas as pd


class SQLiteLoader:
    def __init__(self, database_path="etl_data.db"):
        self.database_path = database_path
        self.connection = None
        self.cursor = None

    def connect(self):
        """Establish connection to SQLite database"""
        self.connection = sqlite3.connect(self.database_path)
        self.cursor = self.connection.cursor()
        return self.connection

    def test_connection(self):
        """Test if connection is working"""
        try:
            self.connect()
            self.cursor.execute("SELECT 1")
            print("✓ SQLite connection successful")
            return True
        except Exception as e:
            print(f"✗ SQLite connection failed: {e}")
            return False

    def load_table(self, df, table_name, if_exists='replace'):
        """Load DataFrame to SQLite table"""
        try:
            if not self.connection:
                self.connect()

            # SQLite uses 'replace' instead of 'truncate' for overwriting
            df.to_sql(table_name, self.connection, if_exists=if_exists, index=False)
            print(f"✓ Loaded {len(df)} rows into '{table_name}' table")

        except Exception as e:
            print(f"✗ Error loading to '{table_name}': {e}")
            raise

    def close(self):
        """Close the connection"""
        if self.connection:
            self.connection.close()
            print("Connection closed")