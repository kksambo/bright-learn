import pandas as pd
import sqlite3



def extract_and_load_staging(csv_file, db_file="staging.db", table_name="staging_data"):
    """Extract CSV and load directly to SQLite staging"""

    # Extract
    df = pd.read_csv(csv_file)
    print(f"Extracted {len(df)} rows from {csv_file}")

    # Load to SQLite
    conn = sqlite3.connect(db_file)
    df.to_sql(table_name, conn, if_exists='replace', index=False)

    # Verify
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    conn.close()

    print(f"Loaded {count} rows to {db_file} as table '{table_name}'")
    return df



def read_staging_data(db_path="staging.db", table_name="staging_data"):
    """Read all data from staging database and return as DataFrame"""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


if __name__ == "__main__":
    df = extract_and_load_staging("BrightLearn_Raw_Data.csv")
    print(df.describe())
    print(df.dtypes)
    print( df.select_dtypes(include="object").columns)

    