import asyncio
import time
import os
from extract import extract_and_load_staging
from transform import (
    clean_data,
    create_customer_dimension,
    create_store_dimension,
    create_product_dimension,
    create_transaction_dimension,
    create_sales_fact,
)
from load import SQLiteLoader


async def run_pipeline(csv_path, pipeline, db_config):
    start = time.time()

    pipeline["logs"] = []
    pipeline["progress"] = 0

    pipeline["status"] = "Extract"

    pipeline["logs"].append(f"Reading CSV: {os.path.basename(csv_path)}")

    # Extract - using the same function but now it should work with SQLite
    df = extract_and_load_staging(csv_path)

    pipeline["rows"] = len(df)

    pipeline["progress"] = 20

    await asyncio.sleep(1)

    pipeline["status"] = "Transform"

    pipeline["logs"].append("Cleaning data")

    df = clean_data(df)

    customers = create_customer_dimension(df)
    stores = create_store_dimension(df)
    products = create_product_dimension(df)
    transactions = create_transaction_dimension(df)

    sales = create_sales_fact(
        df,
        customers,
        stores,
        products,
        transactions,
    )

    pipeline["customers"] = len(customers)
    pipeline["stores"] = len(stores)
    pipeline["products"] = len(products)
    pipeline["transactions"] = len(transactions)
    pipeline["sales"] = len(sales)

    pipeline["progress"] = 70

    await asyncio.sleep(1)

    pipeline["status"] = "Load"

    pipeline["logs"].append("Connecting to SQLite database")

    # Use SQLiteLoader instead of PostgreSQLLoader
    database_path = db_config.get("database_path", "brightlearn_data.db")
    loader = SQLiteLoader(database_path=database_path)

    loader.test_connection()

    tables = [
        ("customers", customers),
        ("stores", stores),
        ("products", products),
        ("transactions", transactions),
        ("sales", sales),
    ]

    for name, table in tables:
        loader.load_table(table, name)
        pipeline["logs"].append(f"Loaded {name} ({len(table)} records)")
        pipeline["progress"] += 6
        await asyncio.sleep(0.8)

    loader.close()

    pipeline["progress"] = 100
    pipeline["status"] = "Completed"
    pipeline["time"] = round(time.time() - start, 2)

    pipeline["logs"].append("ETL Completed Successfully")
    pipeline["logs"].append(f"Execution Time: {pipeline['time']} seconds")
