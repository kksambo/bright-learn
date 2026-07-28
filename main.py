from extract import extract_and_load_staging,read_staging_data
from transform import (
    clean_data,
    create_customer_dimension,
    create_store_dimension,
    create_product_dimension,
    create_transaction_dimension,
    create_sales_fact,
)
from load import SQLiteLoader

CSV_FILE = "BrightLearn_Raw_Data.csv"


def main():

    # Extract and load data to staging database
    extract_and_load_staging(csv_file=CSV_FILE)

    #read data from staging database
    df = read_staging_data()


    # Transform

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


    # Load data to data warehouse database


    loader = SQLiteLoader(database_path="brightlearn_data.db")

    loader.test_connection()

    loader.load_table(customers, "customers")
    loader.load_table(stores, "stores")
    loader.load_table(products, "products")
    loader.load_table(transactions, "transactions")
    loader.load_table(sales, "sales")

    loader.close()
    print("\nETL completed successfully.")


if __name__ == "__main__":
    main()