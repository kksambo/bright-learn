"""
Contains all cleaning and normalization logic.
"""

import pandas as pd


def clean_data(df):
    """
    Cleans and normalizes the raw dataset.


    """

    print("Cleaning data...")

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Remove leading/trailing whitespace from text columns
    text_columns = df.select_dtypes(include="object").columns

    for column in text_columns:
        df[column] = df[column].astype(str).str.strip()

    # Standardize email addresses
    df["customer_email"] = (
        df["customer_email"]
        .str.lower()
        .str.strip()
    )
    df["customer_first_name"] = df["customer_first_name"].str.title()
    df["customer_last_name"] = df["customer_last_name"].str.title()
    df["customer_first_name"] = df["customer_first_name"].fillna("unknown")
    df["customer_last_name"] = df["customer_last_name"].fillna("unknown")

    # Convert date columns
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    )

    df["customer_since"] = pd.to_datetime(
        df["customer_since"],
        format="mixed",
        dayfirst=True,
        errors="coerce"
    )

    # Convert numeric columns
    numeric_columns = [
        "transaction_amount",
        "transaction_discount",
        "unit_price",
        "cost_price",
        "qty",
        "line_amount",
        "stock_on_hand",
        "reorder_threshold",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    # Replace missing discounts
    df["transaction_discount"] = df["transaction_discount"].fillna(0)

    # Replace missing loyalty tier
    df["customer_loyalty_tier"] = (
        df["customer_loyalty_tier"]
        .fillna("Bronze")
    )

    # Determine transaction type
    df["transaction_type"] = df["transaction_amount"].apply(
        lambda amount: "Refund" if amount < 0 else "Sale"
    )

    print("Cleaning completed.")

    return df


def create_customer_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the Customer Dimension.
    """

    customers = (
        df[
            [
                "customer_first_name",
                "customer_last_name",
                "customer_email",
                "customer_phone",
                "customer_city",
                "customer_province",
                "customer_loyalty_tier",
                "customer_since",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    customers.insert(
        0,
        "customer_id",
        range(1, len(customers) + 1),
    )

    return customers


def create_store_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the Store Dimension.
    """

    stores = (
        df[
            [
                "store_name",
                "store_city",
                "store_province",
                "store_region",
                "store_manager",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    stores.insert(
        0,
        "store_id",
        range(1, len(stores) + 1),
    )

    return stores


def create_product_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the Product Dimension.
    """

    products = (
        df[
            [
                "sku",
                "product_name",
                "category",
                "sub_category",
                "unit_price",
                "cost_price",
                "supplier",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    products.insert(
        0,
        "product_id",
        range(1, len(products) + 1),
    )

    return products


def create_transaction_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates the Transaction Dimension.
    """

    transactions = (
        df[
            [
                "transaction_date",
                "payment_method",
                "cashier_name",
                "transaction_amount",
                "transaction_discount",
                "transaction_type",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    transactions.insert(
        0,
        "transaction_id",
        range(1, len(transactions) + 1),
    )

    return transactions


def create_sales_fact(
    df: pd.DataFrame,
    customers: pd.DataFrame,
    stores: pd.DataFrame,
    products: pd.DataFrame,
    transactions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Creates the Sales Fact Table.

    The fact table contains only foreign keys and measures.
    """

    # Merge Transaction Dimension
    fact = df.merge(
        transactions,
        on=[
            "transaction_date",
            "payment_method",
            "cashier_name",
            "transaction_amount",
            "transaction_discount",
            "transaction_type",
        ],
        how="left",
    )

    # Merge Customer Dimension
    fact = fact.merge(
        customers,
        on=[
            "customer_first_name",
            "customer_last_name",
            "customer_email",
            "customer_phone",
            "customer_city",
            "customer_province",
            "customer_loyalty_tier",
            "customer_since",
        ],
        how="left",
    )

    # Merge Store Dimension
    fact = fact.merge(
        stores,
        on=[
            "store_name",
            "store_city",
            "store_province",
            "store_region",
            "store_manager",
        ],
        how="left",
    )

    # Merge Product Dimension
    fact = fact.merge(
        products,
        on=[
            "sku",
            "product_name",
            "category",
            "sub_category",
            "unit_price",
            "cost_price",
            "supplier",
        ],
        how="left",
    )

    # Keep only foreign keys and measurable values
    fact = fact[
        [
            "transaction_id",
            "customer_id",
            "store_id",
            "product_id",
            "qty",
            "line_amount",
            "stock_on_hand",
            "reorder_threshold",
        ]
    ].reset_index(drop=True)

    # Add surrogate key
    fact.insert(
        0,
        "sale_id",
        range(1, len(fact) + 1),
    )

    return fact