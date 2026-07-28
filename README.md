
# BrightLearn ETL Data Pipeline

## Overview

This project is an **ETL (Extract, Transform, Load) data pipeline** built with **Python, Pandas, and PostgreSQL**. It reads retail sales data from a CSV file, cleans and transforms the data into a **star schema**, and loads it into a PostgreSQL database for reporting and analytics.

---

## Tech Stack

- Python 3
- Pandas
- PostgreSQL
- SQLAlchemy
- psycopg2

---

## Project Structure

```text
.
├── extract.py          # Extract data from CSV
├── transform.py        # Data cleaning and transformation
├── load.py             # Load data into PostgreSQL
├── main.py             # ETL pipeline
├── schema.sql          # Database schema
├── BrightLearn_Raw_Data.csv
└── README.md
```

---

## Data Pipeline

```text
CSV File
    │
    ▼
Extract
    │
    ▼
Clean & Transform
    │
    ▼
Create Dimensions & Fact Table
    │
    ▼
Load into PostgreSQL
```

---

## Database Schema

### Dimensions

- Customers
- Products
- Stores
- Transactions

### Fact Table

- Sales

The **Sales** fact table stores foreign keys to each dimension along with sales measures.

---

## Running the Project

### 1. Install dependencies

```bash
pip install pandas sqlalchemy psycopg2-binary
```

### 2. Create the database

```bash
psql -U postgres -f schema.sql
```

### 3. Run the ETL pipeline

```bash
python main.py
```

---

## Features

- Removes duplicate records
- Cleans and standardizes data
- Converts dates and numeric values
- Handles missing values
- Generates surrogate keys
- Implements a star schema
- Loads data into PostgreSQL

---

## Output Tables

- customers
- stores
- products
- transactions
- sales

---

## Author

**Bohlale Mohlabi**

