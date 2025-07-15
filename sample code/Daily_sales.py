# daily_sales_pipeline.py
import os
import math
import pandas as pd
import logging
import mysql.connector

FILE_PATH = r"C:\Users\Muskaan.Thawani\Desktop\Patient_Square\supermarket_sales.csv"
CLEANED_PATH = r"C:\Users\Muskaan.Thawani\Desktop\Patient_Square\cleaned_sales.csv"

def download_data(**kwargs):
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError("Sales file not found.")
    kwargs['ti'].xcom_push(key='file_path', value=FILE_PATH)
    print("File downloaded.")

def validate_file(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path')
    df = pd.read_csv(file_path)
    is_empty = df.empty
    kwargs['ti'].xcom_push(key='is_empty', value=is_empty)
    print(f"File validation complete: Empty={is_empty}")

def transform_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='file_path')
    df = pd.read_csv(file_path)

    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M', errors='coerce').dt.time
    df.dropna(inplace=True)
    df.rename(columns={'Invoice ID': 'Invoice_id'}, inplace=True)
    df['bracket'] = df['cogs'].apply(lambda cogs: f"{math.floor(cogs / 50) * 50}-{(math.floor(cogs / 50) + 1) * 50}")
    df.to_csv(CLEANED_PATH, index=False)

    kwargs['ti'].xcom_push(key='cleaned_path', value=CLEANED_PATH)
    print("Data transformed.")

def load_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(key='cleaned_path')
    df = pd.read_csv(file_path)

    conn = mysql.connector.connect(
        host='localhost',
        dbname='sales',
        user='postgres',
        password='Admin1',
        port=3306
    )
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO daily_sales (
                Invoice_id, Store, City, Customer_type, Gender, Product_line,
                Unit_price, Quantity, Tax_5_percent, Total, Date, Time, Payment,
                cogs, gross_margin_percentage, gross_income, Rating, bracket
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            row['Invoice_id'], row['Store'], row['City'], row['Customer type'], row['Gender'],
            row['Product line'], row['Unit price'], row['Quantity'], row['Tax 5%'], row['Total'],
            row['Date'], row['Time'], row['Payment'], row['cogs'], row['gross margin percentage'],
            row['gross income'], row['Rating'], row['bracket']
        ))
    conn.commit()
    cur.close()
    conn.close()
    print("Data loaded to PostgreSQL.")
