from datetime import datetime, timedelta
import os
import shutil
import pandas as pd
import math
import logging
import psycopg2
# from airflow import DAG
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.utils.dates import days_ago
# from airflow import DAG


def copy_file_to_folder(file_path, dest_folder):
    """
    Copies a file from file_path to the destination_folder.
    
    Args:
        file_path (str): Path to the source file.
        destination_folder (str): Path to the destination folder.
    
    Returns:
        str: Full path of the copied file.
    """
    # Check if file exists
    if not os.path.isfile(file_path):
        return f"File does not exist: {file_path}"
    
    # Check if file is empty
    if os.path.getsize(file_path) == 0:
        return f"File is empty and was not copied: {file_path}"
    
    # Ensure destination folder exists
    os.makedirs(dest_folder, exist_ok=True)
    
    # Create the full destination path
    destination_path = os.path.join(dest_folder, os.path.basename(file_path))
    
    # Copy the file
    shutil.copy(file_path, destination_path)
    
    print(f"File successfully copied to: {destination_path}")
    return destination_path

file_path = r"C:\Users\Muskaan.Thawani\Desktop\Patient_Square\supermarket_sales.csv"
dest_folder = r"C:\Users\Muskaan.Thawani\Desktop\Patient_Square\Assignment 1 - Sales\Dataset"

dest_path = copy_file_to_folder(file_path=file_path, dest_folder=dest_folder)
