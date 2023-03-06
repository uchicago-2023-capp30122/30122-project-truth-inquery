#Dema Therese
#Creates five databases:
# 1. api.db - API data sourced from https://www.abortionpolicyapi.com
# 2. CPC_clinics.db - CPC clinic data coded and saved by Aaron Haefner in data subdirectory
# 3. HPC_clinics.db - HPC clinic crawled by Aaron Haefner and saved in data subdirectory
# 4. Tokens_CPC_clinics.db - Token files created by Aaron Haefner for CPCs statewise
# 5. Tokens_HPC_clinics.db - Token files created by Aaron Haefner for HPCs statewise

#poetry run python truth_inquery/database_model/database.py

import glob
import re
import os
import pandas as pd
import sqlite3

path1 = "../30122-project-truth-inquery/truth_inquery/database_model"
path2 = "../30122-project-truth-inquery/truth_inquery/output" 
path3 = "../30122-project-truth-inquery/truth_inquery/data" 

def api_db(): 
    """
    A function to assist in API source database creation by first concatenating 
    all the csv format files in a given path and then converting it to a dataframe
    Returns:
        A compiled dataframe of all the csv files in a database
    """
    conn = sqlite3.connect("api.db")
    files = glob.glob(path1 + "/*.csv")
    content = []
    for filename in files:
        df = pd.read_csv(filename)
        content.append(df)
    df = pd.concat(content)
    df.to_sql(name = "API", con = conn)
    return conn.close()


def concat_clinic_db(type, title): 
    """
    A function to assist in clinic level database creation by 
    converting all the csv format files in a given path to a dataframe
    Input:
        type -
        A string indicating the category of clinic type i.e. CPC or HPC 
        title -
        A string indicating the table name for each category in the database
    Returns:
        A database for the category with a table for each category
    """
    conn = sqlite3.connect(f"{title}.db")
    files = []
    for fname in os.listdir(path=path3):
        if re.match(f"{type}_.*\.csv", fname):
            files.append(path3 + "/" + fname)
    content = []
    for filename in files:
        df = pd.read_csv(filename)
        if type == "CPC":
            df["IV"] = 1
        else:
            df["IV"] = 0
        content.append(df)
    df = pd.concat(content)
    df.to_sql(name = f"{title}", con = conn)
    return conn.close()


def token_related_db(type, title):
    """
    A function to assist in tokens of clinic categories database creation by 
    converting all the csv format files in a given path to a dataframe
    Input:
        type -
        A string indicating the category of clinic type i.e. CPC or HPC 
        title -
        A string indicating the table name for each state in the database
    Returns:
        A database for the category with a table for each state 
    """
    conn = sqlite3.connect(f"{title}.db")
    files_toks = []
    for fname in os.listdir(path2):
        if re.match(f"{type}_.*\.csv", fname):
            files_toks.append(path2 + "/" + fname) 
    for file_name in files_toks:
        table_name = file_name.split('_')[-2].split(".")[0]
        df = pd.read_csv(file_name)
        df.to_sql(table_name, conn, if_exists = 'append', index = False)
    conn.close()


# Implements/calls the respective functions for API, CPC and HPC to convert them to 
# sqlite3 databases
if __name__ == "__main__":
    api_db()
    print("api database created")

    concat_clinic_db("CPC", "CPC_clinics")
    concat_clinic_db("HPC", "HPC_clinics")
    print("CPC_clinics and HPC_clinics databases created")

    token_related_db("CPC", "Tokens_CPC_clinics")
    token_related_db("HPC", "Tokens_HPC_clinics")
    print("Tokens_CPC_clinics and Tokens_HPC_clinics databases created")




