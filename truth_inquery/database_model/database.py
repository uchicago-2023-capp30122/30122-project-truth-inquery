#poetry run python database.py 
#run from database_model directoy
import glob
import re
import os
import pandas as pd
import sqlite3


path1 = "../database_model" 
path2 = "../output" 
path3 = "../data" 


def concat_files(): 
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


def concat_clinic_files(type, title): 
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
concat_files()

concat_clinic_files("CPC", "CPC_clinics")
concat_clinic_files("HPC", "HPC_clinics")

token_related_db("CPC", "Tokens_CPC_clinics")
token_related_db("HPC", "Tokens_HPC_clinics")




