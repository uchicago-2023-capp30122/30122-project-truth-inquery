# #poetry run python database.py #run from database_model directoy
import glob
import re
import os
import pandas as pd
import sqlite3

path1 = "../database_model" #API path truth_inquery/database_model/gestational_limits.csv
path2 = "../output" 
path3 = "../data" 

def concat_files(): 
    """
    A function to assist in API source database creation by first concatenating 
    all the csv format files in a given path and then converting it to a dataframe
    Input:
        index -
        A path to the folder in the directory that stores the required csv files
        path -
        An integer that serves as the index of the datafram
    Returns:
        A compiled dataframe of all the csv files
    """
    files = glob.glob(path1 + "/*.csv")
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

def concat_files1(type): #helper
    """
    """
    files = []
    for fname in os.listdir(path=path3):
        if re.match(f"{type}_.*\.csv", fname):
            files.append(path3 + "/" + fname)
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

# def clinic_related_db(type, title):
#     """
#     A function to assist in statewise clinic level database creation by 
#     converting all the csv format files in a given path to a dataframe
#     Input:
#         type -
#         A string indicating the category of clinic type i.e. CPC or HPC 
#         title -
#         A string indicating the table name for each state in the database
#     Returns:
#         A database for the category with a table for each state 
#     """
#     conn = sqlite3.connect(f"{title}.db")
#     files = []
#     for fname in os.listdir(path3):
#         if re.match(f"{type}_.*\.csv", fname):
#             files.append(path3 + "/" + fname) 
#     for file_name in files:
#         if type == "CPC":
#             table_name = file_name[file_name.find('(')+1:file_name.find(')')]
#         else:
#             table_name = file_name.split('_')[-1].split(".")[0]
#         df = pd.read_csv(file_name)
#         df.to_sql(table_name, conn, if_exists='append', index=False)
#     conn.close()


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
    conn1 = sqlite3.connect(f"{title}.db")
    files_toks = []
    for fname in os.listdir(path2):
        if re.match(f"{type}_.*\.csv", fname):
            files_toks.append(path2 + "/" + fname) 
    for file_name in files_toks:
        table_name = file_name.split('_')[-2].split(".")[0]
        df = pd.read_csv(file_name)
        df.to_sql(table_name, conn1, if_exists='append', index=False)
    conn1.close()

# ### Implements/calls the respective functions for API, CPC and HPC to convert them to 
# ### sqlite3 databases
API_data = concat_files()
conn = sqlite3.connect("api.db")
API_data.to_sql(name="API", con = conn)
conn.close()

cpc_clinics = concat_files1("CPC")
hpc_clinics = concat_files1("HPC")

conn2 = sqlite3.connect("CPC_clinics.db")
cpc_clinics.to_sql(name="CPC_clinics", con = conn2)
conn.close()

conn3 = sqlite3.connect("HPC_clinics.db")
hpc_clinics.to_sql(name="HPC_clinics", con = conn3)
conn.close()

# CPC_clinics = clinic_related_db("CPC", "CPC_clinics")
# HPC_clinics = clinic_related_db("HPC", "HPC_clinics")

Tokens_CPC_clinics = token_related_db("CPC", "Tokens_CPC_clinics")
Tokens_HPC_clinics = token_related_db("HPC", "Tokens_HPC_clinics")



