import glob
import re
import os
import pandas as pd
import sqlite3

# path1 = "../database_model" 
path1 = "truth_inquery" 
path2 = "../truth_inquery/output" 
path3 = "../truth_inquery/data"


### Implements the glob module to access all the csv files in specified pathname
files = glob.glob(path3 + "/*.csv")
conn = sqlite3.connect("token_states.db") 
for file_name in files:
    table_name = file_name.split('/')[-1]
    df = pd.read_csv(file_name)
    df.to_sql(table_name, conn, if_exists='append', index=False)
conn.close()

def concat_files(path, index): 
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
    files = glob.glob(path + "/*.csv")
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename, index_col = index)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

def concat_files1(): 
    """
    A function to assist in CPC source database creation by first concatenating 
    all the csv format files in a given path and then converting it to a dataframe.
    Input:
        index -
        A path to the folder in the directory that stores the required csv files
        path -
        An integer that serves as the index of the datafram
    Returns:
        A compiled dataframe of all the csv files
    """
    filesCPC = []
    for fname in os.listdir(path=path3):
        if re.match("CPC_.*\.csv", fname):
            filesCPC.append(path3 + "/" + fname)
    dataframe = pd.DataFrame()
    content = []
    for filename in filesCPC:
        df = pd.read_csv(filename, index_col = 3)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

def concat_files2(): 
    """
    A function to assist in HPC source database creation by first concatenating 
    all the csv format files in a given path and then converting it to a dataframe.
    Input:
        index -
        A path to the folder in the directory that stores the required csv files
        path -
        An integer that serves as the index of the datafram
    Returns:
        A compiled dataframe of all the csv files
    """
    filesHPC = []
    for fname in os.listdir(path=path3):
        if re.match("HPC_.*\.csv", fname):
            filesHPC.append(path3 + "/" + fname)
    dataframe = pd.DataFrame()
    content = []
    for filename in filesHPC:
        df = pd.read_csv(filename)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame





### Implements the respective functions for API, CPC and HPC to convert them to 
### sqlite3 databases

cpc_clinics = concat_files1()
hpc_clinics = concat_files2()
API_data = concat_files(path1, 0)

conn1 = sqlite3.connect("api.db")
API_data.to_sql(name="API", con = conn1)
conn.close()

conn2 = sqlite3.connect("CPC_clinics.db")
cpc_clinics.to_sql(name="CPC_clinics", con = conn2)
conn.close()

conn3 = sqlite3.connect("HPC_clinics.db")
hpc_clinics.to_sql(name="HPC_clinics", con = conn3)
conn.close()


