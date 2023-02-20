import glob
import re
import os
import pandas as pd
import sqlite3

path1 = "../database_model"
path2 = "../truth_inquery/output" 
path3 = "../truth_inquery/data"

files = glob.glob(path3 + "/*.csv")
conn = sqlite3.connect("token_states.db") 
for file_name in files:
    table_name = file_name.split('/')[-1]
    df = pd.read_csv(file_name)
    df.to_sql(table_name, conn, if_exists='append', index=False)
conn.close()

def concat_files(path, index): #helper
    """
    """
    files = glob.glob(path + "/*.csv")
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename, index_col = index)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

def concat_files1(): #helper
    """
    """
    files = []
    for fname in os.listdir(path=path3):
        if re.match("CPC_.*\.csv", fname):
         files.append(fname)
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename, index_col = 3)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

def concat_files2(): #helper
    """
    """
    files = []
    for fname in os.listdir(path=path3):
        if re.match("HPC_.*\.csv", fname):
         files.append(fname)
    # files = glob.glob(path + re.match("/HPC_.*\.csv"))
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename, index_col = index)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

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
cpc_linics.to_sql(name="HPC_clinics", con = conn2)
conn.close()


