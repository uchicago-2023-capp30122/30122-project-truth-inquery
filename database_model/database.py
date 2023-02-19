import glob
import pandas as pd
import sqlite3

path1 = "../database_model/API_data"
path2 = "../truth_inquery/output" 
path3 = "../truth_inquery/data"

files = glob.glob(path2 + "/*.csv")
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

clinics = concat_files(path3, 3)
API_data = concat_files(path1, 0)

conn1 = sqlite3.connect("api.db")
API_data.to_sql(name="API", con = conn1)
conn.close()

conn2 = sqlite3.connect("clinics.db")
clinics.to_sql(name="clinics", con = conn2)
conn.close()


