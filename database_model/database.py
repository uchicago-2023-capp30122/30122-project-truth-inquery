import glob
import sqlite3
import pandas as pd

path1 = "./database_model/API_data"
path2 = "./truth_inquery/output"
path3 = "./truth_inquery/data"

def concat_files(path): #helper function 
    """
    """
    files = glob.glob(path + "/*.csv")
    dataframe = pd.DataFrame()
    content = []
    for filename in files:
        df = pd.read_csv(filename, index_col = None)
        content.append(df)
    data_frame = pd.concat(content)
    return data_frame

clinics = concat_files(path3)

# # connection = sqlite3.connect("AlabamaTokens.db")
# # df.to_sql("token_AL", connection, if_exists = "replace")
# # connection.close()


# file_names = ["/API\ data/",
#                 "/Users/dematherese/capp30122/30122-project-truth-inquery/project/output/Alabama tokens.csv",
#                 "/Users/dematherese/capp30122/30122-project-truth-inquery/project/output/Colorado tokens.csv",
#                 "/Users/dematherese/capp30122/database_model/API data/insurance_coverage_table.csv",
#                 "/Users/dematherese/capp30122/database_model/API data/gestational_limits_table.csv"]
# conn = sqlite3.connect("token_states.db") 
# for file_name in file_names:
#     table_name = file_name.split('.')[0]
#     df = pd.read_csv(file_name)
#     df.to_sql(table_name, conn, if_exists='append', index=False)
# conn.close()




# # with open("/Users/dematherese/capp30122/30122-project-truth-inquery/project/output/Alabama tokens.csv") as f:
# #     d = csv.DictReader(f)
# #     s = [(i[""], i["Total"]) for i in d]
# #     print(s)

# # sqliteConnection = sqlite3.connect('sql.db')
# # cursor = sqliteConnection.cursor()

# # cursor.execute('create table token(name varchar2(10), Total float);')
# # cursor.executemany(
# #         "insert into token("", Total) VALUES (?, ?);",)
# # cursor.execute('select * from token;')
# # result = cursor.fetchall()
# # print(result)