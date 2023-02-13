import sqlite3
# import csv
import pandas as pd

df = pd.read_csv("/Users/dematherese/capp30122/30122-project-truth-inquery/project/output/Alabama tokens.csv")

connection = sqlite3.connect("AlabamaTokens.db")
df.to_sql("token_AL", connection, if_exists = "replace")
connection.close()




# with open("/Users/dematherese/capp30122/30122-project-truth-inquery/project/output/Alabama tokens.csv") as f:
#     d = csv.DictReader(f)
#     s = [(i[""], i["Total"]) for i in d]
#     print(s)

# sqliteConnection = sqlite3.connect('sql.db')
# cursor = sqliteConnection.cursor()

# cursor.execute('create table token(name varchar2(10), Total float);')
# cursor.executemany(
#         "insert into token("", Total) VALUES (?, ?);",)
# cursor.execute('select * from token;')
# result = cursor.fetchall()
# print(result)