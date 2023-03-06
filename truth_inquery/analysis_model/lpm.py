# Matt Ryan
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import pandas
# from dataframe_cleaner import *
from analysis_model.dataframe_cleaner import *

# I think i need to add this line to help get everything working w one command
# if __name__ == "__main__":

def analyze():

    fake_clinics_df = fake_clinic_db_to_df('CPC_clinics.db')

    real_clinics_df = real_clinic_db_to_df('HPC_clinics.db')

    all_clinics_df = pandas.concat([fake_clinics_df, real_clinics_df], ignore_index = True, axis = 0)

    policy_df = policyapi_db_to_df('api.db')

    clinic_and_policy_df = pandas.merge(all_clinics_df, policy_df, on = "state", how = "inner")

    x_df = clinic_and_policy_df[["waiting_period_hours", "counseling_visits", "banned_after_weeks_since_LMP"]]

    y_df = clinic_and_policy_df[["IV"]]

    X_train, X_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=25)

    reg = LinearRegression().fit(X_train, y_train)

    y_pred = reg.predict(X_train)
    mse = mean_squared_error(y_train, y_pred)
    y_pred_test = reg.predict(X_test)


    # The coefficients - i can make this into a nicely printed table
    print("Coefficients: \n", reg.coef_)
    print(mse)


# analyze()

# I'd like to plot at least some things. 
# plt.scatter(X_train["waiting_period_hours"], y_train)
# plt.plot(X_train["waiting_period_hours"], y_pred, color="red")
# plt.xlabel("x")
# plt.ylabel("y")
# plt.savefig("/home/mattryan/programmingturk/FinalProject/truth_inquery/analysis_model/plot.png")
# plt.show()








## remove - just in case


#create a dataframe from the CPC clinics

# cpc_connection = sqlite3.connect('CPC_clinics.db')

# cpc_query= 'SELECT IV, Website, State FROM CPC_Clinics'

# cpc_dataframe = pandas.read_sql_query(cpc_query, cpc_connection)
# cpc_dataframe = cpc_dataframe.rename(columns = {"Website" : "url", "State" : "state"})
# cpc_dataframe["state"] = cpc_dataframe["state"].str[-4:]
# cpc_dataframe["state"] = cpc_dataframe["state"].str.replace(r'[()]',"", regex = True)

# print()
# print(cpc_dataframe.head())
# print()

# I'm not sure if this is necessary, seems like it might be good practice from what I've been able to read. !!! Ask about this
# cpc_connection.close()


# Step 3: add dummy variable 'is_cpc' to the cpc_dataframe and initialize it to 1 for all rows
# Dema did this in pre-processing


# Step 4: Once step 1 is complete, create a dataframe from the HPC clinics

# hpc_connection = sqlite3.connect('HPC_clinics.db')

# hpc_query = 'SELECT IV, url, state FROM HPC_clinics'

# hpc_dataframe = pandas.read_sql_query(hpc_query, hpc_connection)
# print()
# print(hpc_dataframe.head())
# print()

# hpc_connection.close()

# Step 5: add dummy variable 'is_cpc' to the hpc_dataframe adn initialize it to 0 for all rows

# Dema did this

# Step 6: Combine the cpc_dataframe and hpc_dataframe one on top of another. Cascade? We can call this combined_clinic_dataframe

# cpc_and_hpc_dataframe = pandas.concat([cpc_dataframe, hpc_dataframe], ignore_index = True, axis = 0)

# print()
# print(cpc_and_hpc_dataframe.head())
# print()



# Step 7: putting API data into a dataframe

# policyapi_connection = sqlite3.connect('api.db')

# policyapi_cursor = policyapi_connection.cursor()

# policyapi_query = "SELECT * FROM API"

# policyapi_dataframe = pandas.read_sql_query(policyapi_query, policyapi_connection)

# # transpose the dataframe and adjust the index header so that policy features like 
# # "waiting_period_hours" are the columns, and mapping the full state names onto
# # abbreviated state names
# policyapi_dataframe = policyapi_dataframe.T
# policyapi_dataframe = policyapi_dataframe[1:]
# policyapi_dataframe.columns = policyapi_dataframe.iloc[0]
# policyapi_dataframe = policyapi_dataframe.iloc[1:]
# policyapi_dataframe.index = policyapi_dataframe.index.map(states.name_to_abbrev)
# policyapi_dataframe["state"] = policyapi_dataframe.index
# policyapi_fill_missing_values_map = {"waiting_period_hours" : 0, "counseling_visits" : 0, "banned_after_weeks_since_LMP" : 0}
# policyapi_dataframe = policyapi_dataframe.fillna(value = policyapi_fill_missing_values_map)

# print()
# print(policyapi_dataframe)
# print()

# policyapi_dataframe

# print()
# print(policyapi_dataframe)
# print()





# policyapi_connection.close()


# Step 8: join api_dataframe to combined_clinic_dateframe . . . on state

# cpchpcpolicy_df = pandas.merge(cpc_and_hpc_dataframe, policyapi_dataframe, on = "state", how = "inner")

# print("here is the joined dataframe for the regression")
# print(cpchpcpolicy_df)
# print()





# Step 9: select all input variables (api legal status, top 10 token counts, etc) into a single dataframe X

# x_df = cpchpcpolicy_df[["waiting_period_hours", "counseling_visits", "banned_after_weeks_since_LMP"]]

# print("this is the x dataframe")
# print(type(x_df["waiting_period_hours"]))
# print(x_df.iloc[200:250])
# print(x_df.shape)

# Step 10: select all outcome variables, in this case (is_cpc) into a single dataframe y

# y_df = cpchpcpolicy_df[["IV"]]
# print(y_df)
# print(x_df.shape)
# print(y_df.shape)

# # Step 11: Split the finalized flattened and joined combined_and_joined_clinic_dataframe into train and test subsets using sklearn

# X_train, X_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=25)

# print("shape check")
# print(X_train.shape)

# # print(X_test.shape)
# # print(y_train.shape)
# # print(y_test.shape)

# reg = LinearRegression().fit(X_train, y_train)

# # logistic regression may be better suited

# y_pred = reg.predict(X_train)
# # mean_squared_error(y_train, y_pred)
# print("LOOK AT THIS")
# print(X_train.shape)
# print(y_pred.shape)

# y_pred_test = reg.predict(X_test)



# print(y_pred.shape)

# # The coefficients
# print("Coefficients: \n", reg.coef_)

# The mean squared error
# print("Mean squared error: %.2f" % mean_squared_error(y_test, y_pred))


# The coefficient of determination: 1 is perfect prediction
# print("Coefficient of determination: %.2f" % r2_score(y_test, y_pred))



# # ploting worked as long as I did one column of the combined df vs y_train

# plt.scatter(X_train["waiting_period_hours"], y_train)
# plt.plot(X_train["waiting_period_hours"], y_pred, color="red")
# plt.xlabel("x")
# plt.ylabel("y")
# plt.savefig("/home/mattryan/programmingturk/FinalProject/truth_inquery/analysis_model/plot.png")
# plt.show()



# Step 12: build the regression model from the training subset
# from sklearn.linear_model import LinearRegression


# Step 13: test the regression model on the test subset