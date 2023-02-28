from sklearn import linear_model
import pandas
import sqlite3

# Step 1: correct schema difference between CPC and HPC clinic data


# Step 2: create a dataframe from the CPC clinics

# is this the correct path for our final directory structure?
cpc_connection = sqlite3.connect('truth_inquery/database_model/CPC_clinics.db')
# this won't work with the current "by state" implementation. Maybe there is a workaround if I use a loop through the tables?
# currently it's written for the previous implementation - not 'by state'
cpc_query = 'SELECT * FROM CPC_clinics'

cpc_dataframe = pandas.read_sql_query(cpc_query, cpc_connection)

# close the database connection?
cpc_connection.close()

# Step 3: add dummy variable 'is_cpc' to the cpc_dataframe and initialize it to 1 for all rows


# Step 4: Once step 1 is complete, create a dataframe from the HPC clinics

hpc_connection = sqlite3.connect('truth_inquery/database_model/HPC_clinics.db')

hpc_query = 'SELECT * FROM HPC_clinics'

hpc_dataframe = pandas.read_sql_query(hpc_query, hpc_connection)

# close the db connection?
hpc_connection.close()

# Step 5: add dummy variable 'is_cpc' to the hpc_dataframe adn initialize it to 0 for all rows



# Step 6: Combine the cpc_dataframe and hpc_dataframe one on top of another. Cascade? We can call this combined_clinic_dataframe



# Step 7: putting API data into a dataframe . . . hmmm. . each row would be a state?



# Step 8: join api_dataframe to combined_clinic_dateframe . . . on state?


# Step 9: select all input variables (api legal status, top 10 token counts, etc) into a single dataframe X


# Step 10: select all outcome variables, in this case (is_cpc) into a single dataframe y


# Step 11: Split the finalized flattened and joined combined_and_joined_clinic_dataframe into train and test subsets using sklearn
# from sklearn.metrics import mean_squared_error
# y_pred = reg.predict(X_test)
# mean_squared_error(y_test, y_pred)


# Step 12: build the regression model from the training subset
# from sklearn.linear_model import LinearRegression
# reg = LinearRegression().fit(X_train, y_train)

# Step 13: test the regression model on the test subset








