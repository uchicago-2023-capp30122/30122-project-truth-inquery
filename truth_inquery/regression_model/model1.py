from sklearn import linear_model
import pandas
import sqlite3
import json
import states

# 
# Step 1: correct schema difference between CPC and HPC clinic data 

# I think this is kind of taken care of w the changes to the scraper

# Step 2: create a dataframe from the CPC clinics

cpc_connection = sqlite3.connect('../database_model/CPC_clinics.db')

cpc_query= 'SELECT IV, Website, State FROM CPC_Clinics'

cpc_dataframe = pandas.read_sql_query(cpc_query, cpc_connection)
cpc_dataframe = cpc_dataframe.rename(columns = {"Website" : "url", "State" : "state"})
cpc_dataframe["state"] = cpc_dataframe["state"].str[-4:]
cpc_dataframe["state"] = cpc_dataframe["state"].str.replace(r'[()]',"", regex = True)

print()
print(cpc_dataframe.head())
print()

# I'm not sure if this is necessary, seems like it might be good practice from what I've been able to read. !!! Ask about this
cpc_connection.close()


# Step 3: add dummy variable 'is_cpc' to the cpc_dataframe and initialize it to 1 for all rows
# Dema did this in pre-processing


# Step 4: Once step 1 is complete, create a dataframe from the HPC clinics

hpc_connection = sqlite3.connect('../database_model/HPC_clinics.db')

hpc_query = 'SELECT IV, url, state FROM HPC_clinics'

hpc_dataframe = pandas.read_sql_query(hpc_query, hpc_connection)
print()
print(hpc_dataframe.head())
print()

hpc_connection.close()

# Step 5: add dummy variable 'is_cpc' to the hpc_dataframe adn initialize it to 0 for all rows

# Dema did this

# Step 6: Combine the cpc_dataframe and hpc_dataframe one on top of another. Cascade? We can call this combined_clinic_dataframe

cpc_and_hpc_dataframe = pandas.concat([cpc_dataframe, hpc_dataframe], ignore_index = True, axis = 0)
print()
print(cpc_and_hpc_dataframe.head())
print()



# Step 7: putting API data into a dataframe

policyapi_connection = sqlite3.connect('../database_model/api.db')

policyapi_cursor = policyapi_connection.cursor()

policyapi_query = "SELECT * FROM API"

policyapi_dataframe = pandas.read_sql_query(policyapi_query, policyapi_connection)

# transpose the dataframe and adjust the index header so that policy features like 
# "waiting_period_hours" are the columns, and mapping the full state names onto
# abbreviated state names
policyapi_dataframe = policyapi_dataframe.T
policyapi_dataframe = policyapi_dataframe[1:]
policyapi_dataframe.columns = policyapi_dataframe.iloc[0]
policyapi_dataframe = policyapi_dataframe.iloc[1:]
policyapi_dataframe.index = policyapi_dataframe.index.map(states.name_to_abbrev)
policyapi_dataframe["state"] = policyapi_dataframe.index

print()
print(policyapi_dataframe)
print()

policyapi_dataframe

print()
print(policyapi_dataframe)
print()





# policyapi_connection.close()


# Step 8: join api_dataframe to combined_clinic_dateframe . . . on state

cpchpcpolicy_df = pandas.merge(cpc_and_hpc_dataframe, policyapi_dataframe, on = "state", how = "inner")

print()
print(cpchpcpolicy_df)
print()





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


# cpc_state_list = ['AK',  'CA',  'CT',  'DE',  'GA',  'IA',  'IN',  'MA',  'ME',  'MN',  
# 'NC',  'NH',  'NM', 'NY',  'OR',  'RI',  'UT',  'VT',  'WY', 'AZ',  'CO',  'DC', 'FL',  
# 'HI',  'IL',  'KS',  'MD',  'MI',  'MT',  'NE',  'NJ',  'NV', 'OH',  'PA',  'SC',  'VA',  'WA']


#print(cpc_state_list)


# this won't work with the current "by state" implementation. Maybe there is a workaround if I use a loop through the tables?
# currently it's written for the previous implementation - not 'by state'
# cpc_query = 'SELECT * FROM CPC_clinics'
# def abc(one, two, table):
#     """
#     """
#     cpc_query = f"SELECT {}, {} FROM {}" 


# for state in cpc_state_list:
#     sample = abc(state, website, state)
#     print(sample)


#   "Kentucky" TEXT,
#   "Oklahoma" TEXT,
#   "Kansas" TEXT,
#   "Ohio" TEXT,
#   "Arizona" TEXT,
#   "Mississippi" TEXT,
#   "Tennessee" TEXT,
#   "Wisconsin" TEXT,
#   "Alabama" TEXT,
#   "Missouri" TEXT,
#   "Idaho" TEXT,
#   "Virginia" TEXT,
#   "Pennsylvania" TEXT,
#   "Florida" TEXT,
#   "South Carolina" TEXT,
#   "Georgia" TEXT,
#   "Minnesota" TEXT,
#   "Indiana" TEXT,
#   "Utah" TEXT,
#   "North Carolina" TEXT,
#   "Arkansas" TEXT,
#   "North Dakota" TEXT,
#   "Iowa" TEXT,
#   "Michigan" REAL,
#   "Nebraska" TEXT,
#   "South Dakota" TEXT,
#   "Louisiana" TEXT,
#   "Texas" TEXT,
#   "Alaska" TEXT,
#   "West Virginia" TEXT,
#   "Nevada" TEXT,
#   "New Hampshire" TEXT,
#   "Maine" TEXT,
#   "New York" TEXT,
#   "Wyoming"
#   "Oregon"
#   "California"
#   "New Jersey"
#   "Massachusetts"
#   "Delaware" 
#   "Maryland" 
#   "Connecticut" 
#   "Colorado" 
#   "District of Columbia" 
#   "Vermont" 
#   "Illinois"
#   "New Mexico"
#   "Montana"
#   "Hawaii"
#   "Rhode Island"
#   "Washington"