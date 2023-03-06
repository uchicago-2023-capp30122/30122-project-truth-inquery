# Matt Ryan
import sqlite3
import pandas
# need to uncomment this line to keep it running from top evel
# from analysis_model import states
# import states

def fake_clinic_db_to_df(fake_clinic_db_path):
    '''
    Generates and cleans a dataframe of fake clinics for analysis. 

    Inputs:
        - path to the fake clinic (CPC) database

    Returns: fake_clinic_df filtered to relevant columns and with state normalized
    to the 2-letter abbreviation
    '''


    cpc_connection = sqlite3.connect(fake_clinic_db_path)
    cpc_query= 'SELECT IV, Website, State FROM CPC_Clinics'

    cpc_dataframe = pandas.read_sql_query(cpc_query, cpc_connection)
    cpc_dataframe = cpc_dataframe.rename(columns = {"Website" : "url", "State" : "state"})

    # these lines normalize the data contained in the "state" field
    cpc_dataframe["state"] = cpc_dataframe["state"].str[-4:]
    cpc_dataframe["state"] = cpc_dataframe["state"].str.replace(r'[()]',"", regex = True)

    cpc_connection.close()

    return cpc_dataframe

def real_clinic_db_to_df(real_clinic_db_path):
    '''
    Generates and cleans a dataframe of real clinics for analysis. 

    Inputs:
        - path to the real clinic (HPC) database

    Returns: real_clinic_df filtered to relevant columns with state normalized to the 
    two letter abbreviation
    '''

    hpc_connection = sqlite3.connect(real_clinic_db_path)

    hpc_query = 'SELECT IV, url, state FROM HPC_clinics'

    hpc_dataframe = pandas.read_sql_query(hpc_query, hpc_connection)

    hpc_connection.close()

    return hpc_dataframe
    


def policyapi_db_to_df(api_db_path):
    '''
    Generates and cleans a dataframe of policy data from abortionpolicyapi.com for analysis
    the policy api has information on gestational limits, waiting periods, 
    medical insurance, and policies related to minors seeking abortion care. 

    Inputs:
        - path to the policyapi database

    Returns: real_clinic_df filtered to relevant columns with state normalized to the 
    two letter abbreviation
    '''

    policyapi_connection = sqlite3.connect(api_db_path)

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
    policyapi_fill_missing_values_map = {"waiting_period_hours" : 0, "counseling_visits" : 0, "banned_after_weeks_since_LMP" : 0}
    policyapi_dataframe = policyapi_dataframe.fillna(value = policyapi_fill_missing_values_map)

    policyapi_connection.close()

    return policyapi_dataframe

def keyword_count(state, keyword):
    '''
    Counts word frequencies for a user selected keyword 
    for each real or fake clinic in a user selected state. 

    Inputs:
        - State, in a 2 letter abbreviation please
        - a keyword you are interested in extracting use use frequency data from across fake clinic and real clinic websites
    
    Prints statements containing every clinic (real and fake) in the 
    given state along with the frequency they keyword was used for that clinics 
    '''
    # could include exception error for stuff about user not entering a valid state abbrev

    token_query = "SELECT * FROM"+ " " + state
    cpc_token_connection = sqlite3.connect("Tokens_CPC_clinics.db")
    hpc_token_connection = sqlite3.connect("Tokens_HPC_clinics.db")

    cpc_token_df = pandas.read_sql_query(token_query, cpc_token_connection)
    hpc_token_df = pandas.read_sql_query(token_query, hpc_token_connection)

    print("fake clinics using" + " " + keyword)
    for i in range(0, cpc_token_df.shape[0]):
        for j in range(1, 200):
            # would love to add a regex here to allow for fuzzy comparisons
            if cpc_token_df.iloc[i]["token"+str(j)] == keyword:
                print(cpc_token_df.iloc[i]["url"])
                print(cpc_token_df.iloc[i]["count"+str(j)])
            

    print("real clinics using" + " " + keyword)
    for i in range(0, hpc_token_df.shape[0]):
        for j in range(1, 200):
            # would love to add a regex here to allow for fuzzy comparisons
            if hpc_token_df.iloc[i]["token"+str(j)] == keyword:
                print(hpc_token_df.iloc[i]["url"])
                print(hpc_token_df.iloc[i]["count"+str(j)])

    
def keyword_count_df_generator(state, keywords):
    '''
    Counts word frequencies for a selected keyword 
    for each real or fake clinic in a selected state. 

    Inputs:
        - State, in a 2 letter abbreviation
        - a keyword you are interested in extracting use use frequency data from
    
    Returns keyword_count_df containing every clinic (real and fake) in the 
    given state along with the frequency they keyword was used for that clinics 
    '''

    urls = []
    counts = []

    token_query = "SELECT * FROM"+ " " + state
    cpc_token_connection = sqlite3.connect("Tokens_CPC_clinics.db")
    hpc_token_connection = sqlite3.connect("Tokens_HPC_clinics.db")

    cpc_token_df = pandas.read_sql_query(token_query, cpc_token_connection)
    hpc_token_df = pandas.read_sql_query(token_query, hpc_token_connection)

    for i in range(0, cpc_token_df.shape[0]):
        for j in range(1, 200):
            for keyword in keywords:
                if cpc_token_df.iloc[i]["token"+str(j)] == keyword:
                    urls.append(cpc_token_df.iloc[i]["url"])
                    counts.append(cpc_token_df.iloc[i]["count"+str(j)])
    
    fake_clinics_tokens_df = pandas.DataFrame(counts, columns = keywords, index = urls)


    return fake_clinics_tokens_df