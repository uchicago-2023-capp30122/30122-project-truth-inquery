# Matt Ryan
import sqlite3
import pandas
from analysis_model import states

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

# def keyword_count_df(state, keyword):
#     '''
#     Generates a dataframe of word counts for a user selected keyword 
#     for each real or fake clinic in a user selected state. 

#     Inputs:
#         - State, in a 2 letter abbreviation please
#         - a keyword
    
#     Returns keyword_count_df containing every clinic (real and fake) in the 
#     given state along with the frequency they keyword was used 
#     '''

#     token_query = "SELECT * FROM "
