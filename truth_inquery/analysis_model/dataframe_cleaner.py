# Matt Ryan
import sqlite3
import pandas
# need to uncomment this line to keep it running from top evel
# from analysis_model import states
# import states


STATES = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

name_to_abbrev = {v: k for k, v in STATES.items()}

def fake_clinic_db_to_df(fake_clinic_db_path):
    '''
    Generates and cleans a dataframe of fake clinics for analysis. 
    Note: This can be expanded later and the selection of IV, Website, State is for normalizing
    the schema between fake and real clinics. With more development, more columsn could be added and reconciled. 

    Inputs:
        - path to the fake clinic (CPC) database

    Returns: fake_clinic_df filtered to relevant columns and with state normalized
    to the 2-letter abbreviation
    '''


    cpc_connection = sqlite3.connect(fake_clinic_db_path)
    cpc_query= 'SELECT IV, Website, State FROM CPC_Clinics'

    cpc_dataframe = pandas.read_sql_query(cpc_query, cpc_connection)
    cpc_dataframe = cpc_dataframe.rename(columns = {"Website" : "url", "State" : "state"})

    # these lines normalize the data contained in the "state" field so that state abbreviation can be used to join with 
    # state level policy data from the api
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
    policyapi_dataframe.index = policyapi_dataframe.index.map(name_to_abbrev)
    policyapi_dataframe["state"] = policyapi_dataframe.index
    policyapi_fill_missing_values_map = {"waiting_period_hours" : 0, "counseling_visits" : 0, "banned_after_weeks_since_LMP" : 0}
    policyapi_dataframe = policyapi_dataframe.fillna(value = policyapi_fill_missing_values_map)

    policyapi_connection.close()

    return policyapi_dataframe

def keyword_count(state, keyword):
    '''
    Counts word frequencies for a user selected keyword for each real or fake clinic in a user selected state. 
    The idea of this function is to be used as a sort of initial test in consultation with a policy expert to 
    explore single word choices' differences in use (frequency) between fake and real clinic websites. It's an
    initial way to continue the conversation with historians / strategists from the realm of the doctrinal / conceptual
    into the quantitative. 

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

    
def fake_clinic_keyword_count_df_generator(state, keyword):
    '''
    Counts word frequencies for a selected keyword for each FAKE clinic in a selected state. 
    Note: I kept real and fake functions separate here anticipating that in the future, it may be useful to add to these 
    dataframes separately. Compared to an if statement within one function or other design, this is more cleanly accessible 
    for future changes (to me at least). 

    Inputs:
        - State, in a 2 letter abbreviation
        - a keyword you are interested in extracting use use frequency data from
    
    Returns keyword_count_df containing every fake clinic in the 
    given state along with the frequency they keyword was used for that clinics 
    '''

    urls = []
    counts = []

    token_query = "SELECT * FROM"+ " " + state
    cpc_token_connection = sqlite3.connect("Tokens_CPC_clinics.db")

    cpc_token_df = pandas.read_sql_query(token_query, cpc_token_connection)

    # Ok, yes this is ugly, and there was likely a better way to do this from the outset, but the way we stored keyword counts initially 
    # directly enabled our visualizations across fake vs real clinics and across states, so at some point the data had to be transformed to 
    # counts by specific term or vice versa. That's what this block does: loops each row (website) in a state, then loops each of the term
    # and term count columns (which happen to be token1, count1 in our db) for the top 100 most frequent tokens per website. within these 
    # token and token count columns (which are unique for each url or row), this constructs a dataframe that standardizes a single column 
    # called the keyword (i.e.: the word "ultrasound"), and fills in the entries in that column coresponding to the number of times that 
    # token / keyword was used in the scraped data of each website. 
    for i in range(0, cpc_token_df.shape[0]):
        for j in range(1, 200):
            if cpc_token_df.iloc[i]["token"+str(j)] == keyword:
                urls.append(cpc_token_df.iloc[i]["url"])
                counts.append(cpc_token_df.iloc[i]["count"+str(j)])    
    
    fake_clinics_tokens_df = pandas.DataFrame(counts, columns = [keyword], index = urls)

    return fake_clinics_tokens_df

def real_clinic_keyword_count_df_generator(state, keyword):
    '''
    Counts word frequencies for a selected keyword 
    for each REAL clinic in a selected state. 

    Inputs:
        - State, in a 2 letter abbreviation
        - a keyword you are interested in extracting use use frequency data from
    
    Returns keyword_count_df containing every real clinic in the 
    given state along with the frequency they keyword was used for that clinics 
    '''

    urls = []
    counts = []

    token_query = "SELECT * FROM"+ " " + state
    hpc_token_connection = sqlite3.connect("Tokens_HPC_clinics.db")

    hpc_token_df = pandas.read_sql_query(token_query, hpc_token_connection)

    # This does the same thing as the previous loop in the previous function, only it does it for real clinics. I separated the two functions
    # for ease of modification later, when I expect we will want to do different things with the fake and real term count dataframes. See comment in 
    # the previous function for explanation of the loop. 
    for i in range(0, hpc_token_df.shape[0]):
        for j in range(1, 200):
            if hpc_token_df.iloc[i]["token"+str(j)] == keyword:
                urls.append(hpc_token_df.iloc[i]["url"])
                counts.append(hpc_token_df.iloc[i]["count"+str(j)])
        
    real_clinics_tokens_df = pandas.DataFrame(counts, columns = [keyword], index = urls)

    return real_clinics_tokens_df

def fake_and_real_keyword_across_states(keyword):
    '''
    Creates a dataframe of frequencies for a selected keyword across both real and fake clinics across all states in the data.

    Inputs:
        - a keyword you are interested in extracting use use frequency data from
    
    Returns keyword_count_df containing the frequency they keyword was used across real and fake clinics for all states in the data. 
    '''

    keyword_count_df = pandas.DataFrame()

    for state in STATES.keys():
        try:
            fake_clinic_keyword_count_df = fake_clinic_keyword_count_df_generator(state, keyword)
            real_clinic_keyword_count_df = real_clinic_keyword_count_df_generator(state, keyword)
            state_combined_keyword_count_df = pandas.concat([fake_clinic_keyword_count_df, real_clinic_keyword_count_df], axis = 0)
            keyword_count_df = pandas.concat([keyword_count_df, state_combined_keyword_count_df], axis = 0)
            print(state, "completed")
        # this for states such as AL, that don't have a table in the DB
        except:
            continue 

    return keyword_count_df

