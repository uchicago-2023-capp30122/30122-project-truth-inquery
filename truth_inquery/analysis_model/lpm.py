# Matt Ryan
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import pandas
from truth_inquery.analysis_model.dataframe_cleaner import *
from truth_inquery.analysis_model.states import *
import sys



def analyze(keyword):
    '''
    Given a keyword that you're interested in examining in terms of its frequency across fake and real aboriton clinics across all 50 states 
    with some controls for state to state variation, this analyze function constructs the relevant dataframes from web crawled and api data sources, 
    executes a linear regression (linear probability model or LPM), prints a table of the coefficients associated with each variable and the mean squared
    error of the regression for that keyword.  

    Inputs:
        - keyword, a string of interest to use in comparing real to fake clinic websites. 
    
    Prints a table with coefficients, the keyword, and mean squared error for the regression

    Returns the mean squared error and constructed dataframe that the regression was run on containing real and fake clinic urls, state policy data, 
    and the frequency the given keyword appears across each website. 

    '''

    # keyword = "ultrasound"
    assert isinstance (keyword, str), "keyword should be a python string type variable and needs to be entered with single ' '  or double " " quotations"

    fake_clinics_df = fake_clinic_db_to_df('CPC_clinics.db')

    real_clinics_df = real_clinic_db_to_df('HPC_clinics.db')

    all_clinics_df = pandas.concat([fake_clinics_df, real_clinics_df], ignore_index = True, axis = 0)

    policy_df = policyapi_db_to_df('api.db')

    clinic_and_policy_df = pandas.merge(all_clinics_df, policy_df, on = "state", how = "inner")

    keyword_across_states_df = fake_and_real_keyword_across_states(keyword)
    keyword_across_states_df = keyword_across_states_df.reset_index()
    keyword_across_states_df = keyword_across_states_df.rename(columns = {"index" : "url"})
    # here was the change
    keyword_across_states_df_missing_values_map = {str(keyword) : 0}
    keyword_across_states_df = keyword_across_states_df.fillna(value = keyword_across_states_df_missing_values_map)

    clinic_policy_keyword_count_df = pandas.merge(clinic_and_policy_df, keyword_across_states_df, on = "url", how = "inner")

    x_df = clinic_policy_keyword_count_df[[keyword, "waiting_period_hours", "counseling_visits", "banned_after_weeks_since_LMP"]]

    y_df = clinic_policy_keyword_count_df[["IV"]]

    X_train, X_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=25)

    reg = LinearRegression().fit(X_train, y_train)

    y_pred = reg.predict(X_train)
    mse = mean_squared_error(y_train, y_pred)
    y_pred_test = reg.predict(X_test)
    waiting_period_coef = reg.coef_[0][0]
    counseling_visits_coef = reg.coef_[0][1]
    banned_after_weeks_since_LMP_coef = reg.coef_[0][2]
    keyword_coef = reg.coef_[0][3] 


    # Display output
    space = " "
    s10 = space*10
    print("---------------Coefficients---------------\n waiting period | counseling visits | abortion banned(wks) |"+" "+keyword+" \n", reg.coef_)
    print(space*40, "---------------Coefficients---------------\n","waiting period", s10, "|", s10, "counseling_visits", s10, "|", s10, "abortion banned(wks)", s10,  "|", s10, keyword)
    print() 
    print(waiting_period_coef, s10, counseling_visits_coef, s10, banned_after_weeks_since_LMP_coef, s10, keyword_coef)
    print()
    print("-----mean squared error-----\n", mse)
    print("a lower mean squared error between selected keywords indicates that the selected keyword is a better predictor of a fake clinic website")

    return mse, clinic_policy_keyword_count_df
