# Matt Ryan
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import pandas
from truth_inquery.analysis_model.dataframe_cleaner import *
# from analysis_model.dataframe_cleaner import *
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

def analyze():

    fake_clinics_df = fake_clinic_db_to_df('CPC_clinics.db')

    real_clinics_df = real_clinic_db_to_df('HPC_clinics.db')

    all_clinics_df = pandas.concat([fake_clinics_df, real_clinics_df], ignore_index = True, axis = 0)

    policy_df = policyapi_db_to_df('api.db')

    clinic_and_policy_df = pandas.merge(all_clinics_df, policy_df, on = "state", how = "inner")

    keyword = "ultrasound"

    keyword_across_states_df = fake_and_real_keyword_across_states(keyword)
    keyword_across_states_df = keyword_across_states_df.reset_index()
    keyword_across_states_df = keyword_across_states_df.rename(columns = {"index" : "url"})
    keyword_across_states_df_missing_values_map = {"ultrasound" : 0}
    keyword_across_states_df = keyword_across_states_df.fillna(value = keyword_across_states_df_missing_values_map)
    print(keyword_across_states_df)

    clinic_policy_keyword_count_df = pandas.merge(clinic_and_policy_df, keyword_across_states_df, on = "url", how = "inner")

    print(clinic_policy_keyword_count_df)

    # x_df = clinic_and_policy_df[["waiting_period_hours", "counseling_visits", "banned_after_weeks_since_LMP"]]

    x_df = clinic_policy_keyword_count_df[[keyword, "waiting_period_hours", "counseling_visits", "banned_after_weeks_since_LMP"]]

    y_df = clinic_policy_keyword_count_df[["IV"]]

    X_train, X_test, y_train, y_test = train_test_split(x_df, y_df, test_size=0.2, random_state=25)

    print(X_train)

    reg = LinearRegression().fit(X_train, y_train)

    y_pred = reg.predict(X_train)
    mse = mean_squared_error(y_train, y_pred)
    y_pred_test = reg.predict(X_test)
    # r2 = r2_score(y_test, y_pred)

    # The coefficients - i can make this into a nicely printed table
    print("Coefficients: \n", reg.coef_)
    print("mean squared error:", mse)
    # print("r^2:", r2)

    return X_train, y_train, y_pred, clinic_policy_keyword_count_df


# for running from somewhere else
# analyze()


# I'd like to plot at least some things. 
# plt.scatter(X_train["waiting_period_hours"], y_train)
# plt.plot(X_train["waiting_period_hours"], y_pred, color="red")
# plt.xlabel("x")
# plt.ylabel("y")
# plt.savefig("/home/mattryan/programmingturk/FinalProject/truth_inquery/analysis_model/plot.png")
# plt.show()

# Plot sepal width as a function of sepal_length across days
# g = sns.lmplot(
#     data=penguins,
#     x="bill_length_mm", y="bill_depth_mm", hue="species",
#     height=5
# )

# # Use more informative axis labels than are provided by default
# g.set_axis_labels("Snoot length (mm)", "Snoot depth (mm)")