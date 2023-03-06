# Matt Ryan
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import pandas
from analysis_model.dataframe_cleaner import *


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

# for running from somewhere else
# analyze()

# I'd like to plot at least some things. 
# plt.scatter(X_train["waiting_period_hours"], y_train)
# plt.plot(X_train["waiting_period_hours"], y_pred, color="red")
# plt.xlabel("x")
# plt.ylabel("y")
# plt.savefig("/home/mattryan/programmingturk/FinalProject/truth_inquery/analysis_model/plot.png")
# plt.show()

