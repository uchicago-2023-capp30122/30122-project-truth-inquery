from database_model.api_requests import convert_json_csv
from database_model.database import *
from analysis_model.lpm import analyze
from analysis_model.dataframe_cleaner import *


if __name__ == "__main__":

    convert_json_csv() 
    print("Successfully stored API json and csv files in database_model")

    api_db()
    print("api database created")

    concat_clinic_db("CPC", "CPC_clinics")
    concat_clinic_db("HPC", "HPC_clinics")
    print("CPC_clinics and HPC_clinics databases created")

    token_related_db("CPC", "Tokens_CPC_clinics")
    token_related_db("HPC", "Tokens_HPC_clinics")
    print("Tokens_CPC_clinics and Tokens_HPC_clinics databases created")

    analyze()


