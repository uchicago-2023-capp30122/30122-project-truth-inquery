#Dema Therese
#Retrieves data using GET requests from https://www.abortionpolicyapi.com and 
#converts the json files to csv files 

#poetry run python truth_inquery/database_model/api_requests.py

import os
import requests
import json
import pandas as pd
from urllib.parse import urlparse

api_key = "########"

dir_path = "truth_inquery/database_model"

relative_urls = ["/v1/gestational_limits/states/", 
                "/v1/insurance_coverage/states/", 
                "/v1/minors/states/", 
                "/v1/waiting_periods/states/"]

def make_link_absolute(rel_url, current_url):
    """
    Combines a relative and complete url
    Input:
        rel_url:      
        a URL or fragment
        current_url:  
        a complete URL used to make the request that contained a link to rel_url
    Returns:
        A full URL with protocol & domain that refers to rel_url.
    """
    url = urlparse(current_url)
    if rel_url.startswith("/"):
        return f"{url.scheme}://{url.netloc}{rel_url}"
    elif rel_url.startswith("?"):
        return f"{url.scheme}://{url.netloc}{url.path}{rel_url}"
    else:
        return rel_url

def list_urls():
    """
    Returns a list of full URLs/queries of different data tables from 
    https://www.abortionpolicyapi.com to run the function "api_calls()".
    """
    current_url = "http://api.abortionpolicyapi.com"
    urls = []
    for rel_url in relative_urls:
        url = make_link_absolute(rel_url, current_url)
        urls.append(url)
    return urls


def api_calls():
    """
    A function to perform the GET requests to the specified urls in the list 
    generated from "list_urls()".
    Returns:
        A list of json file names of the respective GET requests. 
    """
    urls = list_urls()
    headers = { 'token': api_key }
    files = []
    for url in urls:
        url_parts = url.split('/')
        r = requests.get(url, headers=headers)
        a = r.json()
        fileName = f"{url_parts[4]}" 
        filepath = os.path.join(dir_path, fileName + ".json")
        files.append(filepath)
        with open(filepath, "w", encoding = 'utf-8') as outfile:
            json.dump(a, outfile)
    print("Creating API json files")
    return files


def convert_json_csv():
    """
    Calls the api_calls function to generate a list of json file names and 
    converts the json files to csv format.
    Returns:
        A csv file for each json file and stored in the current (database_model) 
        subdirectory.
    """
    files = api_calls()
    for file in files:
        with open(file, encoding = 'utf-8') as inputfile:
            df = pd.read_json(inputfile)
            filepath = f"{file[:len(file)-5]}.csv"
            df.to_csv(filepath, encoding = 'utf-8', index = True)
    print("Converting API json files to CSV files")

if __name__ == "__main__":
    convert_json_csv() 
    print("Successfully stored API json and csv files in database_model")
