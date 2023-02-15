import requests
import json
import pandas as pd
from urllib.parse import urlparse

apikey = '#######'

def make_link_absolute(rel_url, current_url):
    """
    Given a relative URL like "/abc/def" or "?page=2"
    and a complete URL like "https://example.com/1/2/3" this function will
    combine the two yielding a URL like "https://example.com/abc/def"
    Parameters:
        * rel_url:      a URL or fragment
        * current_url:  a complete URL used to make the request that contained a link to rel_url
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
    """
    relative_urls = ["/v1/gestational_limits/states/", 
                    "/v1/insurance_coverage/states/", 
                    "/v1/minors/states/", 
                    "/v1/waiting_periods/states/"]
    current_url = "http://api.abortionpolicyapi.com"
    urls = []
    for rel_url in relative_urls:
        url = make_link_absolute(rel_url, current_url)
        urls.append(url)
    return urls


def api_calls():
    """
    """
    urls = list_urls()
    headers = { 'token': apikey }
    files = []
    for url in urls:
        url_parts = url.split('/')
        r = requests.get(url, headers=headers)
        a = r.json()
        fileName = f"{url_parts[4]}.json"
        files.append(fileName)
        with open(fileName, "w", encoding='utf-8') as outfile:
            json.dump(a, outfile)
    return files

def convert_json_csv():
    """
    """
    files = api_calls()
    for file in files:
        with open(file, encoding='utf-8') as inputfile:
            df = pd.read_json(inputfile)
        df.to_csv(f"{file[:len(file)-5]}.csv", encoding='utf-8', index=False)

convert_json_csv()
