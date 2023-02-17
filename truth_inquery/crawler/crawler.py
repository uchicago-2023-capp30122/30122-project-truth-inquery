import lxml.html
import pandas as pd
import re
import scrapelib
import time
import glob
import sys

s = scrapelib.Scraper(retry_attempts=0, retry_wait_seconds=0)

PATTERN = r'[\[0-9-()="?!}{<>.,~`@#$%&*^_+:;|\]\\\/]'
INDEX_IGNORE = (
    "and",
    "are",
    "for",
    "from",
    "has",
    "its",
    "that",
    "the",
    "was",
    "were",
    "will",
    "with"
)

def clean_df(df):
    """
    Clean pd dataframe. replace NaN with 0.
    collapse columns into single column by summing token count Total
    """
    output = df.fillna(0)
    output['Total'] = df.sum(axis=1)
    output = output[['Total']]
    return output

def csv_extract(input_file):
    """
    Loads .csv input file and extracts 'Website'
    column to convert to list of urls to iterate over

    Inputs:
        input_file (str): File path to .csv file
    
    Returns: list of URLs
    """
    f = pd.read_csv(input_file)
    df = f[f['Website'].notna()]
    df = df[['Website','Zip Code']]
    
    df = df.rename(columns={'Website':'url','Zip Code':'zip'})
    df['zip'] = df['zip'].astype(str)
    df['zip'] = df['zip'].str.zfill(5)
    return df
    
def get_root(url):
    """
    Extracts html root from url if possible

    Inputs:
        url (str): URL

    Returns: set of URLs otherwise None
    """
    try:
        response = s.get(url)
        root = lxml.html.fromstring(response.text)
        return root
    except:
        return None

def tokenize(root):
    """
    Tokens are created by iterating over the split text contained in
    all HTML tags on a website.

    Inputs:
        HTML 'Root' element from a website from which text is scraped
    
    Returns: dictionary of token-frequency key-values pairs.
    """
    tokens = {} 
    pattern = re.compile(PATTERN)

    # Spend a maximum of 5 minutes tokenizing text
    timeout = time.time() + 60*5
    all_text = ''.join(root.itertext())

    for key in all_text.split():
        str_key = str(key).lower()
        # Ignore if token contains any chars in pattern
        if re.search(pattern, str_key) is not None:
            continue
        # Word exclusion and length restrictions
        if str_key not in INDEX_IGNORE and len(str_key) > 2 and len(str_key) < 25:
            if str_key not in tokens:
                tokens[str_key] = 1
            else:
                tokens[str_key] += 1

        if time.time() > timeout:
            return tokens

    return tokens

def crawl(url, limit):
    """
    Crawls the URLs linked to the base url (input) up to the limit # of urls.

    Inputs
        - base url (str): url
        - limit (int) number of urls to be scraped

    Returns pandas dataframe out of tokenized network of URLs
    """
    dct = {}
    urls_visited = 0

    root = get_root(url)
    if root is None:
        return pd.DataFrame(dct)

    # Tokenize the base URL (input)
    dct['base'] = tokenize(root)
    urls = set(root.xpath('//a/@href'))
    
    # Tokenize up to limit # sub-URLs 
    for u in urls:
        subroot = get_root(u)
        if subroot is not None:
            urls_visited += 1
            dct['url'+str(urls_visited)] = tokenize(subroot)

        if urls_visited >= limit:
            break

    return pd.DataFrame(dct)

def network_crawl(urllst, outpath, limit=15):
    """
    Takes in URL list as list of base urls and crawls up to 
    the limit # of adjacent URLs (one click away)

    Writes data to CSV

    Inputs
        - urllst (list of strings): URLs to loop through
        - limit (int): maximum number of links to be scraped for each
                        base url

    Returns: None, creates csv file
    """
    # Counter
    b = 1

    # First URL is put in DF to join with others
    df = clean_df(crawl(urllst[0], limit))

    for i, b_url in enumerate(urllst[1:]):
        # Crawl and clean
        print("Base URL", b, "crawling")
        new_df = clean_df(crawl(b_url, limit))

        # Join df and new_df
        df = df.join(new_df.add_suffix(str(i+1)), how='outer')
        df = df.fillna(0)
        print("Finished")
        b += 1

    # Clinic/URL-level data
    clinic = df.reset_index()
    clinic = clinic.rename({'index':'token','Total':'Total0'}, axis=1)
    clinic.to_csv(outpath.replace("_tokens","_clinics"))

    # Row-wise sum, one column of token counts by state
    state = clean_df(df)
    state = state.reset_index()
    state = state.rename({'index':'token'}, axis=1)
    state.to_csv(outpath)
    print("CSV saved")
    return df

if __name__ == "__main__":

    #banned: Alabama Arkansas Idaho Kentucky Louisiana Mississippi Missouri Oklahoma South Dakota Tennessee Texas West Virginia
    #stopped scheduling: North Dakota Wisconsin
    # state_inputs = glob.glob("truth_inquery/data/*).csv")
    state_inputs = ["truth_inquery/data/Ohio (OH).csv"]
    for file in state_inputs:
        state = file[-7:]
        state = state[:2]
        print("Crawling CPCs in", state)
        df = csv_extract(file)
        urls = df['url'].tolist()
        outpath = "truth_inquery/output/" + state + "_CPC_tokens.csv"
        
        network_crawl(urls, outpath, limit = 50)

    # Collect tokens from HCPs
    states = ["MI", "MN", "OH"]
    base = "truth_inquery/data/hcp_urls_"
    # clinic_inputs = glob.glob("truth_inquery/data/hcp_urls*")
    # for file in clinic_inputs:
    for state in states:
        file = base + state + ".csv"
        # state = file[-6:-4]
        print("Crawling HCPs in", state)
        hcp = pd.read_csv(file)
        hcp_urls = hcp.iloc[0].to_list()
        
        baseurls = list(set(hcp_urls))
        outpath = "truth_inquery/output/" + state + "_HCP_tokens.csv"
        network_crawl(baseurls, outpath, limit = 50)

    print(state,"CPCs saved")
