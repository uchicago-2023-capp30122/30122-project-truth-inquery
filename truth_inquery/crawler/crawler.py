import lxml.html
import pandas as pd
import re
import scrapelib
import time
from urllib.error import HTTPError
from hcp_urls import get_hcp_base
s = scrapelib.Scraper(retry_attempts=0, retry_wait_seconds=0)



"""
Change state crawl function to work as network crawl function
that works with a list of urls as inputs (extract URLs outside of function from state files)
- maybe generate ahref lists in separate helper function.

zip codes:
- we can visualize where the CPCs are at the state-level using maps (plotly, geo spacial data)
- top tokens by state

CPC ZIP CODE, potentially look up the zip code that is captured in the Abortion Finder website

- Seaborn
- mapping
- work with data interactively in jupyter notebooks 
i.e., import your two datasets in a different notebook
"""


INDEX_IGNORE = (
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "he",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "that",
    "the",
    "to",
    "was",
    "were",
    "will",
    "with"
)

def clean_df(df):
    """
        Clean pd dataframe. replace NaN with 0.
        collapse columns into joint columns by summing key total
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
        url (str): url

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
    Tokens are created by iterating over all text 
    elements (regardless of tag - needs refinement), on a website.

    Currently: tokens are selected by excluding anything with a special character
    Inputs:
        HTML 'Root' element from a website from which text is scraped
    
    Returns dictionary of with token-frequency key-values
    pairs from website.
    """
    tokens = {} 
    pattern = re.compile(r'[0-9()="?!}{<>.,~`@#$%&*^_+:;|]')
    all_text = ''.join(root.itertext())
    timeout = time.time() + 60*5   # 5 minutes from now
    for key in all_text.split():
        str_key = str(key).lower()
        if re.search(pattern, str_key) is not None:
            continue

        if str_key not in INDEX_IGNORE and len(str_key) > 1:
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

    dct['base'] = tokenize(root)
    urls = set(root.xpath('//a/@href'))
    
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
    b = 1
    # print("Base URL", b, "crawling")
    df = clean_df(crawl(urllst[0], limit))
    # print("Finished")

    for i, b_url in enumerate(urllst[1:]):
        print("Base URL", b, "crawling")
        new_df = clean_df(crawl(b_url, limit))
        df = df.join(new_df.add_suffix(str(i+1)), how='outer')
        df = df.fillna(0)
        print("Finished")
        b += 1
    # Row-wise sum, one column of token counts by state
    df = clean_df(df)
    df = df.reset_index()
    df.to_csv(outpath)
    print("CSV saved")
    return df

if __name__ == "__main__":

    #banned: Alabama Arkansas Idaho Kentucky Louisiana Mississippi Missouri Oklahoma South Dakota Tennessee Texas
    # datafiles = ["truth_inquery/data/Georgia (GA).csv"]
    # datafiles = ["truth_inquery/data/Hawaii (HI).csv", "truth_inquery/data/Maryland (MD).csv", "truth_inquery/data/Michigan (MI).csv", "truth_inquery/data/Montana (MT).csv", "truth_inquery/data/Nebraska (NE).csv", "truth_inquery/data/New Mexico (NM).csv"]
    # datafiles = ["truth_inquery/data/North Carolina (NC).csv", "truth_inquery/data/North Dakota (ND).csv", "truth_inquery/data/Ohio (OH).csv", "truth_inquery/data/Oregon (OR).csv", "truth_inquery/data/Pennsylvania (PA).csv", "truth_inquery/data/Rhode Island (RI).csv"]
    # datafiles = ["truth_inquery/data/South Carolina (SC).csv", "truth_inquery/data/Utah (UT).csv", "truth_inquery/data/Vermont (VT).csv", "truth_inquery/data/Virginia (VA).csv", "truth_inquery/data/Washington (WA).csv", "truth_inquery/data/West Virginia (WV).csv", "truth_inquery/data/Wisconsin (WI).csv" , "truth_inquery/data/Wyoming (WY).csv"]
    datafiles = ["truth_inquery/data/Maine (ME).csv"]
    for file in datafiles:
        state = file[-7:]
        state = state[:2]

        df = csv_extract(file)
        urls = df['url'].tolist()
        outpath = "truth_inquery/output/" + state + "_CPC_tokens.csv"
        print("Crawling CPCs in ", state)
        # network_crawl(urls, outpath, limit = 50)

        # Collect tokens from HCPs
        # zips = set(df['zip'].tolist())
        # hcp = get_hcp_base(zips, state)
        # hcp_urls = hcp.loc['url'].to_list()

        hcp = pd.read_csv("truth_inquery/data/hcp_urls_" + state + ".csv")
        hcp_urls = hcp.iloc[0].to_list()
        
        baseurls = list(set(hcp_urls))
        outpath = "truth_inquery/output/" + state + "_HCP_tokens.csv"
        print("Crawling HCPs in", state)
        network_crawl(baseurls, outpath, limit = 50)

        print(state,"saved")
