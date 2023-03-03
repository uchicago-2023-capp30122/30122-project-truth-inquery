# Aaron Haefner
import lxml.html
import pandas as pd
import re
import scrapelib
import time
from collections import defaultdict

s = scrapelib.Scraper(retry_attempts=0, retry_wait_seconds=0)

LIMIT = 25

CPCIN = "truth_inquery/data/CPC_"
CPCOUT = "truth_inquery/output/CPC_state_clinics.csv"

HPCIN = "truth_inquery/data/HPC_urls_state.csv"
HPCOUT = "truth_inquery/output/HPC_state_clinics.csv"

PATTERN = r'[\[0-9()="?!}{<>.,~`@#$%&*^_+:;|\]\\\/]'

# source: https://gist.github.com/rogerallen/1583593
STATES = {
    'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticwut', 'DC': 'District of Columbia', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'IA': 'Iowa', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana',
    'MA': 'Massachusetts', 'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota',
    'MO': 'Missouri', 'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina','ND': 'North Dakota',
    'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NV': 'Nevada',
    'NY': 'New York', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania',
    'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee',
    'TX': 'Texas', 'UT': 'Utah', 'VA': 'Virginia', 'VT': 'Vermont', 'WA': 'Washington', 
    'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'
}

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
    Takes in pandas dataframe, replace NaN with 0 and 
    generate new column sum 'count' across all columns

    Input: pandas dataframe
    Returns token,count pandas dataframe
    """
    output = df.fillna(0)
    output['count'] = output.sum(axis=1)
    output = output[['count']]
    return output

def csv_extract(input_file):
    """
    Loads CPC csv input file and extracts 'Website'
    column to convert to list of urls

    Inputs:
        input_file (str): File path to .csv file

    Returns: list of URLs
    """
    f = pd.read_csv(input_file)
    df = f[f['Website'].notna()]
    df = df[['Name ', 'Zip Code', 'State', 'Website']]

    # Rename columns, front-fill zipcode with zeros.
    df = df.rename(columns={'Name ':'name', 'Zip Code':'zip', 'State':'state', 'Website':'url'})
    df['zip'] = df['zip'].astype(str)
    df['zip'] = df['zip'].str.zfill(5)
    df['url'] = df['url']
    return df

def get_root(url):
    """
    Extracts html root from url if possible

    Inputs:
        url (str): URL

    Returns: set of URLs otherwise None
    """
    try:
        response = s.get(url, timeout=5)
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
    tokens = defaultdict(int)
    pattern = re.compile(PATTERN)

    # Spend a maximum of 5 minutes tokenizing a url
    timeout = time.time() + 60*5
    all_text = ''.join(root.itertext())

    for key in all_text.split():
        str_key = str(key).lower()

        # Ignore if token contains any chars in pattern
        if re.search(pattern, str_key) is not None:
            continue
        # Word exclusion and length restrictions
        if str_key not in INDEX_IGNORE and len(str_key) > 2 and len(str_key) < 25:
            tokens[str_key] += 1

        # Checks time EVERY token - not ideal?
        if time.time() > timeout:
            return tokens

    return tokens

def crawl(url, limit):
    """
    Crawls the adjacent HREFs of the input URL up to the limit number.

    Inputs
        - url: (str) url
        - limit: (int) number of adjacent urls to crawl

    Returns:
        - df: pandas dataframe with a column of token counts for each adj. URL
        - urls_visited 
    """
    dct = defaultdict(dict)
    urls_visited = 1

    root = get_root(url)
    if root is None:
        return None

    # Tokenize the base URL (input) and identify unique sub-urls
    dct['base'] = tokenize(root)
    urls = set(root.xpath('//a/@href'))

    # Tokenize up to limit # sub-URLs creating a new column for each
    for u in urls:
        subroot = get_root(u)
        if subroot is not None:
            urls_visited += 1
            dct['url'+str(urls_visited)] = tokenize(subroot)

        if urls_visited == limit:
            break

    # Return df and number of urls visited including base
    df = pd.DataFrame(dct)
    return df, urls_visited

def network_crawl(urllst, outpath, limit=LIMIT):
    """
    Crawls each URL in list and up to the limit number of adjacent HREFs.

    Creates tokenized results dataframe and corresponding xwalk to
    re-link URLs to corresponding tokens

    Inputs
        - urllst: (list of strings) URLs 
        - outpath: (str) file path to save cleaned CSV passed to helper
        - limit: (int) max num of hrefs to crawl for each URL

    Returns: None, passes dataframe output to helper
    """
    # Accumulators 
    df = pd.DataFrame()
    results = defaultdict(dict)

    # Crawl each URL
    for b, b_url in enumerate(urllst):

        print("Base URL", b, "crawling")
        crawldf = crawl(b_url, limit)
        # If root is none skip
        if crawldf is None:
            continue
        
        # Unpack output of crawl and add to data accumulators
        nextdf, urls_visited = crawldf

        # Join df and new_df
        df = df.join(clean_df(nextdf).add_suffix(str(b+1)), how='outer')
        df = df.fillna(0)

        results[b+1] = {'url': b_url, 'urls_visited': urls_visited}
        print("Finished")

    # Prep URL-level data and results by generating common merge index
    # Clinic/URL-level data
    clinic = df.reset_index()
    clinic = clinic.rename({'index':'token'}, axis=1)
    clinic = clinic.transpose()
    clinic = clinic.reset_index()

    # Xwalk
    res = pd.DataFrame(results)
    res = res.transpose()
    res = res.reset_index()
    # Key for merge
    res['index'] = "count" +  res['index'].astype(str)

    # save url-level csv containing top tokens 
    top_clinic_tokens(clinic, res, outpath)


def top_clinic_tokens(df, df_ids, outpath, num=200):
    """
    Cleans input token df to merge with df_ids and saves csv with
    top num tokens.

    Inputs
        - df: (pd dataframe) token counts for each URL
        - df_id: (pd df) xwalk with URL and num urls visited 
        - output: (str) outpath for csv
        - num: (int) number of top tokens to include

    Returns: None, writes standardized dataframe of nlargest tokens to csv
    """
    # Accumulate top num tokens in newdf
    newdf = pd.DataFrame()
    df = df.transpose()
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    # Loop over columns and generate (token,count) tuple
    for col in df.columns[1:]:

        # Single (column) df 'sdf'
        sdf = df[['token',col]]
        # Sort descending by token count and select first num rows
        sdf[col] = sdf[col].astype(float)
        sdf = sdf.sort_values(by=col, ascending = False)
        sdf = sdf.iloc[0:num,]

        sdf = sdf.reset_index()
        sdf = sdf.drop('index', axis=1)

        # Generate tuple row and replace original col with tuples
        sdf['tuple'] = list(zip(sdf['token'], sdf[col]))
        sdf = sdf.drop(['token',col], axis=1)
        sdf = sdf.rename(columns={'tuple':col})
        sdf = sdf.transpose()

        # Add single row (URL) with top num token tuples as columns
        newdf = pd.concat([newdf, sdf])
    
    # Merge tokens with xwalk on count`num` identifier
    output = pd.merge(df_ids, newdf, left_on='index',right_on='index',how='outer')
    output.to_csv(outpath)

# States that are not crawled due to abortion restrictions
# banned: Alabama Arkansas Idaho Kentucky Louisiana Mississippi Missouri 
#         Oklahoma South Dakota Tennessee Texas West Virginia
# stopped scheduling: North Dakota Wisconsin
# This takes all input data for each state CPCs and HPCs and tokenizes URLs into CSVs
if __name__ == "__main__":

    for stabb, name in STATES.items():
        # Crawl CPC urls
        try:
            CPCinput = CPCIN + name + " (" + stabb + ").csv"
            CPCoutput = CPCOUT.replace("state", stabb)
            df = csv_extract(CPCinput)
        except FileNotFoundError:
            print(stabb, "file does not exist")
            continue

        urls = df['url'].tolist()[0:6]

        # print("Crawling CPCs in", stabb)
        network_crawl(urls, CPCoutput, LIMIT)

        # Crawl HPC urls
        HPCinput = HPCIN.replace("state", stabb)
        HPCoutput = HPCOUT.replace("state", stabb)

        try: 
            HPC = pd.read_csv(HPCinput)
        except FileNotFoundError:
            print(stabb, "file does not exist")
            continue

        HPC_urls = HPC['url'].to_list()

        print("Crawling HPCs in", stabb)
        network_crawl(HPC_urls, HPCoutput, LIMIT)

        print(stabb,"CPCs and HPCs saved")
