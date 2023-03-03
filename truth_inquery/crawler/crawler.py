# Aaron Haefner
import lxml.html
import pandas as pd
import re
import scrapelib
import time
from collections import defaultdict
import glob 

s = scrapelib.Scraper(retry_attempts=0, retry_wait_seconds=0)


LIMIT = 25
CPCIN = "truth_inquery/data/CPC_"
CPCOUT = "truth_inquery/temp/CPC_state_clinics.csv"
HPCIN = "truth_inquery/data/HPC_urls_state.csv"
HPCOUT = "truth_inquery/temp_output2/HPC_state_clinics.csv"

PATTERN = r'[\[0-9()="?!}{<>.,~`@#$%&*^_+:;|\]\\\/]'

# source: https://gist.github.com/rogerallen/1583593
STATES = {
    'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia', 'DE': 'Delaware',
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

def clean_df(df, n=""):
    """
    Clean pd dataframe. replace NaN with 0.
    collapse columns into single column by summing token count Total
    """
    output = df.fillna(0)
    output['count'] = output.sum(axis=1)
    output = output[['count']]

    output['count'] = output.sum(numeric_only=True, axis=1)
    output = output[['token', 'count']]
    output = output.sort_values(by=['count'], axis=0, ascending = False)
    # output = output[['count'][:150]]
    # not this 
    # output = output['count'][:150]
    # output = output.iloc(0:100,axis=0]

    # Keep top X

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
    df = df[['Name ', 'Zip Code', 'State', 'Website']]

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
            tokens[str_key] += 1

        # Checks time EVERY token - not ideal?
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
    dct = defaultdict(dict)
    urls_visited = 1

    root = get_root(url)
    if root is None:
        return None

    # Tokenize the base URL (input)
    dct['base'] = tokenize(root)
    urls = set(root.xpath('//a/@href'))

    # Tokenize up to limit # sub-URLs
    for u in urls:
        subroot = get_root(u)
        if subroot is not None:
            urls_visited += 1
            dct['url'+str(urls_visited)] = tokenize(subroot)

        if urls_visited == limit:
            break

    df = pd.DataFrame(dct)
    return df, urls_visited

def network_crawl(urllst, outpath, limit=2):
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
    df = pd.DataFrame()
    results = defaultdict(dict)

    for b, b_url in enumerate(urllst):

        print("Base URL", b, "crawling")
        crawldf = crawl(b_url, limit)
        if crawldf is None:
            continue

        nextdf, urls_visited = crawldf

        # Join df and new_df
        df = df.join(clean_df(nextdf).add_suffix(str(b+1)), how='outer')
        df = df.fillna(0)

        results[b+1] = {'url': b_url, 'urls_visited': urls_visited}
        print("Finished")


    # Prep URL-level data and results for merge
    # Clinic/URL-level data
    clinic = df.reset_index()
    clinic = clinic.rename({'index':'token'}, axis=1)
    # clinic = clinic.transpose()
    # clinic = clinic.reset_index()

    return clinic
    # clinic.to_csv(outpath)

    # # # Xwalk
    # res = pd.DataFrame(results)
    # res = res.transpose()
    # res = res.reset_index()
    # res['index'] = "count" +  res['index'].astype(str)
    
    # # res.to_csv(outpath.replace("_clinics", "_links"))

    # output = pd.merge(clinic, res, left_on='index', right_on='index', how='outer')
    # output.to_csv(outpath, index=False)

# FUCK
def top_clinic_tokens(csv, num):
    """
    Cleans token-count pandas dataframe for single URL (clinic)

    Inputs
        - df: (pd dataframe) dataframe as token-count columns (2)
        - col (string): column with token counts of URL
        - top_num (int): number of top tokens to return

    Returns standardized dataframe of nlargest tokens by count 
    """
    # filter and sort
    newdf = pd.DataFrame()
    df = pd.read_csv(csv)
    dfid = df[['url', 'urls_visited', 'index']]
    df = df.drop(['url', 'urls_visited'], axis=1)

    df = df.transpose()
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    for col in df.columns[1:]:
        sdf = df[['token',col]]
        sdf[col] = sdf[col].astype(float)
        sdf = sdf.sort_values(by=col, ascending = False)
        sdf = sdf.iloc[0:num,]
        sdf = sdf.reset_index()
        sdf = sdf.drop('index', axis=1)
        sdf['tuple'] = list(zip(sdf['token'], sdf[col]))
        sdf = sdf.drop(['token',col], axis=1)
        sdf = sdf.rename(columns={'tuple':col})
        sdf = sdf.transpose()
        newdf = pd.concat([newdf, sdf])
    
    output = pd.merge(dfid, newdf, left_on='index',right_on='index',how='outer')
    output = output.iloc[1:]
    output.to_csv(csv.replace('temp_output', 'temp_output2'))

for csv in glob.glob('truth_inquery/temp_output/*'):
    top_clinic_tokens(csv, 200)

# States that are not crawled due to abortion restrictions
# banned: Alabama Arkansas Idaho Kentucky Louisiana Mississippi Missouri 
#         Oklahoma South Dakota Tennessee Texas West Virginia
# stopped scheduling: North Dakota Wisconsin
# if __name__ == "__main__":

#     # for state in STATES.keys():
#     #     clinics = "truth_inquery/temp/CPC_state_clinics.csv".replace("state", state)
#     #     links = "truth_inquery/temp/CPC_state_links.csv".replace("state", state)
#     #     try:
#     #         merge_data(clinics, links)
#     #     except:
#     #         continue
#         # out = clinics.replace("temp","temp_output")
#         # out.to_csv(index=False)
#     # for stabb, name in STATES2.items():
#     # for stabb in ["CA"]:
#         # Crawl CPC urls
#     #     try:
#     #         CPCinput = CPCIN + name + " (" + stabb + ").csv"
#     #         CPCoutput = CPCOUT.replace("state", stabb)
#     #         df = csv_extract(CPCinput)
#     #     except FileNotFoundError:
#     #         print(stabb, "file does not exist")
#     #         continue

#     #     urls = df['url'].tolist()

#     #     print("Crawling CPCs in", stabb)
#     #     network_crawl(urls, CPCoutput, LIMIT)

#     for stabb, name in STATES2.items():a
#         # Crawl HPC urls
#         HPCinput = HPCIN.replace("state", stabb)
#         HPCoutput = HPCOUT.replace("state", stabb)

#         try: 
#             HPC = pd.read_csv(HPCinput)
#         except FileNotFoundError:
#             print(stabb, "file does not exist")
#             continue
#         HPC_urls = HPC['url'].to_list()

#         print("Crawling HPCs in", stabb)
#         network_crawl(HPC_urls, HPCoutput, LIMIT)

#         print(stabb,"CPCs and HPCs saved")
