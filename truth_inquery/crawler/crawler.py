# Aaron Haefner
# Crawl and tokenize URLs
"""
- clone repo - 
cd 30122-project-truth-inquery
poetry install
poetry run python truth_inquery/crawler
"""
import lxml.html
import pandas as pd
import re
import scrapelib
import time
from collections import defaultdict

s = scrapelib.Scraper(retry_attempts=0, retry_wait_seconds=0)

LIMIT = 1
PATTERN = r'[\[0-9()="?!}{<>.,~`@#$%&*^_+:;|\]\\\/]'

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
        if len(str_key) > 2 and len(str_key) < 25:
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

    # Save url-level csv containing top tokens 
    tidy_top_tokens(clinic, res, outpath)


def tidy_top_tokens(df, df_ids, outpath, num=200):
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
    ordered_cols = []
    for n in range(0,num+1):
        ordered_cols.append('token'+str(n))
        ordered_cols.append('count'+str(n))

    newdf = pd.DataFrame()
    df = df.transpose()
    df.columns = df.iloc[0]
    df = df.iloc[1:]

    # Loop over columns and generate token and count columns for each top token
    for col in df.columns[1:]:

        # Single (column) df 'sdf'
        sdf = df[['token',col]]

        # Sort descending by token count and select first num rows
        sdf[col] = sdf[col].astype(int)
        sdf = sdf.sort_values(by=col, ascending = False)
        sdf = sdf.iloc[0:num+1,]

        # Reset index, create index column for reshape
        sdf = sdf.reset_index()
        sdf = sdf.drop('index', axis=1)
        sdf = sdf.reset_index()
        sdf['id'] = col
        sdf = sdf.rename(columns={col:'count'})
        sdf['index'] = sdf['index'].astype(str)
        sdf = sdf.pivot(values=['token','count'], columns='index', index='id')
        sdf.columns = [', '.join(col).replace(', ','',1) for col in sdf.columns]
        sdf = sdf[ordered_cols]

        # Add single row (URL) with top num token tuples as columns
        newdf = pd.concat([newdf, sdf])

    # Merge tokens with xwalk on count`num` identifier
    output = pd.merge(df_ids, newdf, left_on='index',right_on='id',how='outer')
    output.to_csv(outpath)
