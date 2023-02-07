from urllib.error import HTTPError
import lxml.html
import pandas as pd
import re
import scrapelib
from crawler.clean_data import clean_df
s = scrapelib.Scraper(retry_attempts=0, retry_wait_seconds=0)

INDEX_IGNORE = (
    "a",
    "an",
    "and",
    "&",
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

def extract_urls(input_file):
    """
    Loads .csv input file and extracts 'Website'
    column to convert to list of urls to iterate over

    Inputs:
        input_file (str): File path to .csv file
    
    Returns: list of URLs
    """
    f = pd.read_csv(input_file)
    df = f[f['Website'].notna()]
    return df['Website'].tolist()
    
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

    Tokens are stripped of special characters and converted to lowercase.
    The tokens and frequencies are stored as key, value pairs in a dictionary.

    Inputs:
        HTML 'Root' element from a website from which text is scraped
    
    Returns dictionary of with token-frequency key-values
    pairs from website.
    """
    tokens = {} 
    pattern = re.compile(r'\W+')
    all_text = ''.join(root.itertext())

    for key in all_text.split():
        str_key = str(key).lower()
        if re.search(pattern, str_key) is not None:
            continue

        if str_key not in INDEX_IGNORE and len(str_key) > 1:
            if str_key not in tokens:
                tokens[str_key] = 1
            else:
                tokens[str_key] += 1
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

def state_crawl(input_file, limit=15):
    """
    Takes state-level csv file containing column of website URLs 
    and scrapes up to the limit of URLs that are connected to each base url.

    Writes data to CSV

    Inputs
        - input_file (str): file path to CSV file containing URL data
        - limit (int): maximum number of links to be scraped for each
                        base url

    Returns: None, creates csv file
    """
    b = 1
    output = input_file.replace('data','output')
    output = output.replace('.csv',' cleaned.csv')
    base_urls = extract_urls(input_file)
    df = clean_df(crawl(base_urls[0], limit)).add_suffix('_0')

    for i, b_url in enumerate(base_urls[1:]):
        print("Base URL", b, "crawling")
        new_df = clean_df(crawl(b_url, limit))
        df = df.join(new_df.add_suffix('_' + str(i+1)), how='outer')
        df = df.fillna(0)
        print("Base URL", b, "finished")
        b += 1

    df.to_csv(output)
    print("CSV saved")
