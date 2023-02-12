import sys
from truth_inquery.crawler.crawler import csv_extract, network_crawl

if __name__ == "__main__":
    # CPC sites
    cpcinput = ["data/Illinois (IL).csv"]
    # inputs = ["data/Alabama (AL).csv", "data/Delaware (DE).csv", "data/Arizona (AZ).csv",  "data/Colorado (CO).csv",  "data/Mississippi (MS).csv",]
    for f in cpcinput:
        outpath = f.replace('data','output')
        outpath = outpath[:-9] + " tokens.csv"
        df = csv_extract(f)
        urls = df['url'].tolist()
        network_crawl(urls, outpath, limit = 50)

    # Healthcare sites
    zips =  df['zip'].tolist()
    hcurl = "https://www.fpachicago.com"
    hcinput = [hcurl]
    outpath = "output/hc_crawl.csv"
    network_crawl(hcinput, outpath, limit = 50)
