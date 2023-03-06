import pandas as pd
from crawler import network_crawl, LIMIT
from hpc_urls import CPCIN, CPCOUT, HPCIN, HPCOUT, csv_extract, get_HPC_base
from truth_inquery.analysis_model.states import STATES

"""
poetry run python truth_inquery/crawler

States that are not crawled due to abortion restrictions
banned: Alabama Arkansas Idaho Kentucky Louisiana Mississippi Missouri 
        Oklahoma South Dakota Tennessee Texas West Virginia
stopped scheduling: North Dakota Wisconsin
This takes all input data for each state CPCs and HPCs and tokenizes URLs into CSVs
"""
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

        # CPC URLs and zip codes
        # Crawl these
        urls = df['url'].tolist()[0:6]

        print("Crawling CPCs in", name)
        network_crawl(urls, CPCoutput, LIMIT)

        # Collect HPC URLs using zip codes
        zips = set(df['zip'].tolist())
        get_HPC_base(zips, stabb)

        HPCinput = HPCIN.replace("state", stabb)
        HPCoutput = HPCOUT.replace("state", stabb)

        # Crawl collected URLs
        HPC = pd.read_csv(HPCinput)
        HPC_urls = HPC['url'].to_list()

        print("Crawling HPCs in", name)
        network_crawl(HPC_urls, HPCoutput, LIMIT)

        print(stabb,"CPCs and HPCs saved")
