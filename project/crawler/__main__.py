import sys
from crawler.crawler import state_crawl

if __name__ == "__main__":
    input_file = "data/Mississippi (MS).csv"
    state_crawl(input_file, limit = 50)
