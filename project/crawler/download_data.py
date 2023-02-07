# import requests
# import json
import lxml.html
import scrapelib
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
import time


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

s = scrapelib.Scraper(retry_attempts=3, retry_wait_seconds=3)
url = "https://www.exposefakeclinics.com/list-of-states"
response = s.get(url)
root = lxml.html.fromstring(response.text)
rel_urls = root.xpath('//a/@href')

rel_urls = rel_urls[rel_urls.index("/alabama"):rel_urls.index("/wyoming")+1]
abs_urls = [make_link_absolute(rel_url, url) for rel_url in rel_urls]

driver = webdriver.Chrome()
actionChains = ActionChains(driver)
driver.get(url)
time.sleep(5)

# driver.find_element(By.XPATH, '//div[@class="ml-half truncate flex-auto"]').click()
driver.find_element(By.XPATH, '//div[@id="hyperbaseContainer"]//div[text()="Download CSV"]').click()
time.sleep(1)
# text()="Download CSV"
# <div class="ml-half truncate flex-auto">Download CSV</div>