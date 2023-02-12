# Collect healthcare provider base URLs using CPC zip codes
# Input: CPC zip codes, Output: list of healthcare URLs in a state
import lxml.html
import scrapelib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import time

# Illinois
zips = [
38614,
39074,
39503,
39402,
39401,
39206,
39440,
39301,
38652,
38655,
39350,
39466,
39157,
38663,
38671,
38671,
38759,
38801,
39180,
39216]

def download_data():
    # s = scrapelib.Scraper(retry_attempts=3, retry_wait_seconds=3)
    start = "https://www.abortionfinder.org"

    driver = webdriver.Chrome()
    driver.get(start)
    # response = s.get(start)
    xpath = '//*[@id="location-box"]'
    ele = driver.find_element(By.XPATH, xpath)
    box = driver.find_element(By.XPATH, '//*[@id="root"]/div/div[2]/div[2]/div/form/div[3]/label/img')
    ele.send_keys(z)
    box.click()
    ele.send_keys(Keys.RETURN)

    # root = lxml.html.fromstring(response.text)
#
    # ele = root.x#.send_keys("some text")
    # rel_urls = root.xpath('//a/@href')