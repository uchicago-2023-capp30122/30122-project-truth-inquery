# Collect healthcare provider base URLs using CPC zip codes
# Input: CPC zip codes, Output: list of healthcare urls to crawl
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

WAIT = 5
START = "https://www.abortionfinder.org"

def get_hcp_base(zips, state =""):
    """
    Navigates web browser to an abortion finder search and 
    enters each CPC zip code to search for nearby abortion healthcare providers (HCPs).

    When available, the crawler collects distance, address, and url (base URL) for the
    top search result (ordered by distance in miles, alphabetical otherwise).

    Input:
        - state (str): US state 2 letter abbreviation ******************************
        - zips (iterable/set): set of zip codes with at least one CPC in the state
    
    Returns:
    df (dataframe) containing data for each inital search result URL (base URL) (str) with
        - List of sub-URLS for each base (list of lists)
        - Distance between CPC and HCP (str)
        - Address of HCP (str)
    """
    # Create webdriver client for Chrome
    driver = webdriver.Chrome()
    hcp_data = {}
    k = 1

    # Loop through CPC zip codes
    for z in zips:
        base = {}
        driver.get(START)

        # Enter zip code
        driver.find_element(By.XPATH, '//*[@id="location-box"]').send_keys(z)

        # Tab to box to fill
        actions = ActionChains(driver)
        for _ in range(3):
            actions.send_keys(Keys.TAB)
            actions.perform()
            
        actions.send_keys(Keys.RETURN)
        actions.perform()

        # Submit search
        driver.find_element(By.XPATH, '//button[@type="submit"]').click()
        time.sleep(WAIT)

        # Limit to physical locations
        try:
            driver.find_element(By.XPATH, '//button[@class="telehealth-service ribbon-item desktop-only  "]').click()
            time.sleep(WAIT//WAIT)
            driver.find_element(By.XPATH, '//input[@id="physical_only"]').click()
            time.sleep(WAIT//WAIT)
            driver.find_element(By.XPATH, '//button[@class="apply "]').click()
        except NoSuchElementException:
            continue

        # Try to collect distance on current page
        try:
            distance_xpath = '//div[@class="clinic-summary-container"]//h4'
            distance = driver.find_element(By.XPATH, distance_xpath).text
        except NoSuchElementException:
            distance = ''

        # Try to navigate to the next page and collect url (base url)
        # If base url can be collected, collect sub URLs (a tags with hrefs)
        try:
            # First result
            driver.find_element(By.XPATH, '//div[@class="pill-link-container"]').click()
            time.sleep(WAIT)
            # Try to collect HCP website "Visit Website"
            ele = driver.find_element(By.XPATH, '//a[@class="clinic-link"]')
            base_url = ele.get_attribute("href")
            if not base_url:
                base_url = driver.current_url
            # print(base_url)
        except NoSuchElementException:
            continue
        
        try:
            address = driver.find_element(By.XPATH, '//section[@class="location-hours"]//p').text
            if state not in address:
                continue
        except NoSuchElementException:
            address = ''
        
        # Build dictionary for each HCP search result
        base['url'] = base_url
        base['distance'] = distance
        base['address'] = address
        hcp_data[k] = base
        k += 1
        print(k)

    # Dictionary of dictionaries to DF
    driver.quit()
    df =  pd.DataFrame(hcp_data)
    out = "truth_inquery/data/hcp_urls_" + state + ".csv"
    df.to_csv(out)
    return df
    