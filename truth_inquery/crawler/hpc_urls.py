# Collect healthcare provider clinic (HPC) URLs using CPC datasets
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from crawler import csv_extract, CPCIN, HPCIN, STATES

WAIT = 5
START = "https://www.abortionfinder.org"

def get_HPC_base(zips, state):
    """
    Navigates web browser to an abortion finder search and
    enters each CPC zip code to search for nearby abortion healthcare providers (HPCs).

    When available, the crawler collects distance, address, and url (base URL) for the
    top search result (ordered by distance in miles, alphabetical otherwise).
    Crawler does not require all data to be collected, hence many try/except blocks.

    Input:
        - zips (iterable/set): set of zip codes with at least one CPC in the state
        - state (str): US state 2 letter abbreviation ******************************
    
    Returns:
    df (dataframe) containing data for each inital search result URL (base URL) (str) with
        - List of sub-URLS for each base (list of lists)
        - Distance between CPC zip and HPC (str)
        - Address of HPC (str)
    """
    # Create webdriver client for Chrome
    driver = webdriver.Chrome()
    actions = ActionChains(driver)
    HPC_data = {}
    k = 1

    # Loop through CPC zip codes
    for z in zips:
        base = {}
        driver.get(START)

        # Enter zip code
        driver.find_element(By.XPATH, '//*[@id="location-box"]').send_keys(z)

        # Tab checkbox and fill
        for _ in range(3):
            actions.send_keys(Keys.TAB)
            actions.perform()

        actions.send_keys(Keys.RETURN)
        actions.perform()

        # Submit search
        driver.find_element(By.XPATH, '//button[@type="submit"]').click()
        time.sleep(WAIT)

        # Limit to physical locations, wait one second between clicks
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
        try:
            # First result
            driver.find_element(By.XPATH, '//div[@class="pill-link-container"]').click()
            time.sleep(WAIT)
            # Try to collect HPC website "Visit Website"
            ele = driver.find_element(By.XPATH, '//a[@class="clinic-link"]')
            base_url = ele.get_attribute("href")
            if not base_url:
                base_url = driver.current_url
        except NoSuchElementException:
            continue
        
        # Exclude if zipcode search results in abortion provider not in input state (state of CPC)
        try:
            address = driver.find_element(By.XPATH, '//section[@class="location-hours"]//p').text
            if state not in address:
                continue
        except NoSuchElementException:
            address = ''
        
        # Build dictionary
        base['url'] = base_url
        base['distance'] = distance
        base['address'] = address
        HPC_data[k] = base
        k += 1

    # Dictionary of dictionaries to DF
    driver.quit()
    df =  pd.DataFrame(HPC_data)
    df.to_csv(HPCIN.replace("state", state))
    return df #optional

# Collect URLs for all states with CPC data (where abortion legal)
if __name__ == "__main__":

    for state, name in STATES.items():
        HPCinput = CPCIN + name + " (" + state + ").csv"
        try:
            df = csv_extract(HPCinput)
        except FileNotFoundError:
            print(state, "file not in data set, skipping")
            continue

        zips = set(df['zip'].tolist())
        get_HPC_base(zips, state)
