# Aaron Haefner
# Collect healthcare provider clinic (HPC) URLs using CPC datasets
"""
- clone repo - 
cd 30122-project-truth-inquery
poetry install
** DEMO SELENIUM **
poetry run python truth_inquery/crawler/hpc_urls.py <state abbreviation>
"""
import sys
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

WAIT = 5
START = "https://www.abortionfinder.org"

CPCIN = "truth_inquery/data/CPC_"
CPCOUT = "truth_inquery/output/CPC_state_clinics.csv"
HPCIN = "truth_inquery/data/HPC_urls_state.csv"
HPCOUT = "truth_inquery/output/HPC_state_clinics.csv"

# source: https://gist.github.com/rogerallen/1583593
STATES = {
    'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticwut', 'DC': 'District of Columbia', 'DE': 'Delaware',
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

def csv_extract(input_file):
    """
    Loads CPC csv input file and extracts data from columns

    Inputs:
        input_file (str): File path to .csv file

    Returns: dataframe with standardized zip and url columns
    """
    f = pd.read_csv(input_file)
    df = f[f['Website'].notna()]
    df = df[['Name ', 'Zip Code', 'State', 'Website']]

    # Rename columns, front-fill zipcode with zeros.
    df = df.rename(columns={'Name ':'name', 'Zip Code':'zip', 'State':'state', 'Website':'url'})
    df['zip'] = df['zip'].astype(str)
    df['zip'] = df['zip'].str.zfill(5)
    df['url'] = df['url']
    return df

def get_HPC_base(zips, state):
    """
    Navigates web browser to an abortion finder search and
    enters each CPC zip code to search for nearby abortion healthcare providers (HPCs).

    When available, the crawler collects distance, address, and url (base URL) for the
    top search result (ordered by distance in miles, alphabetical otherwise).

    Input:
        - zips (iterable/set): set of zip codes with at least one CPC in the state
        - state (str): US state 2 letter abbreviation

    Returns: None, writes URL-level dataset to csv used for crawling
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
    df = df.transpose()

    df.to_csv(HPCIN.replace("state", state))

# Collect URLs for one state with CPC data (where abortion legal)
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "usage: python {} <state abbrev>".format(sys.argv[0])
        )
        sys.exit(1)
    state = sys.argv[1]
    name = STATES[state]
    HPCinput = CPCIN + name + " (" + state + ").csv"

    try:
        df = csv_extract(HPCinput)
    except FileNotFoundError:
        print(state, "file not in data set, aborting")
        sys.exit(1)

    zips = set(df['zip'].tolist())
    get_HPC_base(zips, state)
