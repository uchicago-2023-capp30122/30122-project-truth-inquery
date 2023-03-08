# Project Truth InQuery

After cloning the repo, you can set up the project as follows:

```python
cd 30122-project-truth-inquery
poetry install
```

You can run the entire project from the command line using the following commands:

1. We use a separate command to crawl URLs for data since it takes more than a day to complete. This command crawls crisis pregnancy center websites then collects the websites of nearby healthcare providers websites and crawls those.

```python
poetry run python truth_inquery/crawler
```

2. The databases are created and regression analysis completed using,
```python
poetry run python truth_inquery
```

3. Finally, all of the graph output is generated using, 
```python
poetry run python truth_inquery/crawler/graphs.py 15
```



## Data Sources
### Abortion Policy API
1. We requested and were granted access to the ([Abortion Policy API](https://www.abortionpolicyapi.com/)) which contains information on abortion policy by state. We use three variables from this API (waiting period required by law, number of counseling visits mandated by law, and a measure of how many weeks after last menstrual period (LMP) aboriton is legally restricted in that state) as controls to account for the variation across states. 
Note: We have sent James our API Key that is required to generate a portion of the data this project relies upon through API calls. 

### Data collection
1. We collect data from crisis pregnancy center (CPC) websites and up to 50 URLs to which the base CPC website links. 
We use labeled data from the advocacy organization ([Expose Fake Clincs](https://www.exposefakeclinics.com/)) which contains information on CPCs by state. 

We identify legitimate and nearby healthcare providers by searching the 
site using the provided CPC zip code. 

2. We collect and clean data in the same way using networks of URLs of real healthcare clinics providing reproductive and contraceptive care which come from ([Abortion Finder](https://www.abortionfinder.org/)). 

### Database

## Analysis


### Model

### Data visualization
