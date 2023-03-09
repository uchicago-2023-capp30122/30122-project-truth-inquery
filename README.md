# Project Truth InQuery

## Description
We download data from [Expose Fake Clinics](https://www.exposefakeclinics.com) that contains information on Crisis Pregnancy Centers (CPCs) by U.S. state. We CPC zip codes to collect the website and other information on nearby legitimate healthcare providers that provide abortion and other family planning health services. We crawl the websites of CPCs and the legitimate clinics (referred to as HPCs) to identify the top terms in both sets of URLs.

We also use data from the [Abortion Policy API](https://www.abortionpolicyapi.com). These data are include state-level policy indicators and other measures of abortion care access in the state. Using these data sources, our goal was to train a model that could classify a URL as a CPC or not.

## Installation
After cloning the repo, you can set up the project as follows:

```python
cd 30122-project-truth-inquery
poetry install
```

You'll next need to add the API key to 30122-project-truth-inquery/truth_inquery/database_model/api_requests.py.

You can run the entire project from the command line using the following commands:

1. We use a separate command to crawl URLs for data since it takes more than a day to complete. This command crawls crisis pregnancy center websites then collects the websites of nearby healthcare providers websites and crawls those.

```python
poetry run python truth_inquery/crawler
```

2. The databases are created and regression analysis completed using,
```python
poetry run python truth_inquery "keyword"
```
where keyword is the word you're interested in using as an analysis term. We check that this is a string, so please include " " on either side of the term. 

3. Finally, all of the graph output is generated using, 
```python
poetry run python truth_inquery/crawler/graphs.py int
```
where int refers to the number of edges in a single-clinic graph. Our output uses 15.
