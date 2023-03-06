# Project Truth InQuery

to run the project with one line, run:

```python
poetry run python truth_inquery
```

note that the crawler takes quite a while (days) to run fully, so this will not execute the crawler collected portion of the data pipeline. 





## Data Sources
### Abortion Policy API

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
