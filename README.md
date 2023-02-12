# Project Truth InQuery

## Data Sources
### Abortion Policy API

### Data collection
1. We collect data from crisis pregnancy center (CPC) websites and up to 50 URLs to which the base CPC website links. 
We use data from the advocacy organization ([Expose Fake Clincs](https://www.exposefakeclinics.com/)) which contains information on CPCs by state. 


2. We collect and clean data in the same way using networks of URLs of real healthcare clinics providing reproductive and contraceptive care which come from ([Abortion Finder](https://www.abortionfinder.org/)). 

### Data Cleaning
1. Tokens are collected and counted if they do not contain characters matching `r'\W+'`.
2. Tokens are aggregated by state and saved in CSV files.