#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 21:32:27 2023

@author: lauradls
"""

import requests
import pandas as pd
import time


#create request user header
headers = {'User Agent': "myemail@gmail.com"}

#get all companies data
companyTickers = requests.get(
"https://www.sec.gov/files/company_tickers.json", 
headers=headers)

#review response/keys to see if it was succesful 
print(companyTickers.json().keys())

#format response to dictionary and get first key/value
firstentry = companyTickers.json()['0']
print(firstentry)


#dictionary to dataframe, every key (company) should be a row so use orient = index

companyData = pd.DataFrame.from_dict(companyTickers.json(), orient='index')
companyData.describe()

time.sleep(10)

#add leading zeros to CIK, not included from the CIK link endpoint
companyData['cik_str'] = companyData['cik_str'].astype(str).str.zfill(10)


#Let's try an example

cik = companyData[0:1].cik_str[0]

HEADERS = {'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148'}

#get company specific filing metadata
filingMetadata= requests.get(
    f'https://data.sec.gov/submissions/CIK{cik}.json',
    headers=HEADERS
    
)

filingMetadata.raise_for_status()  # raises exception when not a 2xx response

if filingMetadata.status_code != 204:
    print(filingMetadata.json())
    
#HTTPError: 403 Client Error: Forbidden for url: https://data.sec.gov/submissions/CIK0000320193.json


###########The below line is the one generating the error##############

print(filingMetadata.json().keys())
filingMetadata.json()['filings']
filingMetadata.json()['filings'].keys()
filingMetadata.json()['filings']['recent']
filingMetadata.json()['filings']['recent'].keys()


######Look up all the CIK for a specific entity by only using part of the name

banks_list = ['jp morgan', 'morgan stanley', 'goldman sachs group', 'bank of america', 'wells fargo', 'citi bank', 'td bank']
cik_list = []
ticker_list = []

for i in banks_list:
    out = companyData[companyData['title'].str.contains(i, case=False)]
    if out.empty:
        continue
    else:
        cik_list.append(out.sort_values(by='ticker').iloc[0,0])
        ticker_list.append(out.sort_values(by='ticker').iloc[0,1])




#Focus on Bank of America, JPMorgan, Wells Fargo, Citi Bank, TD Bank


#####A different way to get the data###### SEC API, third party

#pip install sec-api
#pip install lxml
from sec_api import ExtractorApi
import requests
import pandas as pd
#import requests_random_user_agent
import time
from datetime import datetime





#Define API key
extractorApi = ExtractorApi("YOURAPI")

#
# 10-K example
#
# Tesla 10-K filing
# filing_url_10k = "https://www.sec.gov/Archives/edgar/data/1318605/000156459021004599/tsla-10k_20201231.htm"

# # get the standardized and cleaned text of section 1A "Risk Factors"
# section_text = extractorApi.get_section(filing_url_10k, "1A", "text")

# # get the original HTML of section 7 "Management’s Discussion and Analysis of Financial Condition and Results of Operations"
# section_html = extractorApi.get_section(filing_url_10k, "7", "html")

#
# 10-Q example
#
# Tesla 10-Q filing
filing_url_10q = "https://www.sec.gov/Archives/edgar/data/1318605/000095017022006034/tsla-20220331.htm"
#filing_url_10q = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001318605/000095017023013890/tsla-20230331.htm"

# extract section 1A "Risk Factors" in part 2 as cleaned text
extracted_section_10q = extractorApi.get_section(filing_url_10q, "part2item1a", "text")

# sample_10q = requests.get("https://api.sec-api.io/extractor?", 
# url=filing_url_10q,
# item=part1item1,
# type=html,
# token="apikey)



# get the standardized and cleaned text of section
section_text = extractorApi.get_section(filing_url_10q, "part1item1", "html") # 
print(section_text)


companyData = pd.DataFrame.from_dict(companyTickers.json(), orient='index')

table_MN = pd.read_html(section_text)

#Balance Sheet dataframe
df_bs = table_MN[0]


#0th column = name of line items in the balance sheet
#1st column = empty
#2nd column = not valuable info
#3rd column = figures
#7th column = figures

#1st row = As of Month dd
#2nd row = YYYY

df_bs = df_bs.iloc[1::, [0, 3, 7]]


#Transpose the dataframe
df_bst = df_bs.transpose()


#Rename columns to match balance sheet items
df_bst.columns = list(df_bs.iloc[:,0])



#Remove row zero as it matches column names
df_bst = df_bst.drop(index=0)


#Rename columns 0 as date
df_bst.columns.values[0] = 'Date'
df_bst.columns.values[1] = 'Drop'


#reset index to do a functional for loop
df_bst.reset_index(inplace=True, drop=True)

#concatenate the month_day + year columns into one. Add a space so that datetime functions can recognize it. 
#convert to datetime obaject

for i in range(len(df_bst.index)):
    print(i)
    date_string = df_bst.iloc[i,0]+' '+df_bst.iloc[i,1]
    df_bst.iloc[i, 0] = datetime.strptime(date_string, "%B %d, %Y")
    print(i)
    
#Now that the date is in one place, drop the column that only has year information
df_bst = df_bst.drop(df_bst.columns[1], axis=1)
    




####Go thorugh different CIK

cik = [0000072971, 0000713676, 0000105982]#wells fargo, pnc bank
#jp morgan chase, morgan stanley, goldman sachs



"https://www.sec.gov/Archives/edgar/data/1318605/000095017022006034/tsla-20220331.htm"
"https://www.sec.gov/ix?doc=/Archives/edgar/data/72971/000007297123000102/wfc-20230331.htm"
https://www.sec.gov/ix?doc=/Archives/edgar/data/713676/000071367623000037/pnc-20230331.htm


#get company specific filing metadata
filingMetadata= requests.get(
    f'https://data.sec.gov/submissions/CIK{cik}.json',
    headers=headers
)

