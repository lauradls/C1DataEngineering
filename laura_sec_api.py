#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 21:32:27 2023

@author: lauradls
"""

import requests
import pandas as pd
import time
from datetime import datetime
import json
import re
#pip install sec-api
#pip install lxml
from sec_api import ExtractorApi
import time
from sec_api import XbrlApi
import requests
#import requests_random_user_agent




########################
#TRYING OUT SEC FREE API DATA
#########################
#GOAL: Obtain ticker, cik, and name of each bank
#########################

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
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13.4; rv:109.0) Gecko/20100101 Firefox/115.0'}

#get company specific filing metadata
filingMetadata= requests.get(
    f'https://data.sec.gov/submissions/CIK{cik}.json',
    headers=HEADERS
    
)

filingMetadata.raise_for_status()  # raises exception when not a 2xx response

if filingMetadata.status_code != 204:
    print(filingMetadata.json())
    
#HTTPError: 403 Client Error: Forbidden for url: https://data.sec.gov/submissions/CIK0000320193.json, fixed by fixinf the header

#it takes the most recent filings
print(filingMetadata.json().keys())
filingMetadata.json()['filings']
filingMetadata.json()['filings'].keys()
filingMetadata.json()['filings']['recent']
filingMetadata.json()['filings']['recent'].keys()

allForms = pd.DataFrame.from_dict(
    filingMetadata.json()['filings']['recent']
    )

allForms.columns
allForms[['accessionNumber', 'reportDate', 'form']].head(50)

#form colum has the '10k', '10-Q', etc..

#chose the 10q form, which is in index 46
allForms.iloc[46]

#https://github.com/AdamGetbags/secAPI/blob/main/secFilingScraper.py


######Look up all the CIK for a specific entity by only using part of the name

# banks_list = ['jpmorgan chase', 'morgan stanley', 'goldman sachs group', 'bank of america', 'wells fargo', 'citigroup', 'toronto dominion bank']
banks_list = ['JPM', 'MS', 'GS', 'BAC', 'WFC', 'C', 'USB']
cik_list = []
ticker_list = []
lender_name = []

for i in banks_list:
    # out = companyData[companyData['title'].str.contains(i, case=False)]
    out = companyData[companyData['ticker'] == i]
    if out.empty:
        continue
    else:
        cik_list.append(out.iloc[0,0])
        ticker_list.append(out.iloc[0,1])
        lender_name.append(out.iloc[0,2])



########################
#Use SEC-API package, 100 free API calls
#########################
#GOAL: Obtain balance sheet information for each 10-Q and 10-K in 2020
#########################


#Define API key
API_KEY = "8e313261bb867c7ef3affdc88a1f4b1a44fc5880c0ff8bed51043e3efb82eb23"


extractorApi = ExtractorApi(API_KEY) #Only works if statements are filled correctly
xbrlApi = XbrlApi(API_KEY) #works with all filling formats





-----


##################
#Functions
##################

#define this outside of the function
xbrlApi = XbrlApi(API_KEY)

#Created our own function to extract the data from the url, and transform it correctly into a dataset
def balance_sheet_df(url, ticker, cik, lender):
    
    #xbrl to json API
    json = xbrlApi.xbrl_to_json(htm_url=url)
    
    balance_sheets_data = json.get('BalanceSheets', []) #'BalanceSheets' is the key for how the .json is extracted
    
    data = {}
    for key, values in balance_sheets_data.items():
        value_list = []
        instant_list = []
        for entry in values[:1]:
            value_list.append(entry.get('value'))
            instant_list.append(entry.get('period', {}).get('instant'))
        data[key] = {'Value': value_list, 'Instant': instant_list}
    
    # Create DataFrame from the extracted values
    df = pd.DataFrame(data)
    
    #Update all values in the balance sheet to numeric
    ####
    df = df.loc['Value'].applymap(pd.to_numeric)
    
    #Add ticker, cik, and lender name columns
    df['Lender'] = lender
    df['Ticker'] = ticker 
    df['CIK'] = cik
    
    #Turn the 'Instant' cindex into a new column 'Date'
    df['Date']=df.loc['Instant', :].values[0] * len(df.index)
    
    #Only keep the 'Value' column, remove 'Instant' column
    df = df.drop('Instant')
    
    return df



def search_word_in_dataframes(word, dataframes_list):
    # List to store the matching DataFrame objects and their indices
    matching_dataframes = []

    # Iterate through each DataFrame in the list
    for i, df in enumerate(dataframes_list):
        # Flatten the DataFrame into a single string
        flattened_text = ' '.join(df.values.flatten().astype(str))

        # Check if the word is present in the flattened text
        if word in flattened_text:
            matching_dataframes.append((df, i))

    return matching_dataframes

# Call the function to search for "cash equivalents" in the list of dataframes
results = search_word_in_dataframes("cash equivalents", table_MN1)

# Print the matching DataFrame and its index number
for df, idx in results:
    print("Matching DataFrame at index:", idx)
    print(df)
    print()  # Add an empty line for clarity



##MS: table_MN1[134] for assets, "Total liabilities" table_MN1[319]
##MS: Entire table_MN1[293] 

# Confirm the currency that each bank uses to report their financials. 


def remove_text_before_keywords(cell):
    keywords = ['March', 'June','September', 'December']
    for keyword in keywords:
        index = cell.find(keyword)
        if index != -1:
            return cell[index:]
    return cell



#################
##XBRL JSON #####
################


#############
# 10-Q LOOP #
#############


#For loop to iterate through every lender and extract Q-10 balance sheet, use zip to combine the elements
for lender, ticker, cik in zip(lender_name, ticker_list, cik_list):
    
        #next lender
    if ticker == 'JPM':
        df_JPM = pd.DataFrame()
        
        #q1
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000019617/000001961720000299/corpq12020.htm"
        df_JPM = df_JPM.append(balance_sheet_df(url, ticker, cik, lender))
        
        #q2
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000019617/000001961720000372/corpq22020.htm"
        df_JPM = df_JPM.append(balance_sheet_df(url, ticker, cik, lender))
        
        #q3
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000019617/000001961720000438/jpm-20200930.htm"
        df_JPM = df_JPM.append(balance_sheet_df(url, ticker, cik, lender))
        
        #skip to the next iteration, save compute power for the next if statements
        continue
    
    
    #next lender 
    if ticker == 'MS':
        df_MS = pd.DataFrame()
        
        #Using f'string for the MS urls since there is a url pattern for 2020
        
        urls = [
        f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000323/{ticker}q{num}202010q.htm', #q1
        f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000429/{ticker}q{num}202010q.htm', #q2
        f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000527/{ticker}q{num}202010q.htm'  #q3
        ]
        
        for num, url in enumerate(urls, start=1):
            df_MS = df_MS.append(balance_sheet_df(url, ticker, cik, lender))

        continue
    


    
    #next lender
    if ticker == 'GS':
        df_GS = pd.DataFrame()
        
        # q1
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520129324/d907802d10q.htm"
        df_GS = df_GS.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q2
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520212201/d920934d10q.htm"
        df_GS = df_GS.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q3
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520282987/d25996d10q.htm"
        df_GS = df_GS.append(balance_sheet_df(url, ticker, cik, lender))
        
        # Skip to the next iteration, save compute power for the next if statements
        continue
    
    
    

    #next lender
    if ticker == 'BAC':
        df_BAC = pd.DataFrame()
        
        # q1
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000023/bac-0331202010xq.htm"
        df_BAC = df_BAC.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q2
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000040/bac-0630202010xq.htm"
        df_BAC = df_BAC.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q3
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000071/bac-20200930.htm"
        df_BAC = df_BAC.append(balance_sheet_df(url, ticker, cik, lender))
        
        # Skip to the next iteration, save compute power for the next if statements
        continue




    #next lender    
    if ticker == 'WFC':
        df_WFC = pd.DataFrame()
        
        # q1
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000236/wfc-0331x2020x10q.htm"
        df_WFC = df_WFC.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q2
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000288/wfc-0630x2020x10q.htm"
        df_WFC = df_WFC.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q3
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000338/wfc-20200930.htm"
        df_WFC = df_WFC.append(balance_sheet_df(url, ticker, cik, lender))
        
        # Skip to the next iteration, save compute power for the next if statements
        continue



    #next lender
    if ticker == 'C':
        df_C = pd.DataFrame()
        
        # q1
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000044/c-3312020x10q.htm"
        df_C = df_C.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q2
        url = "ttps://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000078/c-20200630.htm"
        df_C = df_C.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q3
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000110/c-20200930.htm"
        df_C = df_C.append(balance_sheet_df(url, ticker, cik, lender))
        
        # Skip to the next iteration, save compute power for the next if statements
        continue
    
    
    
    #next lender
    if ticker == 'USB':
        df_USB = pd.DataFrame()
        
        # q1
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520136359/d897119d10q.htm"
        df_USB = df_USB.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q2
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520211979/d890129d10q.htm"
        df_USB = df_USB.append(balance_sheet_df(url, ticker, cik, lender))
        
        # q3
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520286983/d947218d10q.htm"
        df_USB = df_USB.append(balance_sheet_df(url, ticker, cik, lender))

##END



#############
# 10-K LOOP #
#############



#This is a separate loop to add the 10-K information to each lender's dataframe. 
#Doing it separately because the line items on the quaterly consolidated balance sheet might be different than the ones on the yearly report. 

10kdate = '20201231'

for lender, ticker, cik in zip(lender_name, ticker_list, cik_list):
    
        #next lender
    if ticker == 'JPM':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000019617/000001961721000236/jpm-{10kdate}.htm"
        df_JPM = df_JPM.append(balance_sheet_df(url, ticker, cik, lender))
        
        
    if ticker == 'MS':
        url = 'https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542121000286/ms-{10kdate}.htm'
        df_MS = df_MS.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'GS':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312521049380/d39654d10k.htm"
        #df_GS = df_GS.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'BAC':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085821000023/bac-{10kdate}.htm"
        df_BAC = df_BAC.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'WFC':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297121000197/wfc-{10kdate}.htm"
        df_WFC = df_WFC.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'C':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100121000042/c-{10kdate}.htm"
        df_C = df_C.append(balance_sheet_df(url, ticker, cik, lender))
        
        
    if ticker == 'USB':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520286983/d947218d10q.htm"
        df_USB = df_USB.append(balance_sheet_df(url, ticker, cik, lender))
        
#REVIEW: BAC, WFC, USB #### 10-K
        
#############
#############
# Consolidate the dataframes to have homogeneous columns#
#############
#############


##Main dataframe, this follows the schema of our SQL table
columns = [
    'LenderID',
    'Ticker',
    'CIK',
    'DateId',
    'Section',
    'CashandCashEquivalents',
    'TradingAssetsFairValue',
    'SecuritiesPurchased',
    'SecuritiesBorrowed',
    'LoansMaturity',
    'LoansSale',
    'LoanLossAllowance',
    'Goodwill',
    'TangibleAssets',
    'TotalAssets',
    'Deposits',
    'ShortTermBorrowings',
    'LongTermBorrowings',
    'TradingLiabilities',
    'TotalLiabilities',
    'PreferredStock',
    'CommonStock',
    'AdditionalPaidInCapital',
    'RetainedEarnings',
    'AccCompIncomeLoss',
    'TreasuryStock',
    'EmployeeStockComp',
    'TotalEquity',
    'TotalLiabilitiesAndEquity'
]

df_final = pd.DataFrame(columns=columns)


##Organize each bank's dataframe to fit the above table's schema

for ticker in ticker_list:
    
        #next lender
    if ticker == 'JPM':
        df_JPM_final = df_cons.copy()
        
        #Assets
        df_JPM_final['CashandCashEquivalents'] = df_JPM[['CashAndDueFromBanks', 'InterestBearingDepositsInBanks', 'FederalFundsSoldAndSecuritiesPurchasedUnderAgreementsToResell']].sum()
        df_JPM_final[['LenderID','Ticker','CIK','DateId']] = df_JPM[['Lender', 'Ticker', 'CIK', 'Date']]
        df_JPM_final['SecuritiesBorrowed'] = df_JPM['SecuritiesBorrowed']
        df_JPM_final['Goodwill'] = df_JPM['GoodwillServicingAssetsatFairValueandOtherIntangibleAssets']
        df_JPM_final['TangibleAssets'] = df_JPM['PropertyPlantAndEquipmentNet']
        df_JPM_final['TradingAssets'] = df_JPM['TradingAssetsFairValue']
        df_JPM_final['SecuritiesPurchased'] = df_JPM[['AvailableForSaleSecurities',
                                                      'DebtSecuritiesHeldToMaturityNetOfAllowanceForCreditLosses', 'DebtSecuritiesNetCarryingAmount']]
        df_JPM_final['SecuritiesBorrowed'] = 0
        df_JPM_final['LoansMaturity'] = df_JPM['FinancingReceivableBeforeAllowanceForCreditLossesNetofDeferredIncome']
        df_JPM_final['LoansSale'] = 0
        df_JPM_final['LoanLossAllowance'] = df_JPM['FinancingReceivableAllowanceForCreditLosses']
        df_JPM_final['OtherAssets'] = df_JPM[['OtherAssets', 'NotesReceivableNet', 'AccruedInterestAndAccountsReceivable']]
        df_JPM_final['TotalAssets'] = df_JPM['Assets']
        
        #Liabilities
        df_JPM_final['Deposits'] = df_JPM['Deposits']
        df_JPM_final['ShortTermBorrowings'] = df_JPM['FederalFundsPurchasedSecuritiesSoldUnderAgreementsToRepurchase',
                    'ShortTermBorrowings']
        df_JPM_final['LongTermBorrowings'] = df_JPM ['LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities']
        df_JPM_final['TradingLiabilities'] = df_JPM['TradingLiabilities']
        df_JPM_final['OtherLiabilities'] = df_JPM['OtherLiabilities']
        
        #Equity
        df_JPM_final['PreferredStock'] = df_JPM['PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount']
        df_JPM_final['CommonStock'] = df_JPM['CommonStockValue', 'AdditionalPaidInCapitalCommonStock']
        df_JPM_final['AdditionalPaidInCapital'] = df_JPM['AdditionalPaidInCapitalCommonStock', 'PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount']
        df_JPM_final['RetainedEarnings'] = df_JPM['RetainedEarningsAccumulatedDeficit', 'EmployeeStockComp']
        df_JPM_final['AccCompIncomeLoss'] = df_JPM['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_JPM_final['TreasuryStock'] = df_JPM['CommonStockHeldInTrust', 'TreasuryStockValue']
        df_JPM_final['TotalEquity'] = df_JPM['StockholdersEquity']
        df_JPM_final['TotalLiabilitiesAndEquity'] = df_JPM['LiabilitiesAndStockholdersEquity']


# Print the DataFrame
print(df_JPM)

        

        df_JPM_cons
        
        
    if ticker == 'MS':
        url = 'https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542121000286/ms-{10kdate}.htm'
        df_MS = df_MS.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'GS':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312521049380/d39654d10k.htm"
        #df_GS = df_GS.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'BAC':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085821000023/bac-{10kdate}.htm"
        df_BAC = df_BAC.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'WFC':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297121000197/wfc-{10kdate}.htm"
        df_WFC = df_WFC.append(balance_sheet_df(url, ticker, cik, lender))
        
        
        
    if ticker == 'C':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100121000042/c-{10kdate}.htm"
        df_C = df_C.append(balance_sheet_df(url, ticker, cik, lender))
        
        
    if ticker == 'USB':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520286983/d947218d10q.htm"
        df_USB = df_USB.append(balance_sheet_df(url, ticker, cik, lender))
        


#333#######3333
##############3


df_bs_final = pd.DataFrame()



df_JPM.loc['Value'] = pd.to_numeric(df_JPM.loc['Value'])


    df.loc['Value'] = pd.to_numeric(df.loc['Value'], errors='coerce')
    df.dropna(subset=['Value'], inplace=True)

    df.loc['Value'] = pd.to_numeric(df.loc['Value'])



#########
#TRANSFORMATIONS FOR WHEN OTHER WORKED
########
        
                for num in range(1,3):
            url_10q = f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000323/{ticker}q{num}202010q.htm'
            print(ticker)
            section_text = extractorApi.get_section(url_10q, "part1item1", "html") # 
            table_MN1 = pd.read_html(section_text1)
            
            #table manipulation
            df_loop = table_MN1[0]
    
    
            #0th column = name of line items in the balance sheet
            #1st column = empty
            #2nd column = not valuable info
            #3rd column = figures
            #7th column = figures
            
            #1st row = As of Month dd
            #2nd row = YYYY
            
            df_loop = df_bs.iloc[1::, [0, 3, 7]].transpose()
            
            #Rename columns to match balance sheet items
            df_loop.columns = list(df_bs.iloc[:,0])
            print(ticker)
            
            #Remove row zero as it matches column names
            df_loop = df_loop.drop(index=0)
            
            
            #Rename columns 0 as date
            df_loop.columns.values[0] = 'Date'
            df_loop.columns.values[1] = 'Drop'
            
            
            #reset index to do a functional for loop
            df_loop.reset_index(inplace=True, drop=True)
            
            #concatenate the month_day + year columns into one. Add a space so that datetime functions can recognize it. 
            #convert to datetime obaject
            
            for i in range(len(df_loop.index)):
                print(i)
                date_string = df_loop.iloc[i,0]+' '+df_loop.iloc[i,1]
                df_loop.iloc[i, 0] = datetime.strptime(date_string, "%B %d, %Y")
                print(i)
                
            #Now that the date is in one place, drop the column that only has year information
            df_loop = df_loop.drop(df_loop.columns[1], axis=1)
            print(ticker)
            df_bst.append(df_loop)
        
    else:
        continue
        




#dictionary to dataframe, every key (company) should be a row so use orient = index

companyData = pd.DataFrame.from_dict(companyTickers.json(), orient='index')
companyData.describe()


######GS ##works well

gs_url =    "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520282987/d25996d10q.htm" #q3
gs_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520129324/d907802d10q.htm"
gs_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520212201/d920934d10q.htm"
gs_url_10k = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312521049380/d39654d10k.htm" 



###gs with xbrl

####BAC
bac_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000023/bac-0331202010xq.htm"
bac_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000040/bac-0630202010xq.htm"
bac_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000071/bac-20200930.htm"


####WELLS FARGO
wf_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000338/wfc-20200930.htm"

wf_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000338/wfc-20200930.htm"
wf_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000288/wfc-0630x2020x10q.htm"
wf_url_q1 ="https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000236/wfc-0331x2020x10q.htm"


###Citigroup

citi_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000110/c-20200930.htm"
citi_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000078/c-20200630.htm"
citi_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000044/c-3312020x10q.htm"



##US Bancorp

usb_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520286983/d947218d10q.htm"
usb_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520211979/d890129d10q.htm"
usb_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520136359/d897119d10q.htm"









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
f#iling_url_10q = "https://www.sec.gov/Archives/edgar/data/1318605/000095017022006034/tsla-20220331.htm"
#filing_url_10q = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001318605/000095017023013890/tsla-20230331.htm"

# extract section 1A "Risk Factors" in part 2 as cleaned text
#extracted_section_10q = extractorApi.get_section(filing_url_10q, "part2item1a", "text")

# sample_10q = requests.get("https://api.sec-api.io/extractor?", 
# url=filing_url_10q,
# item=part1item1,
# type=html,
# token="apikey)



# get the standardized and cleaned text of section
#section_text = extractorApi.get_section(filing_url_10q, "part1item1", "html") # 
#print(section_text)


#companyData = pd.DataFrame.from_dict(companyTickers.json(), orient='index')

#table_MN = pd.read_html(section_text)

#Balance Sheet dataframe
#df_bs = table_MN[0]



########################
#EXTRACTOR API
#########################


#0th column = name of line items in the balance sheet
#1st column = empty
#2nd column = not valuable info
#3rd column = figures
#7th column = figures

#1st row = As of Month dd
#2nd row = YYYY

#Select relevant columns and transpose

#df_bs = df_bs.iloc[1::, [0, 3, 7]].transpose()


#Rename columns to match balance sheet items
#df_bst.columns = list(df_bs.iloc[:,0])



#Remove row zero as it matches column names
#df_bst = df_bst.drop(index=0)


#Rename columns 0 as date
#df_bst.columns.values[0] = 'Date'
#df_bst.columns.values[1] = 'Drop'


#reset index to do a functional for loop
#df_bst.reset_index(inplace=True, drop=True)

#concatenate the month_day + year columns into one. Add a space so that datetime functions can recognize it. 
#convert to datetime obaject

#for i in range(len(df_bst.index)):
#    print(i)
#    date_string = df_bst.iloc[i,0]+' '+df_bst.iloc[i,1]
#    df_bst.iloc[i, 0] = datetime.strptime(date_string, "%B %d, %Y")
#    print(i)
    
#Now that the date is in one place, drop the column that only has year information
#df_bst = df_bst.drop(df_bst.columns[1], axis=1)
    



##################################
####Clean up for MS, USED AFTER APPLYING THE WORD SERACH search_word_in_dataframes, remove_text_before_keywords FUNCTIONS
##################################
#df_loop12 = table_MN1[293]



#0th column = name of line items in the balance sheet
#1st column = mix of correct figures for the current bs date, and empty $ fields (exclude)
#2nd column = accurate bs figures

#1st row = As of Month dd
#2nd row = YYYY

#Select relevant columns and transpose

#df_loop1 = df_loop12.iloc[0::, [0,2]].transpose()


#Rename columns to match balance sheet items
#df_loop1.columns = list(df_loop12.iloc[:,0]) #match column names to cacoo table



#Remove row zero as it matches column names, and the first two columns NaN
#Remove the first row, repeats the column names
#df_loop1 = df_loop1.iloc[1::, 2:].reset_index(drop=True)



#Rename columns 0 as date
#df_loop1.columns.values[0] = 'Date'

#reset index to do a functional for loop
#df_loop1.reset_index(inplace=True, drop=True)

#convert this cell into a string format, appropriate for the datetime function to work
#convert to datetime object

#for i in range(len(df_loop1.index)):
 #      print(i)
  #     date_string = remove_text_before_keywords(df_loop1.iloc[i, 0])
   #    df_loop1.iloc[i, 0] = datetime.strptime(date_string, "%B %d, %Y")
      
       

# print(ticker)
# df_bst.append(df_loop)


# ticker == 'MS'
        
# cik='0000895421'
# ticker = 'MS'
# for num in range(1,4):
#     url_10q1 = f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000323/{ticker}q{num}202010q.htm'
#     print(ticker)
#     section_text23 = extractorApi.get_section(url_10q1, "part1item1", "html") # 
#     table_MN23 = pd.read_html(section_text23)
#             
#             #table manipulation
#             df_loop = table_MN1[0]






# ####Go through different CIKs from the lists above


# #get company specific filing metadata
# for cik, ticker in cik_list, ticker_list:
#     if ticker == 'MS':
        
#         #cik='0000895421'
#         #ticker = 'MS'
#         for num in range(1,3):
#             url_10q = f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000323/{ticker}q{num}202010q.htm'
#             print(ticker)
#             section_text = extractorApi.get_section(url_10q, "part1item1", "html") # 
#             table_MN1 = pd.read_html(section_text1)
            
#             #table manipulation
#             df_loop = table_MN1[0]
    
    
#             #0th column = name of line items in the balance sheet
#             #1st column = empty
#             #2nd column = not valuable info
#             #3rd column = figures
#             #7th column = figures
            
#             #1st row = As of Month dd
#             #2nd row = YYYY
            
#             df_loop = df_bs.iloc[1::, [0, 3, 7]].transpose()
            
#             #Rename columns to match balance sheet items
#             df_loop.columns = list(df_bs.iloc[:,0])
#             print(ticker)
            
#             #Remove row zero as it matches column names
#             df_loop = df_loop.drop(index=0)
            
            
#             #Rename columns 0 as date
#             df_loop.columns.values[0] = 'Date'
#             df_loop.columns.values[1] = 'Drop'
            
            
#             #reset index to do a functional for loop
#             df_loop.reset_index(inplace=True, drop=True)
            
#             #concatenate the month_day + year columns into one. Add a space so that datetime functions can recognize it. 
#             #convert to datetime obaject
            
#             for i in range(len(df_loop.index)):
#                 print(i)
#                 date_string = df_loop.iloc[i,0]+' '+df_loop.iloc[i,1]
#                 df_loop.iloc[i, 0] = datetime.strptime(date_string, "%B %d, %Y")
#                 print(i)
                
#             #Now that the date is in one place, drop the column that only has year information
#             df_loop = df_loop.drop(df_loop.columns[1], axis=1)
#             print(ticker)
#             df_bst.append(df_loop)
        
#     else:
#         continue


# filingMetadata= requests.get(
#     f'https://data.sec.gov/submissions/CIK{cik}.json',
#     headers=headers
# )


# testing_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542120000527/msq3202010q.htm"


#  # 

# section_text3 = extractorApi.get_section(testing_q3, "part1item1", "html") # 
# testing_q3_df = pd.read_html(str(section_text3))


# #extraction is messed up for the other 2 10q
# ticker == 'MS':
        
#         cik='0000895421'
#         ticker = 'MS'
#         for num in range(1,4):
#             url_10q = f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000323/{ticker}q{num}202010q.htm'
#             print(ticker)
#             section_text = extractorApi.get_section(url_10q, "part1item1", "html") # 
#             table_MN1 = pd.read_html(section_text1)
            
#             #table manipulation
#             df_loop = table_MN1[0]




# # MS  10-Q filing
# testing_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542120000429/msq2202010q.htm"
# testing_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542120000323/msq1202010q.htm"
# testing_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542120000323/msq2202010q.htm"
# section_text24 = extractorApi.get_section(testing_q2, "part1item1", "html")
# table_MN24 = pd.read_html(section_text24)
            
# #filing_url_10q = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001318605/000095017023013890/tsla-20230331.htm"

# # extract section 1A "Risk Factors" in part 2 as cleaned text
# section_text2 = extractorApi.get_section(testing_q2, "part1item1", "html")

# # sample_10q = requests.get("https://api.sec-api.io/extractor?", 
# # url=filing_url_10q,
# # item=part1item1,
# # type=html,
# # token="apikey)

# table_MN2 = pd.read_html(section_text2)

# # get the standardized and cleaned text of section
# print(section_text)




# cik='0000895421'
# ticker = 'MS'
# termination = ['0323', '0429', '0527']
# for num in range(1,4):
#     #the range from 1-4 is for Q1-Q4
#             url_10q = f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000323/{ticker}q{num}202010q.htm'
#             url_10q = f'https://www.sec.gov/ix?doc=/Archives/edgar/data/{cik}/{cik}20000429/{ticker}q{num}202010q.htm'
#             url_10q = f'https

