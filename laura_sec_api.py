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
API_KEY = "your_API_key"


extractorApi = ExtractorApi(API_KEY) #Only works if statements are filled correctly
xbrlApi = XbrlApi(API_KEY) #works with all filling formats




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
    #df = df.loc['Value'].applymap(pd.to_numeric)
    
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

date10k = '20201231'

for lender, ticker, cik in zip(lender_name, ticker_list, cik_list):
    
        #next lender
    if ticker == 'JPM':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000019617/000001961721000236/jpm-{date10k }.htm"
        df_JPM = df_JPM.append(balance_sheet_df(url, ticker, cik, lender))
        continue
        
        
    if ticker == 'MS':
        url = 'https://www.sec.gov/ix?doc=/Archives/edgar/data/0000895421/000089542121000286/ms-{date10k }.htm'
        df_MS = df_MS.append(balance_sheet_df(url, ticker, cik, lender))
        continue
        
        
    if ticker == 'GS':
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312521049380/d39654d10k.htm"
        #df_GS = df_GS.append(balance_sheet_df(url, ticker, cik, lender))
        continue
        
        
    if ticker == 'BAC': ##REVIEW
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085821000023/bac-{date10k}.htm"
        df_BAC = df_BAC.append(balance_sheet_df(url, ticker, cik, lender))
        continue
        
        
    if ticker == 'WFC': ##REVIEW
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297121000197/wfc-{date10k}.htm"
        df_WFC = df_WFC.append(balance_sheet_df(url, ticker, cik, lender))
        continue
        
        
    if ticker == 'C':
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100121000042/c-{date10k}.htm"
        df_C = df_C.append(balance_sheet_df(url, ticker, cik, lender))
        continue
        
    if ticker == 'USB': ###REVIEW
        #10k
        url = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312521052547/d14650d10k.htm"
        df_USB = df_USB.append(balance_sheet_df(url, ticker, cik, lender))
        continue
    
    
    
#####convert data tu numeric again.....

df_list = [df_JPM, df_MS, df_GS, df_BAC, df_WFC, df_C, df_USB]


for i in df_list:
    i.loc[:, ~i.columns.isin(['Lender','Ticker','CIK','Date'])] = i.loc[:, ~i.columns.isin(['Lender','Ticker','CIK','Date'])].applymap(pd.to_numeric)
        
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
    'TradingAssets',
    'FedFundsSecuritiesPurchased',
    'SecuritiesBorrowed',
    'DebtMaturity',
    'DebtForSale',
    'LoanLossAllowance',
    'Goodwill',
    'TangibleAssets',
    'OtherAssets',
    'TotalAssets',
    'Deposits',
    'ShortTermBorrowings',
    'LongTermBorrowings',
    'TradingLiabilities',
    'OtherLiabilities',
    'TotalLiabilities',
    'PreferredStock',
    'CommonStock',
    'AdditionalPaidInCapital',
    'RetainedEarnings',
    'AccCompIncomeLoss',
    'TreasuryStock',
    'EmployeeStockComp',
    'TotalEquity',
    'TotalLiabilitiesAndStockholdersEquity'
]

df_final = pd.DataFrame(columns=columns)





##Organize each bank's dataframe to fit the above table's schema

for ticker in ticker_list:
    
    #next lender
    if ticker == 'JPM':
        df_JPM_final = df_final.copy()
        
        
        df_JPM_final[['LenderID','Ticker','CIK','DateId']] = df_JPM[['Lender', 'Ticker', 'CIK', 'Date']]
        
        #Assets
        df_JPM_final['CashandCashEquivalents'] = df_JPM['CashAndDueFromBanks'] + df_JPM['InterestBearingDepositsInBanks']
        df_JPM_final['FedFundsSecuritiesPurchased'] = df_JPM['FederalFundsSoldAndSecuritiesPurchasedUnderAgreementsToResell']
        df_JPM_final['SecuritiesBorrowed'] = df_JPM['SecuritiesBorrowed']
        df_JPM_final['Goodwill'] = df_JPM['GoodwillServicingAssetsatFairValueandOtherIntangibleAssets']
        df_JPM_final['TangibleAssets'] = df_JPM['PropertyPlantAndEquipmentNet']
        df_JPM_final['TradingAssets'] = df_JPM['TradingAssets'] + df_JPM['AvailableForSaleSecuritiesDebtSecurities']
        df_JPM_final['DebtForSale'] = df_JPM['DebtSecuritiesNetCarryingAmount'] + df_JPM['FinancingReceivableBeforeAllowanceForCreditLossesNetofDeferredIncome']
        df_JPM_final['DebtMaturity'] = df_JPM['DebtSecuritiesHeldToMaturityNetOfAllowanceForCreditLosses']
        df_JPM_final['LoanLossAllowance'] = df_JPM['FinancingReceivableAllowanceForCreditLosses']
        df_JPM_final['OtherAssets'] = df_JPM['OtherAssets'] + df_JPM['NotesReceivableNet'] + df_JPM['AccruedInterestAndAccountsReceivable']
        df_JPM_final['TotalAssets'] = df_JPM['Assets']
        
        #Liabilities
        df_JPM_final['Deposits'] = df_JPM['Deposits']
        df_JPM_final['ShortTermBorrowings'] = df_JPM['FederalFundsPurchasedSecuritiesSoldUnderAgreementsToRepurchase'] + df_JPM['ShortTermBorrowings']
        df_JPM_final['LongTermBorrowings'] = df_JPM ['LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities']
        df_JPM_final['TradingLiabilities'] = df_JPM['TradingLiabilities']
        df_JPM_final['OtherLiabilities'] = df_JPM['OtherLiabilities']
        
        #Equity
        df_JPM_final['PreferredStock'] = df_JPM['PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount']
        df_JPM_final['CommonStock'] = df_JPM['CommonStockValue'] + df_JPM['AdditionalPaidInCapitalCommonStock']
        df_JPM_final['AdditionalPaidInCapital'] = df_JPM['AdditionalPaidInCapitalCommonStock']
        df_JPM_final['RetainedEarnings'] = df_JPM['RetainedEarningsAccumulatedDeficit']
        df_JPM_final['AccCompIncomeLoss'] = df_JPM['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_JPM_final['TreasuryStock'] = df_JPM['CommonStockHeldInTrust'] + df_JPM['TreasuryStockValue']
        df_JPM_final['TotalEquity'] = df_JPM['StockholdersEquity']
        df_JPM_final['TotalLiabilitiesAndStockholdersEquity'] = df_JPM['LiabilitiesAndStockholdersEquity']
        
        continue
    
    #next lender
    if ticker == 'MS':
        df_MS_final = df_final.copy()
        
        df_MS_final[['LenderID','Ticker','CIK','DateId']] = df_MS[['Lender', 'Ticker', 'CIK', 'Date']]

        # Assets
        df_MS_final['CashandCashEquivalents'] = df_MS['CashAndCashEquivalentsAtCarryingValue']
        df_MS_final['TradingAssets'] = df_MS['TradingAssetsFairValueDisclosure']
        df_MS_final['FedFundsSecuritiesPurchased'] = df_MS['SecuritiesPurchasedUnderAgreementsToResell']
        df_MS_final['SecuritiesBorrowed'] = df_MS['SecuritiesBorrowed']
        df_MS_final['DebtMaturity'] = df_MS['DebtSecuritiesAvailableForSaleAndHeldToMaturity'] 
        df_MS_final['DebtForSale'] = df_MS['LoansReceivableHeldForSaleNetNotPartOfDisposalGroup'] + df_MS['LoansHeldforInvestment']
        df_MS_final['Goodwill'] = df_MS['Goodwill']
        df_MS_final['OtherAssets'] = df_MS['OtherReceivables'] + df_MS['OtherAssets']
        df_MS_final['TotalAssets'] = df_MS['Assets']
        
        #Liabilities
        df_MS_final['Deposits'] = df_MS['Deposits']
        df_MS_final['ShortTermBorrowings'] = df_MS['SecuritiesSoldUnderAgreementsToRepurchase']
        df_MS_final['LongTermBorrowings'] = df_MS['DebtLongtermAndShorttermCombinedAmount'] - df_MS_final['ShortTermBorrowings']
        #df_MS_final['TradingLiabilities'] = df_MS['TradingLiabilities']
        df_MS_final['TotalLiabilities'] = df_MS['Liabilities']
        
        #Equity
        df_MS_final['PreferredStock'] = df_MS['PreferredStockCarryingValue']
        df_MS_final['CommonStock'] = df_MS['CommonStockValue']
        df_MS_final['AdditionalPaidInCapital'] = df_MS['AdditionalPaidInCapital']
        df_MS_final['RetainedEarnings'] = df_MS['RetainedEarningsAccumulatedDeficit']
        df_MS_final['AccCompIncomeLoss'] = df_MS['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_MS_final['TreasuryStock'] = df_MS['TreasuryStockValue']
        df_MS_final['EmployeeStockComp'] = df_MS['CapitalAccumulationPlans']
        df_MS_final['TotalEquity'] = df_MS['StockholdersEquity']
        df_MS_final['TotalLiabilitiesAndStockholdersEquity'] = df_MS['LiabilitiesAndStockholdersEquity']
        
        continue
    
    #next lender
    if ticker == 'GS':
        df_GS_final = df_final.copy()
        
        df_GS_final[['LenderID','Ticker','CIK','DateId']] = df_GS[['Lender', 'Ticker', 'CIK', 'Date']]

        #Assets
        df_GS_final['CashandCashEquivalents'] = df_GS['CashAndCashEquivalentsAtCarryingValue']
        df_GS_final['TradingAssets'] = df_GS['TradingAssetsIncludingPledge'] + df_GS['InvestmentIncludingPledge']
        df_GS_final['SecuritiesBorrowed'] = df_GS['SecuritiesBorrowed']
        df_GS_final['DebtForSale'] = df_GS['LoansAndLeasesReceivableNetReportedAmount']
        #df_GS_final['LoanLossAllowance'] = df_GS['LoanLossAllowance']
        #df_GS_final['Goodwill'] = df_GS['Goodwill']
        #df_GS_final['TangibleAssets'] = df_GS['TangibleAssets']
        df_GS_final['OtherAssets'] = df_GS['CustomerAndOtherReceivables'] + df_GS['OtherAssets']
        df_GS_final['TotalAssets'] = df_GS['Assets']
        
        #Liabilities
        df_GS_final['Deposits'] = df_GS['Deposits']
        df_GS_final['ShortTermBorrowings'] = df_GS['SecuritiesSoldUnderAgreementsToRepurchase'] +  df_GS['UnsecuredShortTermBorrowingsIncludingCurrentPortionOfUnsecuredLongTermBorrowings'] - df_GS['UnsecuredLongTermDebt']
        df_GS_final['LongTermBorrowings'] = df_GS['UnsecuredLongTermDebt']
        df_GS_final['TradingLiabilities'] = df_GS['TradingLiabilities']
        df_GS_final['TotalLiabilities'] = df_GS['Liabilities']
        
        #Equity
        df_GS_final['PreferredStock'] = df_GS['CommonStockValue']
        df_GS_final['CommonStock'] = df_GS['CommonStockValue'] + df_GS['NonvotingCommonStock']
        df_GS_final['AdditionalPaidInCapital'] = df_GS['AdditionalPaidInCapital']
        df_GS_final['RetainedEarnings'] = df_GS['RetainedEarningsAccumulatedDeficit']
        df_GS_final['AccCompIncomeLoss'] = df_GS['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_GS_final['TreasuryStock'] = df_GS['TreasuryStockValue']
        df_GS_final['EmployeeStockComp'] = df_GS['ShareBasedAwards']
        df_GS_final['TotalEquity'] = df_GS['StockholdersEquity']
        df_GS_final['TotalLiabilitiesAndStockholdersEquity'] = df_GS['LiabilitiesAndStockholdersEquity']
        
        continue


    #next lender
    if ticker == 'BAC':
        df_BAC_final = df_final.copy()
        
        df_BAC_final[['LenderID','Ticker','CIK','DateId']] = df_BAC[['Lender', 'Ticker', 'CIK', 'Date']]

        # Assets
        df_BAC_final['CashandCashEquivalents'] = df_BAC['CashAndDueFromBanks'] + df_BAC['InterestBearingDepositsInBanks'] + df_BAC['CashAndCashEquivalentsAtCarryingValue']# + df_BAC['CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents']
        df_BAC_final['TradingAssets'] = df_BAC['TradingSecurities'] + df_BAC['DerivativeAssets']
        df_BAC_final['FedFundsSecuritiesPurchased'] = df_BAC['FederalFundsSoldAndSecuritiesPurchasedUnderAgreementsToResell'] + df_BAC['MarketableSecurities']
        df_BAC_final['DebtMaturity'] = df_BAC['DebtSecuritiesHeldToMaturityNetOfAllowanceForCreditLosses']
        df_BAC_final['DebtForSale'] = df_BAC['LoansReceivableHeldForSaleNetNotPartOfDisposalGroup'] + df_BAC['DebtSecuritiesCarriedAtFairValue'] + df_BAC['LoansAndLeasesReceivableNetReportedAmount']
        df_BAC_final['LoanLossAllowance'] = df_BAC['LoansAndLeasesReceivableAllowance'] 
        df_BAC_final['Goodwill'] = df_BAC['Goodwill']
        df_BAC_final['TangibleAssets'] = df_BAC['PropertyPlantAndEquipmentNet']
        df_BAC_final['OtherAssets'] = df_BAC['TimeDepositsAndOtherShortTermInvestments'] + df_BAC['OtherReceivables'] + df_BAC['OtherAssets']
        df_BAC_final['TotalAssets'] = df_BAC['Assets']
        
        #Liabilities
        df_BAC_final['Deposits'] = df_BAC['Deposits']
        df_BAC_final['ShortTermBorrowings'] = df_BAC['FederalFundsPurchasedAndSecuritiesSoldUnderAgreementsToRepurchase'] + df_BAC['OtherShortTermBorrowings']
        df_BAC_final['LongTermBorrowings'] = df_BAC['LongTermDebt']
        df_BAC_final['TradingLiabilities'] = df_BAC['TradingLiabilities']
        df_BAC_final['TotalLiabilities'] = df_BAC['Liabilities']
        
        #Equity
        df_BAC_final['PreferredStock'] = df_BAC['PreferredStockRedeemableandNonRedeemableValue']
        df_BAC_final['CommonStock'] = df_BAC['CommonStocksIncludingAdditionalPaidInCapital'] #includes additional paid-in-capital
        df_BAC_final['AdditionalPaidInCapital'] = df_BAC['CommonStocksIncludingAdditionalPaidInCapital']
        df_BAC_final['RetainedEarnings'] = df_BAC['RetainedEarningsAccumulatedDeficit']
        df_BAC_final['AccCompIncomeLoss'] = df_BAC['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_BAC_final['TotalEquity'] = df_BAC['StockholdersEquity']
        df_BAC_final['TotalLiabilitiesAndStockholdersEquity'] = df_BAC['LiabilitiesAndStockholdersEquity']
        
        
        continue
    

    #next lender
    if ticker == 'WFC':
        df_WFC_final = df_final.copy()
        
        df_WFC_final[['LenderID','Ticker','CIK','DateId']] = df_WFC[['Lender', 'Ticker', 'CIK', 'Date']]

        # Assets
        df_WFC_final['CashandCashEquivalents'] = df_WFC['CashAndDueFromBanks'] + df_WFC['InterestBearingDepositsInBanks'] + df_WFC['CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents']
        df_WFC_final['TradingAssets'] = df_WFC['TradingSecuritiesDebt'] + df_WFC['AvailableForSaleSecuritiesDebtSecurities']
        df_WFC_final['FedFundsSecuritiesPurchased'] = df_WFC['FederalFundsSoldSecuritiesPurchasedUnderResaleAgreementsOtherShortTermInvestments']
        df_WFC_final['DebtMaturity'] = df_WFC['HeldToMaturitySecurities'] + df_WFC['DebtSecuritiesHeldToMaturityAmortizedCostAfterAllowanceForCreditLoss']
        df_WFC_final['DebtForSale'] = df_WFC['LoansReceivableHeldForSaleNetNotPartOfDisposalGroup'] + df_WFC['LoansAndLeasesReceivableNetReportedAmount']
        df_WFC_final['LoanLossAllowance'] = df_WFC['LoansAndLeasesReceivableAllowance']
        df_WFC_final['Goodwill'] = df_WFC['Goodwill']
        df_WFC_final['TangibleAssets'] = df_WFC['PropertyPlantAndEquipmentNet'] + df_WFC['PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization']
        df_WFC_final['OtherAssets'] = df_WFC['ServicingAssetAtFairValueAmount'] + df_WFC['ServicingAssetAtAmortizedValue'] + df_WFC['EquitySecuritiesFvNiAndWithoutReadilyDeterminableFairValue'] + df_WFC['OtherAssets'] + df_WFC['ServicingAsset']
        df_WFC_final['TotalAssets'] = df_WFC['Assets']
        
        #Liabilities
        df_WFC_final['Deposits'] = df_WFC['Deposits']
        df_WFC_final['ShortTermBorrowings'] = df_WFC['ShortTermBorrowings']
        df_WFC_final['LongTermBorrowings'] = df_WFC['LongTermDebt']
        df_WFC_final['TradingLiabilities'] = df_WFC['DerivativeLiabilities']
        df_WFC_final['TotalLiabilities'] = df_WFC['Liabilities']
        
        #Equity
        df_WFC_final['PreferredStock'] = df_WFC['PreferredStockValue']
        df_WFC_final['CommonStock'] = df_WFC['CommonStockValue']
        df_WFC_final['AdditionalPaidInCapital'] = df_WFC['AdditionalPaidInCapital']
        df_WFC_final['RetainedEarnings'] = df_WFC['RetainedEarningsAccumulatedDeficit']
        df_WFC_final['AccCompIncomeLoss'] = df_WFC['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_WFC_final['TreasuryStock'] = df_WFC['TreasuryStockValue']
        df_WFC_final['TotalEquity'] = df_WFC['StockholdersEquity']
        df_WFC_final['TotalLiabilitiesAndStockholdersEquity'] = df_WFC['LiabilitiesAndStockholdersEquity']
        
        continue
    
     #next lender
    if ticker == 'C':
        df_C_final = df_final.copy()
        
        df_C_final[['LenderID','Ticker','CIK','DateId']] = df_C[['Lender', 'Ticker', 'CIK', 'Date']]

        # Assets
        df_C_final['CashandCashEquivalents'] = df_C['CashAndDueFromBanks'] + df_C['InterestBearingDepositsInBanks']
        df_C_final['TradingAssets'] = df_C['BrokerageReceivables'] + df_C['TradingSecurities']
        df_C_final['SecuritiesBorrowed'] = df_C['CarryingValueOfFederalFundsSoldSecuritiesPurchasedUnderAgreementsToResellAndDepositsPaidForSecuritiesBorrowed']
        df_C_final['FedFundsSecuritiesPurchased'] = df_C['CarryingValueOfFederalFundsSoldSecuritiesPurchasedUnderAgreementsToResellAndDepositsPaidForSecuritiesBorrowed']
        df_C_final['DebtMaturity'] = df_C['DebtSecuritiesHeldToMaturityNetOfAllowanceForCreditLosses']
        df_C_final['DebtForSale'] = df_C['AvailableForSaleSecuritiesDebtSecurities']
        df_C_final['LoanLossAllowance'] = df_C['FinancingReceivableAllowanceForCreditLosses']
        df_C_final['Goodwill'] = df_C['Goodwill']
        df_C_final['OtherAssets'] = df_C['IntangibleAssetsNetExcludingGoodwill'] + df_C['NotesReceivableGross'] + df_C['OtherAssets']
        df_C_final['TotalAssets'] = df_C['Assets']
        
        #Liabialities
        df_C_final['Deposits'] = df_C['Deposits']
        df_C_final['ShortTermBorrowings'] = df_C['CarryingValueOfFederalFundsPurchasedSecuritiesSoldUnderAgreementsToRepurchaseAndDepositsReceivedForSecuritiesLoaned'] + df_C['ShortTermBorrowings']
        df_C_final['LongTermBorrowings'] = df_C['LongTermDebt']
        df_C_final['TradingLiabilities'] = df_C['TradingLiabilities']
        df_C_final['TotalLiabilities'] = df_C['Liabilities']
        
        #Equity
        df_C_final['PreferredStock'] = df_C['PreferredStockValue']
        df_C_final['CommonStock'] = df_C['CommonStockValue']
        df_C_final['AdditionalPaidInCapital'] = df_C['AdditionalPaidInCapital']
        df_C_final['RetainedEarnings'] = df_C['RetainedEarningsAccumulatedDeficit']
        df_C_final['AccCompIncomeLoss'] = df_C['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_C_final['TreasuryStock'] = df_C['TreasuryStockValue']
        df_C_final['TotalEquity'] = df_C['StockholdersEquity']
        df_C_final['TotalLiabilitiesAndStockholdersEquity'] = df_C['LiabilitiesAndStockholdersEquity']
        
        continue
    
        
     #next lender
    if ticker == 'USB':
        df_USB_final = df_final.copy()
        
        df_USB_final[['LenderID','Ticker','CIK','DateId']] = df_USB[['Lender', 'Ticker', 'CIK', 'Date']]       
        
        
        # Assets
        df_USB_final['CashandCashEquivalents'] = df_USB['CashAndDueFromBanks']
        #df_USB_final['TradingAssets'] = df_USB['TradingSecuritiesDebt']
        #df_USB_final['FedFundsSecuritiesPurchased'] = df_USB[['FederalFundsSoldSecuritiesPurchasedUnderResaleAgreementsOtherShortTermInvestments']].sum()
        #df_USB_final['SecuritiesBorrowed'] = df_USB['SecuritiesBorrowed']
        ##df_USB_final['DebtMaturity'] = df_USB['DebtSecuritiesHeldtomaturityNetofAllowanceForCreditLosses']
        df_USB_final['DebtForSale'] = df_USB['LoansReceivableHeldForSaleNetNotPartOfDisposalGroup'] + df_USB['LoansAndLeasesReceivableNetReportedAmount'] + df_USB['AvailableForSaleSecuritiesDebtSecurities']
        df_USB_final['LoanLossAllowance'] = df_USB['LoansAndLeasesReceivableAllowance']
        df_USB_final['Goodwill'] = df_USB['Goodwill']
        df_USB_final['TangibleAssets'] = df_USB['PropertyPlantAndEquipmentNet']
        df_USB_final['OtherAssets'] = df_USB['NotesReceivableGross'] + df_USB['OtherAssets']
        df_USB_final['TotalAssets'] = df_USB['Assets']
        
        #Liabilities
        df_USB_final['Deposits'] = df_USB['Deposits']
        df_USB_final['ShortTermBorrowings'] = df_USB['ShortTermBorrowings']
        df_USB_final['LongTermBorrowings'] = df_USB['LongTermDebt']
        #df_USB_final['TradingLiabilities'] = df_USB['DerivativeLiabilities']
        df_USB_final['TotalLiabilities'] = df_USB['Liabilities']
        
        #Equity
        df_USB_final['PreferredStock'] = df_USB['PreferredStockIncludingAdditionalPaidInCapitalNetOfDiscount']
        df_USB_final['CommonStock'] = df_USB['CommonStockValue']
        df_USB_final['AdditionalPaidInCapital'] = df_USB['AdditionalPaidInCapitalCommonStock']
        df_USB_final['RetainedEarnings'] = df_USB['RetainedEarningsAccumulatedDeficit']
        df_USB_final['AccCompIncomeLoss'] = df_USB['AccumulatedOtherComprehensiveIncomeLossNetOfTax']
        df_USB_final['TreasuryStock'] = df_USB['TreasuryStockCommonValue']
        df_USB_final['TotalEquity'] = df_USB['StockholdersEquity']
        df_USB_final['TotalLiabilitiesAndStockholdersEquity'] = df_USB['LiabilitiesAndStockholdersEquity']

        continue

######Concatenate all the final dataframes


frames = [df_JPM_final, df_MS_final, df_GS_final, df_BAC_final, df_WFC_final, df_C_final, df_USB_final]
factBalanceSheet_dbtable = pd.concat(frames)


####Ensure that the values in the final dataframe are actually numbers
factBalanceSheet_dbtable.iloc[:, 5:] = factBalanceSheet_dbtable.iloc[:, 5:].applymap(lambda x: x.item() if isinstance(x, np.ndarray) else x)
######

##########################
#Turn files into .CSV 
##########################


csv_df_JPM = df_JPM.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_GS = df_GS.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_MS = df_MS.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_BAC = df_BAC.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_WFC = df_WFC.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_C = df_C.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_USB = df_USB.to_csv(index=False, header=True)#.encode('utf-8')
csv_df_final = factBalanceSheet_dbtable.to_csv(index=False, header=True)#.encode('utf-8')



names = ['df_JPM', 'df_MS', 'df_GS', 'df_BAC', 'df_WFC', 'df_C', 'df_USB', 'factBalanceSheet_dbtable']  


dataframes = [csv_df_JPM,
csv_df_GS,
csv_df_MS, 
csv_df_BAC,
csv_df_WFC,
csv_df_C,
csv_df_USB,
csv_df_final]



##########################
##UPLOAD TO AZURE BLOB####
##########################

from azure.storage.blob import BlobServiceClient

# The storage account name and access key Berzaf gave
storage_account_name = "pppdatastorage"
storage_account_key = "yourkey"
azure_sas_token1 ='yourtoken'
azure_storage_connection_string = 'connectionstring'
azure_storage_container = 'pppdata'


########
########




########
########


# Connecting to the storage account
conn_str = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"


blob_service_client = BlobServiceClient.from_connection_string(conn_str)



# Connect to Azure Storage
blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
container_client = blob_service_client.get_container_client(azure_storage_container)

# Enable string logging
options = {
    'Azure-Storage-Log-String-To-Sign': 'true'
}

# List the containers
containers = blob_service_client.list_containers(logging_enable=options)

# Iterate through the containers
for container in containers:
    # Retrieve and print the string to sign for each container
    string_to_sign = container.response.headers['x-ms-string-to-sign']
    print("String to sign:", string_to_sign)



# The container name
container_name = "pppdata"

# Create a reference to the storage container
container_space = blob_service_client.get_container_client(container_name)


# Uploading the DataFrame as a blob to the container
for data, name in zip(dataframes, names):
    # Uploading the file to the container
    container_space.upload_blob(data=data, name=name)
    
    
#
## Upload the updated content as a blob, overwriting the existing blob
#container_client.upload_blob(name=blob_name, data=updated_data, overwrite=True)
#container_space.upload_blob(name='factBalanceSheet_dbtable', data=csv_df_final, overwrite=True)
    
    
##################################
##UPLOAD TABLE TO SNOWFLAKE DB####
##################################
    
    
#Snowflake connection details
snowflake_account = 'acct'
snowflake_user = ''
snowflake_password = ''
snowflake_database = 'PPP_LOAN_DB'
snowflake_schema = 'PPP_STAGING_DATA'
snowflake_warehouse= 'TEAM24DE_WH'




##Establish the connection

conn = snowflake.connector.connect(user=snowflake_user,
                                   password=snowflake_password,
                                   account=snowflake_account,
                                   warehouse = snowflake_warehouse,
                                   database=snowflake_database,
                                   schema = snowflake_schema,
                                   azure_sas_token=azure_sas_token1)

# Create cursor

cursor = conn.cursor()

#Create Tables

create_balancesheet_table_query = '''
CREATE TABLE IF NOT EXISTS factbalancesheettable (
    LenderID TEXT,
    Ticker VARCHAR(3),
    CIK INTEGER,
    DateId DATE,
    Section TEXT,
    CashandCashEquivalents NUMERIC,
    TradingAssets NUMERIC,
    FedFundsSecuritiesPurchased NUMERIC,
    SecuritiesBorrowed NUMERIC,
    DebtMaturity NUMERIC,
    DebtForSale NUMERIC,
    LoanLossAllowance NUMERIC,
    Goodwill NUMERIC,
    TangibleAssets NUMERIC,
    OtherAssets NUMERIC,
    TotalAssets NUMERIC,
    Deposits NUMERIC,
    ShortTermBorrowings NUMERIC,
    LongTermBorrowings NUMERIC,
    TradingLiabilities NUMERIC,
    OtherLiabilities NUMERIC,
    TotalLiabilities NUMERIC,
    PreferredStock NUMERIC,
    CommonStock NUMERIC,
    AdditionalPaidInCapital NUMERIC,
    RetainedEarnings NUMERIC,
    AccCompIncomeLoss NUMERIC,
    TreasuryStock NUMERIC,
    EmployeeStockComp NUMERIC,
    TotalEquity NUMERIC,
    TotalLiabilitiesAndStockholdersEquity NUMERIC
);
'''

#Execute the SQL statement to create the table
cursor.execute("USE SCHEMA PPP_STAGING_DATA")

cursor.execute(create_balancesheet_table_query)

#delete a table in snowflake
#cursor.execute('''DROP TABLE IF EXISTS factbalancesheet;''')


##Create Azure Stage
url = f'azure://{storage_account_name}.blob.core.windows.net/{container_name}/'

# Define the SQL statement
create_azureblob_stage_query = '''
CREATE STAGE myazurestagefinal4
  URL = 'azure://pppdatastorage.blob.core.windows.net/pppdata'
  CREDENTIALS = (
    AZURE_SAS_TOKEN = 'sastoken'
  );
'''
  #FILE_FORMAT = my_csv_format

####error with my csv format now

try:
    # Execute the SQL statement
    cursor.execute(create_azureblob_stage_query)

    # Commit the changes (if necessary)
    conn.commit()

    print("Stage created successfully!")
except snowflake.connector.Error as e:
    # Handle any errors that occurred during execution
    print("Error:", e)


#RESUME
alter_warehouse = '''
ALTER WAREHOUSE TEAM24DE_WH RESUME;
'''

cursor.execute(alter_warehouse)

##Load into Snoflake database from Azure blob

copyrecords_azure_snowflake = '''
COPY INTO factbalancesheettable
FROM @myazurestagefinal4
FILES = ('factBalanceSheet_dbtable')
ON_ERROR = 'CONTINUE';
'''


#  PATTERN='.*factBalanceSheet*.csv'
  
  
cursor.execute(copyrecords_azure_snowflake)

# Commit the changes (if necessary)
conn.commit()



#######################



###LOADING THE DATA INTO SNOWFLAKE
# Azure Storage container details
azure_storage_connection_string = 'DefaultEndpointsProtocol=https;AccountName=pppdatastorage;AccountKey=b4UhnaSHwcv1Tm7MNL0lYCaC6BLMh2T1A5EjU4fgHxltuPg2jCeIPZXgjcIDohKHjtKQe9Qegbso+ASto63eZA==;EndpointSuffix=core.windows.net'
azure_storage_container = 'pppdata'


# Snowflake connection details
snowflake_account = ''
snowflake_user = ''
snowflake_password = ''
snowflake_database = 'PPP_LOAN_DB'
snowflake_schema = 'PPP_STAGING_DATA'
snowflake_warehouse= 'The_PPP_Loan_Pro_WH'
snowflake_table='factbalancesheettable'


# Connect to Azure Storage
blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
container_client = blob_service_client.get_container_client(azure_storage_container)


# Download the CSV file from Azure Storage
csv_file_name = 'factBalanceSheet_dbtable'

blob_client = container_client.get_blob_client(csv_file_name)
downloaded_blob = blob_client.download_blob()
csv_data = downloaded_blob.content_as_text()

# Convert CSV data to a list of dictionaries
csv_data_lines = csv_data.split("\n")
csv_reader = csv.DictReader(csv_data_lines)
data = [dict(row) for row in csv_reader]
#print(data)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse
)
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse)


# Create a cursor to execute SQL statements
cursor = conn.cursor()

# Set the current schema
set_schema_sql = f"USE SCHEMA {snowflake_schema}"
cursor.execute(set_schema_sql)

# Create a warehouse in Snowflake
# create_warehouse_sql = f'''
#     CREATE WAREHOUSE IF NOT EXISTS {snowflake_warehouse}
#     WITH WAREHOUSE_SIZE = 'X-Small'
#     AUTO_SUSPEND = 600
#     AUTO_RESUME = TRUE
#     COMMENT = 'The PPP Loan WH'
# '''
# cursor.execute(create_warehouse_sql)


# Insert CSV data from Azure into Snowflake table
insert_sql = f"INSERT INTO {snowflake_table} VALUES (%s,%s, %s, %s,%s,%s, %s, %s,%s, %s, %s,%s, %s,%s,%s,%s,%s,%s,%s, %s, %s,%s, %s, %s,%s, %s,%s,%s,%s,%s,%s)"
for row in data:
    cursor.execute(insert_sql, (
        row['LenderID'],
        row['Ticker'],
        row['CIK'],
        row['DateId'],
        row['Section'] if row['Section'] != '' else None,
        row['CashandCashEquivalents'],
        row['TradingAssets'],
        row['FedFundsSecuritiesPurchased'],
        row['SecuritiesBorrowed'],
        row['DebtMaturity'],
        row['DebtForSale'],
        row['LoanLossAllowance'],
        row['Goodwill'],
        row['TangibleAssets'],
        row['OtherAssets'] if row['OtherAssets'] != '' else None,
        row['TotalAssets'],
        row['Deposits'],
        row['ShortTermBorrowings'],
        row['LongTermBorrowings'],
        row['TradingLiabilities'],
        row['OtherLiabilities'] if row['OtherLiabilities'] != '' else None,
        row['TotalLiabilities'],
        row['PreferredStock'],
        row['CommonStock'],
        row['AdditionalPaidInCapital'],
        row['RetainedEarnings'],
        row['AccCompIncomeLoss'],
        row['TreasuryStock'],
        row['EmployeeStockComp'],
        row['TotalEquity'],
        row['TotalLiabilitiesAndStockholdersEquity']
    ))

    

# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()


  
####example from class

#INSERT INTO IF FOR SMALLER CHUNKS OF DATA, COPY INTO NORMALLY USED WHEN THINGS ARE STORED
# file_stock = open('data/stocks/stocks_data.csv')
# contents = csv.reader(file_stock)

# insert_records = "INSERT INTO stocks (date, open_price, highest_price, lowest_price, close_price, volume, symbol) VALUES(?, ?, ?, ?, ?, ?, ?)"
 
# # Importing the contents of the file
# # into our person table
# cursor.executemany(insert_records, contents)
  
  
###############
#DELETE A STAGE AZURE/SNOWFLAKE -below
###############

# drop_stage_query = "DROP STAGE IF EXISTS my_bsazure_stage;"

# try:
#     # Execute the SQL statement to drop the stage
#     cursor.execute(drop_stage_query)

#     # Commit the changes (if necessary)
#     conn.commit()

#     print("Stage deleted successfully!")
# except snowflake.connector.Error as e:
#     # Handle any errors that occurred during execution
#     print("Error:", e)

###############
#DELETE A STAGE AZURE/SNOWFLAKE - above
###############

###Test if it worked, if the data was uploaded
cursor.execute("SELECT * FROM factBalanceSheet")

# Example: Fetch all rows returned by the query
rows = cursor.fetchall()
#rows = cursor.execute(select_all).fetchall()



# Example: Loop through the rows and print the data
for row in rows:
    print(row)
    
    

#Commit the changes to the databas

conn.commit()

#Close the database connection
cursor.close()
conn.close()
# Execute SQL statement





    


     


# #333#######3333
# ##############3


# df_bs_final = pd.DataFrame()



# df_JPM.loc['Value'] = pd.to_numeric(df_JPM.loc['Value'])


#     df.loc['Value'] = pd.to_numeric(df.loc['Value'], errors='coerce')
#     df.dropna(subset=['Value'], inplace=True)

#     df.loc['Value'] = pd.to_numeric(df.loc['Value'])



#########
#TRANSFORMATIONS FOR WHEN OTHER WORKED
########
        
#                 for num in range(1,3):
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
        




# #dictionary to dataframe, every key (company) should be a row so use orient = index

# companyData = pd.DataFrame.from_dict(companyTickers.json(), orient='index')
# companyData.describe()


# ######GS ##works well

# gs_url =    "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520282987/d25996d10q.htm" #q3
# gs_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520129324/d907802d10q.htm"
# gs_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312520212201/d920934d10q.htm"
# gs_url_10k = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000886982/000119312521049380/d39654d10k.htm" 



# ###gs with xbrl

# ####BAC
# bac_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000023/bac-0331202010xq.htm"
# bac_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000040/bac-0630202010xq.htm"
# bac_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000070858/000007085820000071/bac-20200930.htm"


# ####WELLS FARGO
# wf_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000338/wfc-20200930.htm"

# wf_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000338/wfc-20200930.htm"
# wf_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000288/wfc-0630x2020x10q.htm"
# wf_url_q1 ="https://www.sec.gov/ix?doc=/Archives/edgar/data/0000072971/000007297120000236/wfc-0331x2020x10q.htm"


# ###Citigroup

# citi_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000110/c-20200930.htm"
# citi_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000078/c-20200630.htm"
# citi_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000831001/000083100120000044/c-3312020x10q.htm"



# ##US Bancorp

# usb_url_q3 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520286983/d947218d10q.htm"
# usb_url_q2 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520211979/d890129d10q.htm"
# usb_url_q1 = "https://www.sec.gov/ix?doc=/Archives/edgar/data/0000036104/000119312520136359/d897119d10q.htm"









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

