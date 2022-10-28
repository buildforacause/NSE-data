from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import date
from datetime import timedelta

# HEADERS for websites
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/97.0.4692.71 Safari/537.36",
    "accept-language": "en-US,en;q=0.9"
}

# FETCH EQUITY
equity_segment_url = "https://www.nseindia.com/market-data/securities-available-for-trading"
equity_segment = requests.get(url=equity_segment_url, headers=headers)
bsObj_equity = BeautifulSoup(equity_segment.text, "lxml")
equity_file_url = bsObj_equity.find_all("div", class_="tab-pane active")[0].find_next("a", class_="file")['href']

# TRIED to access bhavcopy with webscraping but NSE has onlick hrefs instead of html hrefs so was unable to do
# TRIED selenium as well but still did not get the result
# bhavcopy_url = "https://www.nseindia.com/all-reports"
# bhavcopy_request = requests.get(url=bhavcopy_url, headers=headers)
# bsObj_bhavcopy = BeautifulSoup(bhavcopy_request.text, "lxml")
# bhavcopy_file_a_tags = bsObj_bhavcopy.find_all("a", class_="pdf-download-link")
# bhavcopy_main_tag = [i['href'] for i in bhavcopy_file_a_tags if i['href'][-12:] == "bhav.csv.zip"]
# print(bhavcopy_file_a_tags)

# Was able to get bhavcopy data by manipulating URL
today = date.today()
yesterday = today - timedelta(days=1)
year, month, day = yesterday.strftime('%Y'), yesterday.strftime('%b').upper(), yesterday.strftime('%d')
# following URL will get yesterdays bhavcopy data dynamically
bhavcopy_file_url = f"https://archives.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{day + month + year}bhav.csv.zip"

# make DF of both csv data
equity_segment_data = pd.read_csv(equity_file_url)
# bhavcopy was a zip csv file
bhavcopy_data = pd.read_csv(bhavcopy_file_url, compression="zip")

# create db file
engine = create_engine('sqlite:///mydb.sqlite')

# insert csv to db ( should be done only once this is why code is commented)
# equity_segment_data.to_sql('equity_segment_data', engine)
# bhavcopy_data.to_sql('bhavcopy_data', engine)

# QUERY 1
res1 = pd.read_sql_query('SELECT SYMBOL, ((CLOSE - OPEN)/ OPEN) AS GAINS FROM bhavcopy_data ORDER BY GAINS DESC LIMIT 25', engine)

# DISPLAY result
print(res1)
