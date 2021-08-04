import twint
import schedule
import time
import twint
import nest_asyncio
import time
import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from collections import Counter
import sys
from multiprocessing import Process, Queue

nest_asyncio.apply()

def print_but_better(i):
	print(f'{i:.<60}', end='|')

def scrape_data(search, since, until, file_name):

    print(f"scraping from {since.strftime('%Y-%m-%d')} to {until.strftime('%Y-%m-%d')}")

    # Configure
    c = twint.Config()
    c.Search = search
    c.Since = str(since)
    c.Until = str(until)
    c.Store_csv = True
    c.Output = file_name
    c.Min_likes = 10
    c.Hide_output = True
    c.Stats = False

    # Run
    twint.run.Search(c)
    
    print_but_better(f"saved file: {file_name}")

def dates_count(file_name):
    dates = pd.read_csv(file_name).date
    month_count = dict(Counter([i.split('-')[1] for i in list(set(dates))]))
    return month_count[max(month_count, key=month_count.get)]

def scraper(search, start_date, until_date,name):

    i = 1

    file_name = name+str(i)+'.csv'

    while start_date != until_date:

        file_name = name+str(i)+'.csv'
        end_date = start_date + relativedelta(months=1)

        scrape_data(search,start_date,end_date,file_name)

        time.sleep(0.5)

        all_files = list(os.listdir(file_name.split('/')[0]))

        if str(file_name.split('/')[1]) in all_files:
            dates_in_file = dates_count(file_name)
            dates_total = monthrange(start_date.year, start_date.month)[1]

            print(f"obtained data for {dates_in_file:d}/{dates_total:d} days", end='\n')
        else:
            print("NO DATA RETRIEVED", end='\n')
		
        start_date = end_date
        i+=1
    
    print("DONE".center(30,'-'))



until_date = datetime(2021,8,1)
start_date = datetime(2020,1,1)
    



# you can change the name of each "job" after "def" if you'd like.
def jobone():
	print ("Fetching BTC Tweets")
	search = "'Bitcoin' OR 'BTC'"
	folder = 'data_btc'

	# search = "'Ethereum' OR 'ETH'"
	# folder = 'eth_data'

	name = datetime.now().strftime(format='%Y%m%d%s')
	file_name = folder + '/' + name

	scraper(search, start_date, until_date, file_name)

def jobtwo():
	print ("Fetching ETH Tweets")

	search = "'Ethereum' OR 'ETH'"
	folder = 'data_eth'

	name = datetime.now().strftime(format='%Y%m%d%s')
	file_name = folder + '/' + name

	scraper(search, start_date, until_date, file_name)

def start_scraping():    
    while True:
        schedule.run_pending()
        time.sleep(1)
        jobone()
        jobtwo()
        schedule.every(3).hour.do(jobone)
        schedule.every(3).hour.do(jobtwo)

def main():
    p1 = Process(target=start_scraping)
    p2 = Process(target=start_scraping)
    p3 = Process(target=start_scraping)
    p1.start()
    p2.start()
    p3.start()

if __name__=='__main__':
    main()