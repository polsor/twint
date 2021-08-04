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

def print_but_better(i):
	b = f'{i:.<60}'
	sys.stdout.write('\r'+b)

def print_but_much_better(i):
	b = f'{i:.<60}'
	sys.stdout.write('\r'+b)

def scrape_data(search, since, until, file_name):

    print(f"scraping from {since.strftime('%Y-%m-%d')} to {until.strftime('%Y-%m-%d')}")

    # Configure
    c = twint.Config()
    c.Search = search
    c.Since = str(since)
    c.Until = str(until)
    c.Store_csv = True
    c.Output = file_name
    c.Min_likes = 50
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

        time.sleep(1)

        all_files = list(os.listdir(file_name.split('/')[0]))

        if str(file_name.split('/')[1]) in all_files:
            dates_in_file = dates_count(file_name)
            dates_total = monthrange(start_date.year, start_date.month)[1]

            print(f"obtained data for {dates_in_file:d}/{dates_total:d} days".format(width=50), end='\n')
        else:
            print("NO DATA RETRIEVED".format(width=50),end='\n')
		
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

# run once when you start the program

jobone()
jobtwo()

# run every minute(s), hour, day at, day of the week, day of the week and time. Use "#" to block out which ones you don't want to use.  Remove it to active. Also, replace "jobone" and "jobtwo" with your new function names (if applicable)

# schedule.every(1).minutes.do(jobone)
schedule.every().hour.do(jobone)
# schedule.every().day.at("10:30").do(jobone)
# schedule.every().monday.do(jobone)
# schedule.every().wednesday.at("13:15").do(jobone)

# schedule.every(1).minutes.do(jobtwo)
schedule.every().hour.do(jobtwo)
# schedule.every().day.at("10:30").do(jobtwo)
# schedule.every().monday.do(jobtwo)
# schedule.every().wednesday.at("13:15").do(jobtwo)

while True:
  schedule.run_pending()
  time.sleep(1)
