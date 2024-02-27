import os
import time
import requests

from datetime import date
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs


def isParentDirectory(text):
    return 'Parent Directory' in text

def getElements(url, target):
    parser = "html.parser"
    page = requests.get(url)
    soup = bs(page.text, parser)
    elements = soup.select(target)  
    return elements

def download(arg):
    root_url, local_dir, source, delay, latest_date = arg
    source_dir = local_dir + source + '/'
    try: os.makedirs(source_dir, exist_ok=True)
    except Exception as e: 
        print(e)
        return
    source_url = root_url + source + '/'
    
    yearmonths = getElements(source_url, 'tr > td > a')
    latest_month = latest_date[:4] + '.' + latest_date[4:6]
    for yearmonth in yearmonths:
        if isParentDirectory(yearmonth.text): continue
        
        if yearmonth.text < latest_month: continue
        
        date_dir = source_dir + yearmonth.attrs['href'] + '/'
        try: os.makedirs(date_dir, exist_ok=True) 
        except Exception as e: 
            print(e)
            continue

        yearmonth_url = source_url + yearmonth.attrs['href'] + '/'

        datatypes = getElements(yearmonth_url, 'tr > td > a')
        
        for datatype in datatypes:
            if isParentDirectory(datatype.text): continue
            if 'UPDATE' not in datatype.text: continue
            datatype_dir = date_dir + datatype.attrs['href'] + '/'
            try: os.makedirs(datatype_dir, exist_ok=True)
            except Exception as e: 
                print(e)
                continue
            
            updates_url = yearmonth_url + datatype.attrs['href'] + '/'

            updates = getElements(updates_url, 'tr > td > a')
            updates = list(filter(lambda x: x.attrs['href'].split('.')[1] > latest_date
                and x.attrs['href'].endswith('00.bz2'), updates))
            # print(list(map(lambda x: x.attrs['href'], updates)))
            for update in updates:
                if isParentDirectory(update.text): continue

                target_url = updates_url + update.attrs['href']

                target_file = datatype_dir + update.attrs['href']

                # if not target_file.endswith('00.bz2'): continue
                
                if os.path.exists(target_file): continue

                os.system('wget {} -O {}'.format(target_url, target_file))
                time.sleep(delay)

def main():
    root_url = "https://archive.routeviews.org/"
    
    local_dir = '/net/data/routeviews/raw-datasets'

    months = sorted(list(map(lambda x: x.split('.'), os.listdir(local_dir + '/bgpdata/'))))
    months = sorted(os.listdir(local_dir + '/bgpdata/'), reverse=True)
    today = date.today()
    today = today.strftime("%Y%m%d")

    latest_date = None
    if len(months) > 0:
        for month in months:
            print(month)
            dates = os.listdir(local_dir + '/bgpdata/' + month + '/UPDATES/')
            if len(dates) > 0:
                dates = list(filter(lambda x: x.endswith('2300.bz2'), dates ))
                dates = sorted(list(set(map(lambda x: x.split('.')[1], dates))))
                latest_date = dates[-1]
                break
    if latest_date is None:
        today = date.today()
        latest_date = today.strftime("%Y%m%d")

    print(latest_date)

    try: os.makedirs(local_dir, exist_ok=True)
    except Exception as e: 
        print(e)

    sources = getElements(root_url, 'li > a')
    sources = list(filter(lambda x: not isParentDirectory(x.text) and 'UPDATE' in x.text, sources))
    sources = list(map(lambda source: source.attrs['href'], sources))
    delay = len(sources) // 3
    arg_list = list(map(lambda x: (root_url, local_dir, x, delay, latest_date), sources))
    with Pool(len(sources)) as p:
        p.map(download, arg_list)

if __name__ == '__main__':
    main()


