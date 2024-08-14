import os
import pandas
import argparse
from datetime import *



def get_roa(start_date, end_date, roa_dir, vrp_dir):
    dates = os.listdir(vrp_dir)
    if len(dates) != 0:
        dates = list(map(lambda x: x.split('/')[-1].split('.')[0], dates))
        dates = list(map(lambda x: ''.join(x.split('-')[1:]), dates))
        start_date = sorted(dates)[-1]
    start_date = datetime.strptime(start_date, '%Y%m%d') + timedelta(days=1)
    if end_date is not None:
        end_date = datetime.strptime(end_date, '%Y%m%d')
    else:
        end_date = datetime.today()

    targetdates = pandas.date_range(start_date,end_date-timedelta(days=1),freq='d')
    print(targetdates)

    rirs = ['apnic', 'afrinic', 'arin', 'lacnic', 'ripencc']


    ftp = 'https://ftp.ripe.net/rpki'
    vrp_fmt = '{vrp_dir}/vrps-{date}.csv'
    wget_fmt = 'wget {ftp}/{rir}.tal/{date_dir}/roas.csv.xz -O {roa_dir}/{rir}-roas.csv.xz'
    xz_fmt = 'xz -v -d {roa_dir}/{rir}-roas.csv.xz'
    tail_fmt = 'tail -n +2 {roa_dir}/{rir}-roas.csv >> {filename}'
    rm_fmt = 'rm {roa_dir}/{rir}-roas.csv >> {filename}'

    curr_date = start_date
    for curr_date in targetdates:
        date_dir = curr_date.strftime("%Y/%m/%d")
        date = curr_date.strftime("%Y-%m-%d")
        filename = vrp_fmt.format(vrp_dir=vrp_dir, date=date)

        if os.path.exists(filename):
            print('skip {}: file exists'.format(filename))
            continue

        for rir in rirs:
            wget_cmd = wget_fmt.format(ftp=ftp, rir=rir, date_dir=date_dir, roa_dir=roa_dir)
            os.system(wget_cmd)
            print (wget_cmd)
            xz_cmd = xz_fmt.format(roa_dir=roa_dir, rir=rir)
            os.system(xz_cmd)
            print (xz_cmd)

        with open(filename, 'w') as fout:
            fout.write('URI,ASN,IP Prefix,Max Length,Not Before,Not After\n')
        
        for rir in rirs:        
            tail_cmd = tail_fmt.format(roa_dir=roa_dir, rir=rir, filename=filename)
            os.system(tail_cmd)
            print (tail_cmd)
        
        for rir in rirs:
            rm_cmd = rm_fmt.format(roa_dir=roa_dir, rir=rir, filename=filename)
            os.system(rm_cmd)
            print (rm_cmd)
        
        print("Got {}\n".format(filename))

def main():
    parser = argparse.ArgumentParser(description='get vrp\n')
    parser.add_argument('--start_date', type=str, default='20230331')
    parser.add_argument('--end_date', type=str, default=None)
    parser.add_argument('--roa_dir', type=str, default='/net/data/vrps/ziggy-roas/')
    parser.add_argument('--vrp_dir', type=str, default='/net/data/vrps/ziggy-vrps/')

    args = parser.parse_args()

    get_roa(args.start_date, args.end_date, args.roa_dir, args.vrp_dir)

if __name__ == '__main__':
    main()

