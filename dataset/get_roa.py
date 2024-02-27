import os
import datetime


def main():
    start_date = datetime.datetime(2011, 1, 21)
    end_date = datetime.datetime(2011, 1, 21)

    roa_dir = '/home/mhkang/tmp/roas'
    vrp_dir = '/home/mhkang/tmp/vrps'
    rirs = ['apnic', 'afrinic', 'arin', 'lacnic', 'ripencc']


    ftp = 'https://ftp.ripe.net/rpki'
    vrp_fmt = '{vrp_dir}/vrps-{date}.csv'
    wget_fmt = 'wget {ftp}/{rir}.tal/{date_dir}/roas.csv -O {roa_dir}/{rir}-roas.csv'
    tail_fmt = 'tail -n +2 {roa_dir}/{rir}-roas.csv >> {filename}'
    rm_fmt = 'rm {roa_dir}/{rir}-roas.csv >> {filename}'

    curr_date = start_date
    while(curr_date <= end_date):
        date_dir = curr_date.strftime("%Y/%m/%d")
        date = curr_date.strftime("%Y-%m-%d")
        filename = vrp_fmt.format(vrp_dir=vrp_dir, date=date)

        if os.path.exists(filename):
            print('skip {}: file exists'.format(filename))
            curr_date = curr_date + datetime.timedelta(1)
            continue

        for rir in rirs:
            wget_cmd = wget_fmt.format(ftp=ftp, rir=rir, date_dir=date_dir, roa_dir=roa_dir)
            os.system(wget_cmd)
            print (wget_cmd)

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
        curr_date = curr_date + datetime.timedelta(1)

if __name__ == '__main__':
    main()
