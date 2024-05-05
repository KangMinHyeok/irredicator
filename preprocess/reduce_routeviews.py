import os
import sys
import bz2
import json
import argparse
import pandas

import time as t
from tqdm import tqdm
from datetime import *
from datetime import timedelta
from mrtparse import *
from multiprocessing import Pool


sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from bgp.bgp_dump import BgpDump

def readBGPUpdate(arg):
   
    vpName, infile = arg
    b = BgpDump(vpName=vpName)

    print("start read BGP update: {}".format(infile))

    lines = set()
    count = 0

    num_lines = 0
    entries = None
    try:
        entries = Reader(infile)
    except:
        return lines

    # for m in tqdm(entries):
    for m in entries:
        if m.err:
            continue
        if m.data['type'][0] == MRT_T['BGP4MP']:
            for line in b.bgp4mp(m.data, count):
                lines.add(line)
        elif m.data['type'][0] == MRT_T['BGP4MP_ET']:
            for line in b.bgp4mp(m.data, count):
                lines.add(line)

        else:
            continue
    print("{} done".format(infile))
    return lines

def get_target_interval(bgp_path, local_path):

    files = os.listdir(local_path + '/bgpdata')
    start = max(list(map(lambda x: x.split('.')[0], files)))

    bgp_path = bgp_path + '/bgpdata/'
    subdirs = sorted(os.listdir(bgp_path), reverse=True)
    for subdir in subdirs:
        files = sorted(os.listdir(bgp_path + subdir + '/UPDATES'), reverse=True)
        if len(files) == 0:
            continue

        files = list(map(lambda x: x.split('.'), files))
        files = list(filter(lambda x: x[2] == '2300', files))
        end = max(list(map(lambda x: x[1], files)))
        break

    return start, end


def parseBGPUpdate(bgp_path, local_path):
    os.makedirs(local_path, exist_ok=True)

    start, end = get_target_interval(bgp_path, local_path)
    print(start, end)
    
    sources = os.listdir(bgp_path)
    
    targetdates = pandas.date_range(start, end-timedelta(days=1),freq='d')
    targetdates = list(map(lambda x: x.strftime('%Y%m%d'), targetdates))

    print(targetdates)

    with Pool(12) as p:
        
        for date in targetdates:

            for source in sources:
                vpName = source

                curr_local_path = "{}/{}".format(local_path, source)
                os.makedirs(curr_local_path, exist_ok=True)

                if source == "bgpdata":
                    source_path = "{}/{}".format(bgp_path, source)
                else:
                    source_path = "{}/{}/bgpdata".format(bgp_path, source)

                year = date[:4]
                month = date[4:6]


                arg_list = []
                file_path = "{}/{}.{}/UPDATES/updates.{}".format(source_path, year, month, date)
                for i in range(0, 24, 2):
                    filename = file_path + ".{0:02}00.bz2".format(i)
                    arg_list.append((vpName, filename))

                result = set()

                ret = p.map_async(readBGPUpdate, arg_list)

                results = ret.get()
                for r in results:
                    result = result.union(r)
                    
                local_file = "{}/{}".format(curr_local_path, date)
                with open(local_file + '.txt', 'w') as fout:
                    for line in result:
                        fout.write(line + '\n')


def main():
    parser = argparse.ArgumentParser(description='parse BGP update\n')
    parser.add_argument('--bgp_path', default='/net/data/routeviews/raw-datasets')
    
    parser.add_argument('--local_path', default='/net/data/routeviews/update')
    
    parser.parse_args()
    args = parser.parse_args()
    print(args)

    parseBGPUpdate(args.bgp_path, args.local_path)

if __name__ == '__main__':
    main()
    
