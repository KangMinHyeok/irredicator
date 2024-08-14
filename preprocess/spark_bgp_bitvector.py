import os
import sys
import time as t
import calendar
import json
import random
import shutil
import argparse
import numpy as np
import pydoop.hdfs as hdfs

from datetime import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from utils.utils import write_result, ip2binary, get_date, get_dates, get_files, make_dirs

def date_diff(start, end):
    if start == None or end == None:
        return -1
    try:
        start = datetime.strptime(str(start), "%Y%m%d")
        end = datetime.strptime(str(end), "%Y%m%d")
    except:
        return -2
    return (end - start).days

def make_bit_vector(bit_size):
    fill = 0
    num_records = bit_size >> 5                   # number of 8 bit integers
    if (bit_size & 31):                      # if bitSize != (32 * n) add
        num_records += 1                        #    a record for stragglers

    bitArray = [0] * num_records
    return bitArray

def shift_bit_vector(bitvector, delay, bit_size=32, bitval=0):
    new_vector = make_bit_vector(bit_size)
    for i in range(bit_size):
        if bitval == 1 and i < delay: set_bit(new_vector, i)
        if i + delay > bit_size: break
        if testBit(bitvector, i): set_bit(new_vector, i + delay)
    return new_vector


# testBit() returns a nonzero result, 2**offset, if the bit at 'bit_num' is set to 1.
def test_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = 1 << offset
    return(bitvector[record] & mask)

# setBit() returns an integer with the bit at 'bit_num' set to 1.
def set_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = 1 << offset
    bitvector[record] |= mask
    return(bitvector[record])

# clearBit() returns an integer with the bit at 'bit_num' cleared.
def clear_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = ~(1 << offset)
    bitvector[record] &= mask
    return(bitvector[record])

# toggleBit() returns an integer with the bit at 'bit_num' inverted, 0 -> 1 and 1 -> 0.
def toggle_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = 1 << offset
    bitvector[record] ^= mask
    return(bitvector[record])

def print_bit(bitvector):
    for record in bitvector:
        for offset in range(32):
            mask = 1 << offset
            if record & mask:
                sys.stdout.write('1')
            else:
                sys.stdout.write('0')
    sys.stdout.write('\n')

def bitvector2str(bitvector, size=32):
    bstring = ''
    for record in bitvector:
        for offset in range(32):
            mask = 1 << offset
            if record & mask:
                bstring = '1' + bstring
            else:
                bstring = '0' + bstring

    return bstring[32-size:]

def tobitvector(bstring):
    bitvector = []

    bitvectors = [bstring[i:i + 32] for i in range(0, len(bstring), 32)]
    return list(map(lambda x: int(x, 2), bitvectors))

def parseBGP(line, ip_version='ipv4'):
    date, rir, prefix_addr, prefix_len, origin, isp, cc, cnt = line.split('\t')
    
    results = []
    results.append( ((prefix_addr, prefix_len, origin), date) )
    return results

def toBitVector(row, start_date='', num_days=32):
    key, value = row

    prefix_addr, prefix_len, origin = key
    bgps = value

    originDict = {}
    
    if bgps == None: bgps = []
    
    bitvector = make_bit_vector(32)

    for bgp_date in sorted(list(bgps)):
        diff = date_diff(start_date, bgp_date)
        if diff < 0 or diff >= num_days:
            continue
        set_bit(bitvector, diff)

    results = []
    bitvector = bitvector2str(bitvector, size=num_days)
    result = [start_date, prefix_addr, prefix_len, origin, bitvector]
    result = '\t'.join(list(map(str, result)))
    results.append(result)
    
    return results

def getBGPBitvectors(bgp_dir, bgp_dir2, hdfs_dir, local_dir):
    
    hdfs_dir = hdfs_dir + 'raw/'    
    
    make_dirs(hdfs_dir, local_dir)

    bgp_files = get_files(bgp_dir, extension='.tsv')
    bgp_files2 = get_files(bgp_dir2, extension='.tsv')

    bgp_files += bgp_files2
    
    today = str(datetime.today()).split(' ')[0]
    print(today)

    end_year, end_month, _ = list(map(int, str(today).split('-')))
    start_year, start_month = 2022, 4

    currfiles = sorted(os.listdir(local_dir))
    if len(currfiles) > 0:
        date = currfiles[-1].split('.')[0]
        start_year, start_month = list(map(int, [date[:4], date[4:]]))

        start_month += 1
        if start_month > 12:
            start_month = 1
            start_year += 1
    print("start", start_year, start_month)
    print("end", end_year, end_month)
    # exit()
    # start_year, start_month = 2011, 1
    # end_year, end_month = 2022, 4
    # print('start: {} {}'.format(start_year, start_month))
    # print('end: {} {}'.format(end_year, end_month))

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month >= end_month:
                continue
            print("start {} {}".format(year, month))
            target = '{}{:02}'.format(year, month)
            curr_bgp_files = list(filter(lambda x: get_date(x).startswith(target), bgp_files))

            if len(curr_bgp_files) <= 0: 
                print("{}: no bgp files".format(target))
                continue

            conf = SparkConf().setAppName(
                        "BGP bitvector {}".format(target)
                        ).set(
                            "spark.kryoserializer.buffer.max", "512m"
                        ).set(
                            "spark.kryoserializer.buffer", "1m"
                        )
        
            sc = SparkContext(conf=conf)

            spark = SparkSession(sc)

            sc.setLogLevel("WARN")

            _, num_days = calendar.monthrange(year, month)

            start_date = "{}{:02}{:02}".format(year, month, 1)

            bgps = sc.textFile(','.join(curr_bgp_files))\
                        .flatMap(parseBGP)
        

            bitvectorRecords = bgps.groupByKey()\
                            .flatMap(lambda row: toBitVector(row, start_date=start_date, num_days=num_days))

            write_result(bitvectorRecords, hdfs_dir + target, local_dir + target, extension='.tsv')

            sc.stop()


def main():
    parser = argparse.ArgumentParser(description='extract BGP features\n')

    parser.add_argument('--bgp_dir', default='/user/mhkang/routeviews/reduced/')
    parser.add_argument('--bgp_dir2', default='/user/mhkang/bgp/routeview-reduced/')

    parser.add_argument('--hdfs_dir', default='/user/mhkang/routeviews/bitvector/')
    parser.add_argument('--local_dir', default='/net/data/routeviews/bitvector/')


    parser.parse_args()
    args = parser.parse_args()
    print(args)

    # '10000....0'
    # first day of month ~ last day of month
    getBGPBitvectors(args.bgp_dir, args.bgp_dir2, args.hdfs_dir, args.local_dir)

if __name__ == '__main__':

    main()
