import os
import sys
import shutil
import argparse

import numpy as np
import json
from ipaddress import IPv4Address
from multiprocessing import Process
import subprocess

import time
from datetime import *
from ipaddress import *
from operator import add
from operator import itemgetter
from pyspark.sql import SQLContext, Row
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
import pydoop.hdfs as hdfs

# cwd = os.getcwd().split('/')
# sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from utils.utils import write_result, ip2binary, get_date, get_files, make_dirs

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.replace('\n', '').split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []    
    elif ip_version == 'ipv6' and '.' in prefix: return []

    if source == date: source = "RADB"
    source = source.upper()

    try: date2 = datetime.strptime(date, "%Y%m%d")
    except: return []

    results = []
    date = int(date)
    try: origin = int(origin)
    except:
        try: origin = int(origin.split('#')[0])
        except: return []
        
    results.append( (date, "total", source, origin) )
    results.append( (date, rir, source, origin) )
    results.append( (date, "total", "ALL-IRR", origin) )
    results.append( (date, rir, "ALL-IRR", origin) )

    return results

def parseVRP(line, ip_version='ipv4'):
    if line.startswith('#'): return []
    tokens = line.split('\t')
    date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir = tokens[:8]

    if ip_version == 'ipv4' and ':' in prefix_addr: return []
    elif ip_version == 'ipv6' and '.' in prefix_addr: return []

    rirs = ['apnic', 'ripencc', 'afrinic', 'lacnic', 'arin']
    if rir == 'ripe': rir = 'ripencc'
    if rir not in rirs: rir = "None"
    results = []
    
    date, origin = int(date), int(origin)
    
    results.append( (date, "total", "VRP", origin) )
    results.append( (date, rir, "VRP", origin) )

    return results

def parseASN(line):
    date, origin, rir, cc, registered, status = line.split(',')
    
    date, origin = int(date), int(origin)

    rirs = ['apnic', 'arin', 'ripencc', 'afrinic', 'lacnic']
    if rir == 'jpnic': rir = 'apnic'
    if rir not in rirs: rir = 'None'

    results = []
    results.append( (date, "total", origin) )
    results.append( (date, rir, origin) )
    return results

def combine_total_cnt(row):
    key, value = row
    date, rir = key
    asValue, totalCnt = value
    source, cnt = asValue

    results = []
    results.append( ( (date, source), (rir, cnt, totalCnt)) )

    return results

def get_result(row):
    key, value = row
    date, source = key

    dic = {}
    for rir, cnt, totalCnt in value:
        dic[rir] = (cnt, totalCnt)

    rirs = ['total', 'ripencc', 'apnic', 'arin', 'afrinic', 'lacnic', 'None']

    results = [date, source]
    for rir in rirs:
        cnt, totalCnt = dic.get(rir, (0,0))
        results.append(cnt)
        results.append(totalCnt)

    return ','.join(list(map(lambda x: str(x), results)))

def AS_coverage(ip_version, asn_dir, roa_dir, irr_dir, hdfs_dir, local_dir):
    make_dirs(hdfs_dir, local_dir)
    
    hdfs_dir = hdfs_dir + 'raw/'

    make_dirs(hdfs_dir, local_dir)

    asn_files = get_files(asn_dir, extension='.csv')
    irr_files = get_files(irr_dir, extension='.tsv')
    roa_files = get_files(roa_dir, extension='.tsv')

    files = list(filter(lambda x: x.startswith('as-coverage'), os.listdir(local_dir)))
    start = max(list(map(lambda x: x.split('.')[0].split('-')[-1], files)))

    nro_dates = list(map(get_date, asn_files))
    irr_dates = list(map(get_date, irr_files))
    roa_dates = list(map(get_date, roa_files))
    end = min([max(nro_dates), max(irr_dates), max(roa_dates)])
    
    if end <= start:
        print("no new data available")
        print("end date of previpus analysis: {}".format(start))
        print("latest dates of nro, irr, and roa: {}, {}, and {}".format(max(nro_dates), max(irr_dates), max(roa_dates)))
        exit()

    print("target dates: {} ~ {}".format(start, end))


    asn_files = list(filter(lambda x: start <= get_date(x) <= end, asn_files))
    irr_files = list(filter(lambda x: start <= get_date(x) <= end, irr_files))
    roa_files = list(filter(lambda x: start <= get_date(x) <= end, roa_files))

    conf = SparkConf().setAppName(
                "AS Coverage"
                ).set(
                    "spark.kryoserializer.buffer.max", "512m"
                ).set(
                    "spark.kryoserializer.buffer", "1m"
                )
        
    sc = SparkContext(conf=conf)

    sc.setLogLevel("WARN")


    total_ASes = sc.textFile(','.join(asn_files))\
                        .flatMap(parseASN)\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1]), 1) )\
                        .reduceByKey(add)
                        
    roa_ASes = sc.textFile(','.join(roa_files))\
                        .flatMap(lambda line: parseVRP(line, ip_version=ip_version))\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1], row[2]), 1) )\
                        .reduceByKey(add)\
                        .map(lambda row: ((row[0][0], row[0][1]), (row[0][2], row[1])))
    
    irr_ASes = sc.textFile(','.join(irr_files))\
                        .flatMap(lambda line: parseIRR(line, ip_version=ip_version))\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1], row[2]), 1) )\
                        .reduceByKey(add)\
                        .map(lambda row: ((row[0][0], row[0][1]), (row[0][2], row[1])))

    covered_ASes = roa_ASes.union(irr_ASes)

    results = covered_ASes.leftOuterJoin(total_ASes)\
                    .flatMap(combine_total_cnt)\
                    .groupByKey()\
                    .map(get_result)

    filename = 'as-coverage-ipv4-{}'.format(end)
    write_result(results, hdfs_dir + filename, local_dir + filename, extension='.csv')

    sc.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='as coverage\n')
    parser.add_argument('--ip_version', default='ipv4')
    parser.add_argument('--ans_dir', default='/user/mhkang/nrostats/asn/')

    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
    parser.add_argument('--irr_dir', nargs='+', default=['/user/mhkang/irrs/daily-tsv/'])
    
    parser.add_argument('--hdfs_dir', default='/user/mhkang/rpki-irr/outputs/analysis/as-coverage/')
    parser.add_argument('--local_dir', default='/home/mhkang/rpki-irr/outputs/analysis/as-coverage/')


    args = parser.parse_args()
    print(args)
    
    
    AS_coverage(args.ip_version, args.ans_dir, args.roa_dir, args.irr_dir, args.hdfs_dir, args.local_dir)
