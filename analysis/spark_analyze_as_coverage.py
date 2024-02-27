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

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from utils.utils import write_result, ip2binary, get_date, get_files

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

def AS_coverage(asn_dir, roa_dir, irr_dir, hdfs_dir, local_dir, start, end):
    make_dirs(hdfs_dir, local_dir)
    
    savePath = savePath + 'raw/'

    make_dirs(hdfs_dir, local_dir)

    conf = SparkConf().setAppName(
                "AS Coverage"
                ).set(
                    "spark.kryoserializer.buffer.max", "512m"
                ).set(
                    "spark.kryoserializer.buffer", "1m"
                )
        
    sc = SparkContext(conf=conf)

    sc.setLogLevel("WARN")

    asnFiles = get_files(asn_dir, extension='.csv')
    irrFiles = get_files(irr_dir, extension='.tsv')
    roaFiles = get_files(roa_dir, extension='.tsv')
    
    asnFiles = list(filter(lambda x: start <= get_date(x) <= end, asnFiles))
    irrFiles = list(filter(lambda x: start <= get_date(x) <= end, irrFiles))
    roaFiles = list(filter(lambda x: start <= get_date(x) <= end, roaFiles))
    
    print("target dates: {} ~ {}".format(start, end))

    total_ASes = sc.textFile(','.join(asnFiles))\
                        .flatMap(parseASN)\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1]), 1) )\
                        .reduceByKey(add)
                        
    roa_ASes = sc.textFile(','.join(roaFiles))\
                        .flatMap(lambda line: parseVRP(line, ip_version=ip_version))\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1], row[2]), 1) )\
                        .reduceByKey(add)\
                        .map(lambda row: ((row[0][0], row[0][1]), (row[0][2], row[1])))
    
    irr_ASes = sc.textFile(','.join(irrFiles))\
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

    filename = 'as-coverage-{}-{}'.format(start, end)
    write_result(results, hdfs_dir + filename, local_dir + filename, extension='.csv')

    sc.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='as coverage\n')
    parser.add_argument('--start', default='20110101')
    parser.add_argument('--end', default='20230301')
    parser.add_argument('--ans_dir', default='/user/mhkang/nrostats/asn/')

    parser.add_argument('--roa_dir' default='/user/mhkang/vrps/daily-tsv/')
    parser.add_argument('--irr_dir', nargs='+', default=['/user/mhkang/radb/daily-tsv-w-changed/', '/user/mhkang/irrs/daily-tsv-w-changed/'])

    parser.add_argument('--hdfs_dir', default='/user/mhkang/deployment/as-coverage/')
    parser.add_argument('--local_dir', default='/home/mhkang/deployment/as-coverage/')
    
    args = parser.parse_args()
    print(args)
    
    
    AS_coverage(args.ans_dir, args.roa_dir, args.irr_dir, args.hdfs_dir, args.local_dir, args.start, args.end)
