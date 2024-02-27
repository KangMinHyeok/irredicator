import os
import sys
import time
import shutil
import argparse
import random

import json
from multiprocessing import Process
import subprocess

from multiprocessing import Pool
from datetime import *
from ipaddress import *
from operator import add
from operator import itemgetter
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
from multiprocessing import Process
import pydoop.hdfs as hdfs

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from utils.utils import write_result, ip2binary, get_date, get_files, add2dict
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

def origin2int(origin):
    int_origin = -1
    if '.' in origin:
        upper, lower = origin.split('.')
        int_origin = int((int(upper) << 16) + int(lower))
    else:
        int_origin = int(origin)
    return int_origin
    

def parseBGP(line, ip_version='ipv4'):
    try:
        date, rir, prefix_addr, prefix_len, origins, ISPs, countries, totalCnt = line.split('\t')
    except:
        return []

    if ip_version == 'ipv4' and ':' in prefix_addr: return []
    elif ip_version == 'ipv6' and '.' in prefix_addr: return []

    totalCnt = int(totalCnt)
    origins = origins.split('|')

    prefix_len = int(prefix_len)
    
    
    records = []

    try:
        date = int(date)
        totalCnt = int(totalCnt)
    except:
        return []

    origins = list(filter(lambda x: x != -1, map(origin2intInt, origins)))
    if len(origins) <= 0: return []

    records.append( ((date, prefix_addr, prefix_len), (origins, totalCnt, rir)) )

    return records

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    try:
        if source == date: source = "RADB"
        source = source.upper()

        date2 = datetime.strptime(date, "%Y%m%d")
        date = int(date)
        changed = int(changed)
        if '#' in origin:
            origin = origin.split('#')[0]
        else:
            origin = origin.replace('AS', '')
        
        origin = origin2int(origin)
        
        prefix_addr, prefix_len = prefix.split('/')
        prefix_len = int(prefix_len)

    except: return []

    results = []
    results.append( (date, (prefix_addr, prefix_len, origin, source, changed)) )
    
    return results

def parseVRP(line, ip_version='ipv4'):
    if line.startswith('#'): return []
    tokens = line.split('\t')
    date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir  = tokens[:8]

    if ip_version == 'ipv4' and ':' in prefix_addr: return []
    elif ip_version == 'ipv6' and '.' in prefix_addr: return []

    try: 
        date2 = datetime.strptime(date, "%Y%m%d")
        date = int(date)
        origin = origin2int(origin)
        prefix_len = int(prefix_len)
        max_len = int(max_len) if max_len != None and max_len != "None" else prefix_len

    except Exception as e:  
        return []
    
    return [ (date, (prefix_addr, prefix_len, max_len, origin, 'VRP')) ]
    
def toCSV(row):
    key, value = row
    date, rir, source = key
    
    data = [date, rir, source] + list(value)
    return [",".join(list(map(str, data)))]

def get_entries(date, binary_prefix, vrp_dict, irr_dict):
    if binary_prefix == None: return [], [], []
    bgp_length = len(binary_prefix)


    tree, record_set = vrp_dict.get(date, ({}, {}))
    vrp_records = get_records(tree, record_set, binary_prefix)

    vrp_origins = set()

    for prefix_addr, prefix_len, max_len, origin, source in vrp_records:
        if int(prefix_len) <= int(bgp_length)
            vrp_origins.add( (origin, source, max_len) )
    
    irr_origins = set()
    tree, record_set = irr_dict.get(date, ({}, {}))
    irr_records = get_records(tree, record_set, binary_prefix)

    for prefix_addr, prefix_len, origin, source, changed in irr_records:
        if int(prefix_len) <= int(bgp_length)
            irr_origins.add( (origin, source, max_len) )

    return list(vrp_origins), list(irr_origins)

def getCounts(bgpOrigin, origins, vrpCover, vrpValid):

    cover = len(origins) > 0
    numCover = 1 if cover else 0

    valid = bgpOrigin != -1 and bgpOrigin in origins
    numValid = 1 if valid else 0

    return [numCover, numValid, numBothCover, numBothValid]

def getResults(row, roaDict, irrDict, bTargetDates, filterTooSpecific=True, ip_version='ipv4'):
    key, value  = row
    date, prefix_addr, prefix_len = key 
    BGPs = value
    
    if date not in bTargetDates.value: return []
    if filterTooSpecific and prefix_len > 24: return []
    binary_prefix = ip2binary(prefix_addr, prefix_len)
    if binary_prefix == None: return []
    entryDict = {}

    vrp_entries, irr_entries = get_entries(date, binary_prefix, roaDict.value, irrDict.value)
    
    
    vrp_origins = set()
    valid_vrp_origins = set()

    for origin, source, max_len in vrp_entries:
        vrp_origins.add(origin)
        if prefix_len <= max_len:
            valid_vrp_origins.add(origin)
    
    keys = ['AFRINIC', 'APNIC', 'ARIN', 'LACNIC', 'RIPE', 'RADB', 'ALL-IRR']
    irr_dict, irrd4_dict = {}, {}
    for key in keys:
        irr_dict[key] = set()
        irrd4_dict[key] = set()
        
    for origin, source, changed in irr_entries:
        add2dict(irr_dict, 'ALL-IRR', origin)
        add2dict(irr_dict, source, origin)
        
    results = []
    RIR = 'total'
    
    vrp_covered = 1 if len(vrp_origins) > 0 else 0

    for BGPorigins, totalCnt, rir in BGPs:
        if len(BGPorigins) == 0: continue
        numCover, numValid, numBothCover, numBothValid = [0] * 4

        if len(BGPorigins) > 1: 
            source = 'VRP'
            vrpValid = False
            vrpCnts = [numVrpCover, 0, 0, 0]
            cnts = [1] + vrpCnts + ([0] * (4 * 4))
            results.append( ((date, RIR, source), cnts))

            for key in keys:
                irr_origins = irrDict.get(key, set())
                covered = 1 if len(origins) > 0 else 0
                valid = 0
                cnts = [1, covered, valid]
                results.append( ((date, RIR, key), cnts))
        else:
            bgp_origin = BGPorigins[0]
            source = 'VRP'
            vrp_valid = 1 if bgp_origin in valid_vrp_origins else 0
            cnts = [1, vrp_covered, vrp_valid]
            results.append( ((date, RIR, source), cnts))
            
            for key in keys:
                irr_origins = irrDict.get(key, set())
                covered = 1 if len(origins) > 0 else 0
                valid = 1 if bgp_origin in irr_origins else 0 
                cnts = [1, covered, valid]
                results.append( ((date, RIR, key), cnts))

    return results

def addCount(valA, valB):
    return list(map(lambda x: x[0] + x[1], zip(valA, valB)))

def build_dict(files, parse_func):
    if len(files) == 0:
        irr_dict = sc.broadcast({})
    else:
        irr_dict = sc.textFile(','.join(files))\
                    .flatMap(lambda line: parse_func(line))\
                    .groupByKey()\
                    .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                    .collectAsMap()
        
        irr_dict = sc.broadcast(irr_dict)

    return irr_dict

def bgpCoverage(bgp_dir, irr_dir,  roa_dir, hdfs_dir, local_dir, start, end):
    make_dirs(hdfs_dir, local_dir)

    savePath = savePath + 'raw/'
    
    make_dirs(hdfs_dir, local_dir)
    
    roa_files = get_files(roa_dir, extension='.tsv')
    irr_files = get_files(irr_dir, extension='.tsv')
    bgp_files = get_files(bgp_dir, extension='.tsv')

    bgp_files = list(filter(lambda x: start <= get_date(x) <= end, bgp_files))
    irr_files = list(filter(lambda x: start <= get_date(x) <= end, irr_files))
    roa_files = list(filter(lambda x: start <= get_date(x) <= end, roa_files))

    dates = set(map(get_date, bgp_files))
    dates = dates.union(set(map(get_date, irr_files)))
    dates = dates.union(set(map(get_date, roa_files)))

    for year in range(int(start[:4]), int(end[:4]), -1):
        print("start {}".format(year))
        year = str(year)

        target_dates = sorted(list(filter(lambda x: x.startswith(year), dates)), reverse=True)

        if len(target_dates) == 0: continue

        conf = SparkConf(
                        ).setAppName(
                            "BGP coverage: {}".format(year)
                        ).set(
                            "spark.kryoserializer.buffer.max", "1g"
                        ).set(
                            "spark.kryoserializer.buffer", "2m"
                        )
        sc = SparkContext(conf=conf)
        spark = SparkSession(sc)
        sc.setLogLevel("WARN")

        for date in target_dates:
            print('[broadcast start]: {}'.format(datetime.now()))
            if len(targetDates) == 0: continue
            start, end = targetDates[0], targetDates[-1]

            appname = "{} BGP coverage: {}-{}".format(ip_version, start, end)
            
            target_roa_files = sorted(list(filter(lambda x: get_date(x) in target_dates, roa_files)))
            target_irr_files = sorted(list(filter(lambda x: get_date(x) in target_dates, irr_files)))
            target_bgp_files = sorted(list(filter(lambda x: get_date(x) in target_dates, bgp_files)))

            irr_dict = build_dict(target_irr_files, parseIRR)

            roa_dict = build_dict(target_roa_files, parseVRP)

            
            BGPRecords  = sc.textFile(','.join(target_bgp_files))\
                            .flatMap(lambda line: parseBGP(line))\
                            .groupByKey()
            
            results = BGPRecords.flatMap(lambda row: getResults(row, roaDict, irrDict, date, filterTooSpecific=True, ip_version=ip_version))\
                                .reduceByKey(addCount)\
                                .flatMap(toCSV)

            write_result(results, hdfs_dir + filename, local_dir + filename, extension='.csv')

            irrDict.unpersist()
            roaDict.unpersist()
        sc.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='analyze BGP coverage')
    parser.add_argument('--start', default='20110101')
    parser.add_argument('--end', default='20230301')

    parser.add_argument('--bgp_dir', default='/user/mhkang/bgp/routeview-reduced/')

    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
    
    parser.add_argument('--irr_dir', nargs='+', default=['/user/mhkang/radb/daily-tsv-w-changed/', '/user/mhkang/irrs/daily-tsv-w-changed/'])

    parser.add_argument('--hdfs_dir', default='/user/mhkang/bgp/covered-all/')
    parser.add_argument('--local_dir', default='/home/mhkang/bgp/covered-all/')
    
    parser.parse_args()
    args = parser.parse_args()
    print(args)

    bgpCoverage(args.bgp_dir, args.irr_dir, args.roa_dir
        , args.hdfs_dir, args.local_dir, args.start, args.end)

