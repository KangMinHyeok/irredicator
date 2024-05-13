import os
import sys
import time
import shutil
import argparse
import random

import json
from ipaddress import IPv4Address
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

sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from utils.utils import write_result, ip2binary, get_date, get_dates, get_files, make_dirs

def readNcollectAsMap(sc, files, parse_func):
    result = {}
    if len(files) > 0:
        result = sc.textFile(','.join(files))\
            .flatMap(lambda line: parse_func(line))\
            .groupByKey()\
            .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
            .collectAsMap()
    
    return result

def ip2binary(prefix_addr, prefix_len):
    if("." in prefix_addr): # IPv4
        octets = map(lambda v: int(v), prefix_addr.split("."))
        octets = map(lambda v: format(v, "#010b")[2:], octets)
    else: # IPv6
        octets = map(lambda v: str(v), prefix_addr.split(":"))
        prefix_addrs = prefix_addr.split(":")
        for i in range( 8 - len(prefix_addrs)):
            idx = prefix_addrs.index("")
            prefix_addrs.insert(idx, "")
        prefix_addrs += [""] * (8 - len(prefix_addrs)) # 8 groups, each of them has 16 bytes (= four hexadecimal digits)
        octets = []
        for p in prefix_addrs:
            if( len(p) != 4): # 4 bytes
                p = (4 - len(p)) * '0' + p
            for bit in p:
                b = format(int(bit, 16), "04b")
                octets.append( b)
    return "".join(octets)[:int(prefix_len)]


def make_binary_prefix_tree(records):
    tree = {}
    record_set = {}
    for record in records:
        prefix_addr, prefix_len = record[:2]
        binary_prefix = ip2binary(prefix_addr, prefix_len)

        insert(tree, record_set, binary_prefix, record)
        
        
    return tree, record_set

def insert(tree, record_set, binary_prefix, record):

    if binary_prefix not in record_set:
        record_set[binary_prefix] = set()
    
    record_set[binary_prefix].add(tuple(record))
    insert2tree(tree, binary_prefix)

def insert2tree(tree, binary_prefix):
    if len(binary_prefix) == 0:
        return
    
    subtree = tree
    for i in range(len(binary_prefix)):
        if binary_prefix[i] not in subtree:
            subtree[binary_prefix[i]] = {}

        subtree = subtree[binary_prefix[i]]

        if i == len(binary_prefix)-1:
            subtree['l']=True

def get_covered(tree, binary_prefix):
    covered=[]
    subtree = tree
    subp=""
    for t in binary_prefix:
        if t not in subtree:
            break
        subtree = subtree[t]
        subp = subp + t
        if 'l' in subtree:
            covered.append(subp)

    return sorted(covered, key=lambda x: len(x))

def get_records(tree, record_set, binary_prefix):
    
    binary_prefixes = get_covered(tree, binary_prefix)
    
    records = []

    if len(binary_prefixes) == 0:
        return records

    for binary_prefix in binary_prefixes:
        records += record_set.get(binary_prefix, [])
    
    return records

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    try: 
        date2 = datetime.strptime(date, "%Y%m%d")
        date = int(date)
        
        try: origin = int(origin.replace("AS", ""))
        except:
            try: origin = int(origin.split('#')[0])
            except: return []
        prefix_addr, prefix_len = prefix.split('/')
        prefix_len = int(prefix_len)
        # source = source.upper()
    except: return []

    results = []
    results.append( (date, (prefix_addr, prefix_len, origin)) )
    
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
        origin = int(origin)
        prefix_len = int(prefix_len)
        max_len = int(max_len) if max_len != None and max_len != "None" else prefix_len

    except Exception as e:  
        return []
    
    return [ (date, (prefix_addr, prefix_len, max_len, origin)) ]

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

    origins = list(map(int, origins))

    records.append( ((date, prefix_addr, prefix_len), (origins, totalCnt, rir)) )

    return records

def getBgpResults(row, vrp_dict, irr_dict, filterTooSpecific=True, ip_version='ipv4'):
    key, value  = row
    date, prefix_addr, prefix_len = key 
    BGPs = value
    
    if filterTooSpecific and prefix_len > 24: return []
    binary_prefix = ip2binary(prefix_addr, prefix_len)
    if binary_prefix == None: return []

    vrp_dict = vrp_dict.value
    irr_dict = irr_dict.value

    vrp_tree, vrp_record_set = vrp_dict.get(date, ({}, {}))
    irr_tree, irr_record_set = irr_dict.get(date, ({}, {}))

    vrp_records = get_records(vrp_tree, vrp_record_set, binary_prefix)
    irr_records = get_records(vrp_tree, vrp_record_set, binary_prefix)

    bgp_length = len(binary_prefix)

    vrp_covered_origins = set()
    vrp_valid_origins = set()

    for record in vrp_records:
        vrp_prefix_addr, vrp_prefix_len, vrp_max_len, vrp_origin = record

        if vrp_prefix_len <=  prefix_len:
            vrp_covered_origins.add( vrp_origin )
            if prefix_len <= vrp_max_len:
                vrp_valid_origins.add( vrp_origin )
    
    irr_origins = list(
                    map(lambda x: x[2], 
                        filter(lambda x: x[1] <= prefix_len, 
                    irr_records)
                        )
                    )
    
    results = []

    total = 1
    covered_by_vrp = len(vrp_covered_origins) > 0
    covered_by_irr = len(irr_origins) > 0
    covered_by_both = 1 if covered_by_vrp and covered_by_irr else 0

    for BGP_origins, total_cnt, rir in BGPs:
        if len(BGP_origins) <= 0: continue
        
            
        if covered_by_both == 1:
            if len(BGP_origins) > 1:
                consistent = 1
                discrepant = 0
            else:
                bgp_origin = BGP_origins[0]
                valid_against_irr = bgp_origin in irr_origins 
                valid_against_vrp = bgp_origin in vrp_valid_origins
                
                if valid_against_irr == valid_against_vrp:
                    consistent = 1
                    discrepant = 0
                else:
                    consistent = 0
                    discrepant = 1
        else:
            consistent = 0
            discrepant = 0

        results.append( (date, (total, covered_by_both, consistent, discrepant)) )

    return results

def addBGP(valA, valB):
    totalA, bothCoveredA, consistentA, discrepantA = valA
    totalB, bothCoveredB, consistentB, discrepantB = valB
    return (totalA+totalB, bothCoveredA+bothCoveredB, consistentA+consistentB, discrepantA+discrepantB)

def toCSV(row):
    date, value = row
    total, bothCovered, consistent, discrepant = value
    return ','.join(list(map(str, [date, total, bothCovered, consistent, discrepant])))

def count_discrepancy(bgp_dir, irr_dir,  roa_dir, hdfs_dir, local_dir):
    hdfs_dir = hdfs_dir + 'raw/'
    make_dirs(hdfs_dir, local_dir)

    start = max(list(map(lambda x: x.split('.')[0].split('-')[-1], os.listdir(local_dir))))

    roa_files = get_files(roa_dir, extension='.tsv')
    irr_files = get_files(irr_dir, extension='.tsv')
    bgp_files = get_files(bgp_dir, extension='.tsv')

    bgp_dates = list(map(get_date, bgp_files))
    irr_dates = list(map(get_date, irr_files))
    roa_dates = list(map(get_date, roa_files))
    end = min([max(bgp_dates), max(irr_dates), max(roa_dates)])
    
    if end <= start:
        print("no new data available")
        print("end date of previpus analysis: {}".format(start))
        print("latest dates of bgp, irr, and roa: {}, {}, and {}".format(max(bgp_dates), max(irr_dates), max(roa_dates)))
        exit()

    print("target dates: {} ~ {}".format(start, end))

    bgp_files = list(filter(lambda x: start < get_date(x) <= end, bgp_files))
    irr_files = list(filter(lambda x: start < get_date(x) <= end, irr_files))
    roa_files = list(filter(lambda x: start < get_date(x) <= end, roa_files))
    
    target_dates = sorted(list(set(get_dates(bgp_files) + get_dates(roa_files) + get_dates(irr_files))))

    bgpResults = None
    entryResults = None
    overlapResults = None

    batch_size = 3
    batch = [target_dates[i:i + batch_size] for i in range(0, len(target_dates), batch_size)]
    
    for dates in batch:
        end = sorted(dates)[-1]
        curr_bgp_files = list(filter(lambda x: get_date(x) in dates, bgp_files))
        curr_roa_files = list(filter(lambda x: get_date(x) in dates, roa_files))
        curr_irr_files = list(filter(lambda x: get_date(x) in dates, irr_files))


        conf = SparkConf(
        ).setAppName(
            "analyze inconsistent bgp coverage: {}".format(end)
        ).set(
            "spark.kryoserializer.buffer.max", "512m"
        ).set(
            "spark.kryoserializer.buffer", "1m"
        )

        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")

        vrp_dict = readNcollectAsMap(sc, curr_roa_files, parseVRP)
        irr_dict = readNcollectAsMap(sc, curr_irr_files, parseIRR)

        vrp_dict = sc.broadcast(vrp_dict)
        irr_dict = sc.broadcast(irr_dict)
                        
        BGPRecords  = sc.textFile(','.join(curr_bgp_files))\
                            .flatMap(parseBGP)\
                            .groupByKey()
    
        bgpResult = BGPRecords.flatMap(lambda row: getBgpResults(row, vrp_dict, irr_dict))\
                                .reduceByKey(addBGP)\
                                .map(toCSV)
        
        filename = 'inconsistent-bgp-{}'.format(end)

        write_result(bgpResult, hdfs_dir + filename, local_dir + filename, extension='.csv')
        
        sc.stop()
    
    

def main():
    parser = argparse.ArgumentParser(description='summarize as relationship\n')

    parser.add_argument('--bgp_dir', default='/user/mhkang/routeviews/reduced/')
    parser.add_argument('--irr_dir', default='/user/mhkang/irrs/daily-tsv/')
    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
    
    parser.add_argument('--hdfs_dir', default='/user/mhkang/rpki-irr/outputs/analysis/inconsistent-bgp/')
    parser.add_argument('--local_dir', default='/home/mhkang/rpki-irr/outputs/analysis/inconsistent-bgp/')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    count_discrepancy(args.bgp_dir, args.irr_dir, args.roa_dir, args.hdfs_dir, args.local_dir)
    
if __name__ == '__main__':
    main()


