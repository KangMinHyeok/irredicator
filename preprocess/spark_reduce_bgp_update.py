import os
import sys
import time
import shutil
import argparse
import random
import json

from multiprocessing import Pool
from datetime import *
from operator import add
from operator import itemgetter
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
from multiprocessing import Process
import subprocess
import pydoop.hdfs as hdfs

sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from as_info.as2isp import AS2ISP
from utils.utils import write_result, append2dict, get_files, get_dates, get_date, make_dirs

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
        prefix_addr, prefix_len = record[0], record[1]
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

def getRIR(date, prefix_addr, prefix_len, nro_dict):
    if nro_dict == None: return None
    tree, record_set = nro_dict.get(date, ({}, {}))

    binary_prefix = ip2binary(prefix_addr, prefix_len)
    records = get_records(tree, record_set, binary_prefix)
    for prefix_addr, prefix_len, rir in records:
        if rir is not None:
            return rir

    return None

def parseBGP4MP(line, asISPDict, nroDict):
    try:
            
        tokens = line.split("|")
        if len(tokens) < 3: return []
        date, prefix, origin = tokens

        asISPDict = asISPDict.value
        nroDict = nroDict.value
        prefix_addr, prefix_len = prefix.split("/")
        prefix_len = int(prefix_len)

        newOrigins = []
        if '{' in origin:
            newOrigins = origin.replace('{', '').replace('}', '').split(',')
        else:
            newOrigins.append(origin)

        origins = []
        ISPs = [] 
        countries = []
        for origin in newOrigins:   
            try:
                origin = str(int(origin))
            except:
                continue
            origins.append(origin)
            isp, country = asISPDict.get(str(origin), ("", ""))
            ISPs.append(isp)
            countries.append(country)
        if len(origins) == 0:
            return []
        rir = getRIR(date, prefix_addr, prefix_len, nroDict)

        origins = '|'.join(newOrigins)
        ISPs = '|'.join(ISPs)
        countries = '|'.join(countries)

        records = []
        records.append( ((date, rir, prefix_addr, prefix_len, origins, ISPs, countries), 1) )
    
        return records
    except Exception as e:
        raise Exception("error: {}\nline: {}".format(e, line))
        return []

def parseNRO(line): 
    date, prefix_addr, prefix_len, rir, _, _, _ = line.split(",")
    
    return [ (date, (prefix_addr, prefix_len, rir)) ]

def toCSV(row):
    key, value = row
    date, rir, prefix_addr, prefix_len, newOrigins, ISPs, countries = key
    total_cnt = value
    data = [date, rir, prefix_addr, prefix_len, newOrigins, ISPs, countries, total_cnt]
    return "\t".join(str(d) for d in data)

maxLen = 30

def reduceBGP(bgpPath, asISPPath, nroPath, hdfs_path, local_path):
    
    hdfs_path = hdfs_path + 'raw/'
    make_dirs(hdfs_path, local_path)

    start = max(list(map(lambda x: x.split('.')[0], os.listdir(local_path))))
    
    subdirs = hdfs.ls(bgpPath)
    bgp_files = []
    for subdir in subdirs:
        bgp_files += hdfs.ls(subdir)
    bgp_files = list(filter(lambda x: x.endswith('.txt'), bgp_files))
    bgp_dates = list(map(get_date, bgp_files))

    nro_files = hdfs.ls(nroPath)
    nro_files = list(filter(lambda x: x.endswith('.csv'), nro_files))
    nro_dates = get_dates(nro_files)

    
    end = max(bgp_dates)
    
    if end <= start:
        print("no new data available")
        print("end date of previpus analysis: {}".format(start))
        print("latest dates of bgp: {}, {}, and {}".format(max(bgp_dates)))
        exit()

    print("target dates: {} ~ {}".format(start, end))

    target_dates = list(filter(lambda x: start < x <= end, bgp_dates))
    
    if len(target_dates) == 0:
        print("no bgp file")
        exit()
    
    asISP = AS2ISP(asISPPath)

    batchSize = maxLen - 1
    batch = [target_dates[i:i + batchSize] for i in range(0, len(target_dates), batchSize )]

    for dates in batch:
        curr_bgp_files = sorted(list(filter(lambda x: get_date(x) in dates, bgp_files)))
        
        curr_start, curr_end = dates[0], dates[-1]

        if len(curr_bgp_files) == 0:
            print("No BGP! skip {} - {}".format(curr_start, curr_end))
            continue
        
        print("start {} - {}".format(curr_start, curr_end))
        conf = SparkConf().setAppName(
                    "reduce BGP: {} - {}".format(curr_start, curr_end)
                    ).set(
                        "spark.kryoserializer.buffer.max", "512m"
                    ).set(
                        "spark.kryoserializer.buffer", "1m"
                    )

        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")
        
        records = None
        for date in dates:
            curr_nro_files = sorted(list(filter(lambda x: get_date(x) == date, nro_files)))
            if len(curr_nro_files) == 0: 
                diff = list(map(lambda v: abs( (datetime.strptime(v, "%Y%m%d") - datetime.strptime(date, "%Y%m%d")).days), nro_dates))

                nrodate = nro_dates[diff.index(min(diff))]
                curr_nro_files = list(filter(lambda x: get_date(x) == nrodate, nro_files))

            nroDict  = sc.textFile(','.join(curr_nro_files))\
                        .flatMap(lambda line: parseNRO(line))\
                        .groupByKey()\
                        .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                        .collectAsMap()
            
            nroDict = sc.broadcast(nroDict)

            asISPDict = asISP.getASISPDict(date)
            asISPDict = sc.broadcast(asISPDict)

            curr_bgp_files = sorted(list(filter(lambda x: get_date(x) == date, bgp_files)))

            if len(curr_bgp_files) == 0: continue
            results = None

            results = sc.textFile(','.join(curr_bgp_files))\
                            .flatMap(lambda line: parseBGP4MP(line, asISPDict, nroDict))\
                            .reduceByKey(add)\
                            .map(toCSV)

            curr_hdfs_path = "{}{}".format(hdfs_path, date)
            curr_local_path = "{}{}".format(local_path, date)
            write_result(results, curr_hdfs_path, curr_local_path, extension='.tsv')
            
            
        sc.stop()
    
def reduceBGPDataset():
    parser = argparse.ArgumentParser(description='summarize as relationship\n')
    parser.add_argument('--routeviewPath', default='/user/mhkang/routeviews/update/')
    # parser.add_argument('--akamaiPath', default='/user/mhkang/bgp/akamai/update/')
    parser.add_argument('--asISPPath', default='/home/mhkang/caida/as-isp/data/')
    parser.add_argument('--nroPath', default='/user/mhkang/nrostats/ipv4-w-date/')

    parser.add_argument('--hdfs_path', default='/user/mhkang/routeviews/reduced/')
    parser.add_argument('--local_path', default='/net/data/routeviews/reduced/')
    
    parser.parse_args()
    args = parser.parse_args()
    print(args)

    reduceBGP(args.routeviewPath, args.asISPPath, args.nroPath, args.hdfs_path, args.local_path)

if __name__ == '__main__':
    reduceBGPDataset()
    
