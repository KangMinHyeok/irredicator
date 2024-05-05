import os
import sys
import shutil
import argparse
import traceback
import numpy as np
import ujson as json
import pydoop.hdfs as hdfs
from ipaddress import IPv4Address

from multiprocessing import Pool
import time
from datetime import *
from operator import itemgetter
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast

sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from as_info.as2rel import AS2Rel
# from utils.binaryPrefixTree import make_binary_prefix_tree, get_records
from utils.utils import write_result, get_date, get_dates, get_files, make_dirs    

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

def parseVRP(line, ip_version='ipv4'):
    if line.startswith('#'): return []
    tokens = line.split('\t')
    date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir, isp  = tokens[:9]
    if ip_version == 'ipv4' and ':' in prefix_addr: return []
    elif ip_version == 'ipv6' and '.' in prefix_addr: return []

    try: 
        origin = int(origin)
        prefix_len = int(prefix_len)
        max_len = int(max_len) if max_len != None and max_len != "None" else prefix_len

    except Exception as e:  
        return []
    
    return [ (date, (prefix_addr, prefix_len, max_len, origin, isp, cc, rir)) ]

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    try: 
        source = source.upper()
        origin = int(origin)        
        prefix_addr, prefix_len = prefix.split('/')
        prefix_len = int(prefix_len)
    except: return []

    results = []
    results.append( ((date, source, prefix_addr, prefix_len), (origin, isp, country, rir)) )
            
    return results

def count_inconsistent_prefix(row, vrp_dict):
    key, values = row
    date, source, prefix_addr, prefix_len = key

    vrp_dict = vrp_dict.value
    tree, record_set = vrp_dict.get(date, ({}, {}))
    
    binary_prefix = ip2binary(prefix_addr, prefix_len)      

    irr_records = list(values)
    vrp_records = get_records(tree, record_set, binary_prefix)

    results = []
    if len(vrp_records) != 0 and len(irr_origins) != 0:
        vrp_origins = set(map(lambda x: x[1], vrp_records))
        irr_origins = set(map(lambda x: x[0], irr_records))
        
        results.append( ((date, 'ALL-IRR', 'overalp'), 1) )
        results.append( ((date, source, 'overalp'), 1) )

        same_origins = irr_origins.intersection(vrp_origins)
        discrepancy = 'same' if len(same_origins) > 0  else 'discrepant'
        results.append( ((date, 'ALL-IRR', discrepancy), 1) )
        results.append( ((date, source, discrepancy), 1) )

    return results

def addCount(valA, valB):
    return valA + valB

def toCSV(row):
    date, value = row

    dic = {}
    for source, rel, cnt in value:
        if source not in dic:
            dic[source] = {}
        
        dic[source][rel] = cnt
    
    records = []
    rels = ['overalp', 'same', 'discrepant']
    for source in dic.keys():
        record = [date, source]
        record += [dic[source].get(rel, 0) for rel in rels]
        records.append(','.join(list(map(str, record))))

    return records

def analyzeInconsistentObjects(irr_dir, roa_dir, hdfs_dir, local_dir, as_rel_dir):
    hdfs_dir = hdfs_dir + 'raw/'
    make_dirs(hdfs_dir, local_dir)

    irr_files = get_files(irr_dir, extension='.tsv')
    roa_files = get_files(roa_dir, extension='.tsv')
    
    files = list(filter(lambda x: x.startswith('inconsistent-prefix'), os.listdir(local_dir)))
    start = max(list(map(lambda x: x.split('.')[0].split('-')[-1], files)))

    irr_dates = list(map(get_date, irr_files))
    roa_dates = list(map(get_date, roa_files))
    end = min([max(irr_dates), max(roa_dates)])
    
    if end <= start:
        print("no new data available")
        print("end date of previpus analysis: {}".format(start))
        print("latest dates of nro, irr, and roa: {}, {}, and {}".format(max(nro_dates), max(irr_dates), max(roa_dates)))
        exit()

    print("target dates: {} ~ {}".format(start, end))
    target_dates = sorted(list(filter(lambda x: start < x <= end, list(set(irr_dates).union(set(roa_dates))))))

    batch_size = 7
    batches = [target_dates[i:i + batch_size] for i in range(0, len(target_dates), batch_size)]
    print(batches)
    for batch in batches:
        curr_start, curr_end = batch[0], batch[-1]
        conf = SparkConf(
                ).setAppName(
                    "inconsistent object: {}-{}".format(curr_start, curr_end)
                ).set(
                    "spark.kryoserializer.buffer.max", "512m"
                ).set(
                    "spark.kryoserializer.buffer", "1m"
                )

        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")

        curr_roa_files = list(filter(lambda x: curr_start <= get_date(x) <= curr_end, roa_files))
        curr_irr_files = list(filter(lambda x: curr_start <= get_date(x) <= curr_end, irr_files))
        
        if len(curr_roa_files) <= 0:
            print("len(curr_roa_files) <= 0")
            continue
        if len(curr_irr_files) <= 0:
            print("len(curr_irr_files) <= 0")
            continue

        roa_dict = {}
        if len(curr_roa_files) > 0:
            roa_dict = sc.textFile(','.join(curr_roa_files))\
                            .flatMap(parseVRP)\
                            .groupByKey()\
                            .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                            .collectAsMap()
    
        roa_dict = sc.broadcast(roa_dict)

        irr_records = sc.textFile(','.join(curr_irr_files))\
                        .flatMap(parseIRR)\
                        .groupByKey()
        
        results = irr_records.flatMap(lambda row: count_inconsistent_prefix(row, roa_dict))\
                        .reduceByKey(addCount)\
                        .map(lambda row: (row[0][0], (row[0][1], row[0][2], row[1]) ) )\
                        .groupByKey()\
                        .flatMap(toCSV)
    
        filename = 'inconsistent-prefix-{}'.format(end)
        write_result(results, hdfs_dir + filename, local_dir + filename, extension='.csv')

        sc.stop()
        break
    

def main():
    parser = argparse.ArgumentParser(description='irr rpki as relationship\n')
    parser.add_argument('--as_rel_dir', default='/home/mhkang/caida/as-rel/data/')

    parser.add_argument('--irr_dir',default='/user/mhkang/irrs/daily-tsv/')
    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')

    parser.add_argument('--hdfs_dir', default='/user/mhkang/rpki-irr/outputs/analysis/inconsistent-prefix/')
    parser.add_argument('--local_dir', default='/home/mhkang/rpki-irr/outputs/analysis/inconsistent-prefix/')
    
    parser.parse_args()
    args = parser.parse_args()
    print(args)
    analyzeInconsistentObjects(args.irr_dir, args.roa_dir, args.hdfs_dir, args.local_dir, args.as_rel_dir)

if __name__ == "__main__":
    main()

