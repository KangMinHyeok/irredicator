import os
import sys
import time as t
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

from as_info import as2rel
from utils.utils import write_result, get_files, make_dirs

def sumRels(rels):
    result = "none"
    if len(rels) == 0: return 'not-covered'
    elif len(rels) == 1: result = rels[0]
    else:
        priority = ["self", "same", "sameISP", "customer-provider", "provider", "customer", "peer", "peering", "ddos", "none", "None"]
        
        for key in priority:
            if key in rels:
                result = key
                break

    keyMap = {  "self":"same", 
                "same":"same", 
                "sameISP": "sameISP",
                "provider":"customer-provider", 
                "customer": "customer-provider",
                "customer-provider": "customer-provider",
                "peer":"peering",
                "peering":"peering",
                "ddos":"ddos", 
                "None":"none",
                "none":"none"
            }

    result = keyMap.get(result, "none") 
    
    return result

def get_date(filename):
    date = filename
    if '/' in date: date = date.split('/')[-1]
    if '.' in date: date = date.split('.')[0]
    if '-' in date: date = date.split('-')[0]
    return date

def get_dates(files):
    return sorted(list(set(map(get_date, files))))


def ip2binary(prefix_addr, prefix_len):
    if("." in prefix_addr): # IPv4
        octets = map(lambda v: int(v), prefix_addr.split("."))
        octets = map(lambda v: format(v, "#010b")[2:], octets)
    else: # IPv6
        return "exception"
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
        if binary_prefix == 'exception':
            raise Exception("record: {}".format(record))

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


def parseIRR(line, vrp_dict, as2rel_dict, ip_version='ipv4'):
    vrp_dict =  vrp_dict.value
    as2rel_dict = as2rel_dict.value

    if line is None: return []
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []
    
    prefix_addr, prefix_len = prefix.split('/')
    prefix_len = int(prefix_len)
    binary_prefix = ip2binary(prefix_addr, prefix_len)

    tree, record_set = vrp_dict.get(date[:6], ({}, {}))
    records = get_records(tree, record_set, binary_prefix)
    
    results = []

    new_key = (origin, prefix_addr, prefix_len)
    if len(records) == 0:
        results.append( (new_key, (isp, rir, 'unknown', 'not-covered', source, date)) )
    else:
        validation, rels = 'invalid', []

        for record in records:
            _prefix_addr, _prefix_len, _max_len, _origin, _isp = record
            sameOrigin = (_origin == origin)
            matched = (_prefix_len <= prefix_len <= _max_len)
            if matched and sameOrigin:  
                validation = 'valid'
                rels.append('same')
                break
            
            if matched:
                if isp == _isp: rels.append('sameISP')
                else:
                    rel = as2rel_dict.get(origin + '+' + _origin, 'none')
                    rels.append(rel)

        results.append((new_key, (isp, rir, validation, sumRels(rels), source, date) ))
    
    return results

def parseVRP(line, ip_version='ipv4'):
    if line is None: return None
    date, prefix_addr, prefix_len, max_len, origin, num_ip, country, rir, isp, cc = line.split("\t")
    
    if ip_version == 'ipv4':
        if ':' in prefix_addr: return []
        if '.' not in prefix_addr: return []
    elif ip_version == 'ipv6': 
        if '.' in prefix_addr: return []
        if ':' not in prefix_addr: return []

    prefix_len = int(prefix_len)
    max_len = int(max_len)
    
    results = []
    results.append( (date[:6], (prefix_addr, prefix_len, max_len, origin, isp)) )
    return results

def parseBGPFeature(line, ip_version='ipv4'):
    if line is None: return None
    tokens = line.split("\t")

    date, prefix_addr, prefix_len, origin = tokens[:4]

    feature = tokens[4:]
    
    prefix_len = int(prefix_len)

    results = []
    results.append( ((origin, prefix_addr, prefix_len), feature) )
    return results

def toIrrFeature(row):
    key, value = row
    irr, feature = value

    origin, prefix_addr, prefix_len = key
    isp, rir, validation, sumRel, source, date = irr

    if feature is None: 
        return []
        # feature = []
    result = [date, prefix_addr, prefix_len, origin, isp, rir, validation, sumRel, source] + feature
    result = ','.join(list(map(str, result)))
    return [result]


def getIRRFeature(feature_dir, rel_dir, irr_dir, vrp_dir, hdfs_dir, local_dir):

    hdfs_dir = hdfs_dir + 'raw/'
    make_dirs(hdfs_dir, local_dir)

    feature_files = sorted(get_files(feature_dir, extension='.tsv'))
    irr_files = get_files(irr_dir, extension='.tsv')
    vrp_files = get_files(vrp_dir, extension='.tsv')
    
    targets = list(map(lambda x: get_date(x), feature_files))

    today = str(datetime.today()).split(' ')[0]
    print(today)
    end = str(today).replace('-', '')

    start = '20230201'
    currfiles = sorted(os.listdir(local_dir))
    if len(currfiles) > 0:
        start = currfiles[-1].split('.')[0]
        
    print('start: {}'.format(start))
    print('end: {}'.format(end))
    
    year, month = int(start[:4]), int(start[4:6])
    month -= 1
    if month == 0:
        month = 12
        year -= 1
    feature_start = '{}{:02}'.format(year, month)

    asRel = as2rel.AS2Rel(path=rel_dir)
    prev_vrp_file = None
    for feature_file in feature_files:
        date = get_date(feature_file)
        if date < feature_start or date >= end[:6]: continue
        print(date)
        conf = SparkConf().setAppName(
                    "IRR features {}".format(date)
                    ).set(
                        "spark.kryoserializer.buffer.max", "512m"
                    ).set(
                        "spark.kryoserializer.buffer", "1m"
                    )
    
        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")

        feature_records = sc.textFile(','.join([feature_file]))\
                        .flatMap(parseBGPFeature)

        print("feature_records.top(1)")
        print(feature_records.top(1))
        
        as2rel_dict = asRel.getASRelDic(date+'01')
        as2rel_dict = sc.broadcast(as2rel_dict)
        year, month = int(date[:4]), int(date[4:])
        month += 1
        if month == 13: 
            month = 1
            year += 1
        
        target_date = "{}{:02}".format(year, month)
        
        curr_irr_files = list(filter(lambda x: get_date(x).startswith(target_date), irr_files))
        for curr_irr_file in curr_irr_files:
            irr_date = get_date(curr_irr_file)
            curr_vrp_file = list(filter(lambda x: get_date(x) == irr_date, vrp_files))
            try:
                vrp_dict = sc.textFile(','.join(curr_vrp_file))\
                            .flatMap(parseVRP)\
                            .groupByKey()\
                            .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))
            except: 
                if prev_vrp_file is None: continue
                curr_vrp_file = prev_vrp_file
                
                vrp_dict = sc.textFile(','.join(curr_vrp_file))\
                            .flatMap(parseVRP)\
                            .groupByKey()\
                            .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))
            prev_vrp_file = curr_vrp_file

            vrp_dict = vrp_dict.collectAsMap()
                
            vrp_dict = sc.broadcast(vrp_dict)

            irr_records = sc.textFile(','.join([curr_irr_file]))\
                            .flatMap(lambda x: parseIRR(x, vrp_dict, as2rel_dict))\

            print("irr_records.top(1)")
            print(irr_records.top(1))

            irr_features = irr_records.leftOuterJoin(feature_records)\
                                    .flatMap(toIrrFeature)

            write_result(irr_features, hdfs_dir + irr_date, local_dir + irr_date, extension='.tsv')
            

        sc.stop()

def main():
    parser = argparse.ArgumentParser(description='extract BGP features\n')

    parser.add_argument('--feature_dir', default='/user/mhkang/routeviews/feature/')
    parser.add_argument('--rel_dir', default='/home/mhkang/caida/as-rel/data/')
    parser.add_argument('--irr_dir', default='/user/mhkang/irrs/daily-tsv/')
    parser.add_argument('--vrp_dir', default='/user/mhkang//vrps/daily-tsv/')

    parser.add_argument('--hdfs_dir', default='/user/mhkang/irrs/bgp-feature/')
    parser.add_argument('--local_dir', default='/net/data/irrs/bgp-feature/')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    getIRRFeature(args.feature_dir, args.rel_dir, args.irr_dir, args.vrp_dir, args.hdfs_dir, args.local_dir)

if __name__ == '__main__':

    main()



