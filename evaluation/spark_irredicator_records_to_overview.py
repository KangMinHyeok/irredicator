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

from utils.utils import write_result, get_files, make_dirs


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


def parseIRR(line, vrp_dict, ip_version='ipv4'):
    vrp_dict =  vrp_dict.value

    if line is None: return []
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []
    
    prefix_addr, prefix_len = prefix.split('/')
    prefix_len = int(prefix_len)
    binary_prefix = ip2binary(prefix_addr, prefix_len)

    tree, record_set = vrp_dict.get(date, ({}, {}))
    records = get_records(tree, record_set, binary_prefix)
    
    results = []

    new_key = (date, prefix, origin, source)
    if len(records) == 0:
        results.append( (new_key, (country, isp, changed, '')) )
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

        results.append( (new_key, (country, isp, changed, validation)) )
    
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
    results.append( (date, (prefix_addr, prefix_len, max_len, origin, isp)) )
    return results

def parseRecord(line):
    date, rir, prefix_addr, prefix_len, origin, isp, sumRel, validation, source, record_type, score0, score1 = line.replace('\n', '').split('\t')

    prefix = '{}/{}'.format(prefix_addr, prefix_len)
    
    key = (date, prefix, origin, source)
    try:
        pred = softmax([float(score0), float(score1)])[1]
        if pred > 0.01:
            irredicator_status = 'valid'
        else:
            irredicator_status = 'invalid'
    except:
        irredicator_status = ''

    return [(key, irredicator_status)]


def toResult(row):
    key, value = row
    date, prefix, origin, source = key
    irr_records, irredicator_status_records = value
    
    irr_changed = 0
    irr_record = ['','','','']
    
    for irr in irr_records:
        country, isp, changed, validation = irr
        try:
            if int(changed) > int(irr_changed):
                irr_changed = changed
                irr_record = irr
        except:
            pass
    
    country, isp, changed, validation = irr_record
    

    irredicator_status = ''
    if irredicator_status_records is not None and len(irredicator_status_records) > 0:
        for irredicator_status_record in irredicator_status_records:
            irredicator_status = irredicator_status_record
    
    latest_rpki_status = []
    latest_irredicator_status = []

    if validation == '':
        
        if irredicator_status != '':
            latest_irredicator_status.append((irredicator_status, date))
    else:
        latest_rpki_status.append((validation, date))
        latest_irredicator_status.append((validation, date))

    latest_rpki_status = json.dumps(latest_rpki_status)
    latest_irredicator_status = json.dumps(latest_irredicator_status)
    record = [prefix, origin, source, country, isp, changed, latest_rpki_status, latest_irredicator_status]
    
    result = []
    try:
        datetime.strptime(date, "%Y%m%d")
        datetime.strptime(changed, "%Y%m%d")
    except:
        return result
    
    result.append('\t'.join(record))
    return result


def getOverview(record_dir, irr_dir, vrp_dir, hdfs_dir, local_dir):

    hdfs_dir = hdfs_dir + 'raw/'
    make_dirs(hdfs_dir, local_dir)

    record_files = sorted(get_files(record_dir, extension='.tsv'))
    irr_files = get_files(irr_dir, extension='.tsv')
    vrp_files = get_files(vrp_dir, extension='.tsv')
    
    target_dates = list(map(lambda x: get_date(x), record_files))

    out_files = list(filter(lambda x: x.endswith('.tsv'), os.listdir(local_dir) ))
    curr_dates =  list(map(lambda x: get_date(x), out_files))

    target_dates = sorted(list(set(target_dates) - set(curr_dates)), key= lambda x: int(x))

    if len(target_dates) == 0:
        print("len(target_dates) == 0")
        exit()
    print('target_dates: {}-{}'.format(target_dates[0], target_dates[-1]))

    prev_vrp_files = None
    for date in target_dates:
        print(date)
        conf = SparkConf().setAppName(
                    "Overview {}".format(date)
                    ).set(
                        "spark.kryoserializer.buffer.max", "512m"
                    ).set(
                        "spark.kryoserializer.buffer", "1m"
                    )
    
        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")
        curr_record_files = list(filter(lambda x: get_date(x) == date, record_files))

        curr_irr_files = list(filter(lambda x: get_date(x) == date, irr_files))

        curr_vrp_files = list(filter(lambda x: get_date(x) == date, vrp_files))
        try:
            vrp_dict = sc.textFile(','.join(curr_vrp_files))\
                        .flatMap(parseVRP)\
                        .groupByKey()\
                        .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))
        except: 
            if prev_vrp_file is None: continue
            curr_vrp_files = prev_vrp_files
            
            vrp_dict = sc.textFile(','.join(curr_vrp_files))\
                        .flatMap(parseVRP)\
                        .groupByKey()\
                        .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))
        
        prev_vrp_files = curr_vrp_files

        vrp_dict = vrp_dict.collectAsMap()
            
        vrp_dict = sc.broadcast(vrp_dict)

        irr_records = sc.textFile(','.join(curr_irr_files))\
                        .flatMap(lambda x: parseIRR(x, vrp_dict))\
                        .groupByKey()

        irredicator_records = sc.textFile(','.join(curr_record_files))\
                        .flatMap(parseRecord)\
                        .groupByKey()

        irr_records = irr_records.leftOuterJoin(irredicator_records)\
                                .flatMap(toResult)

        write_result(irr_records, hdfs_dir + date, local_dir + date, extension='.tsv')
            

        sc.stop()

def main():
    parser = argparse.ArgumentParser(description='extract BGP features\n')

    parser.add_argument('--record_dir', default='/user/mhkang/irredicator/records/')
    parser.add_argument('--irr_dir', default='/user/mhkang/irrs/daily-tsv/')
    parser.add_argument('--vrp_dir', default='/user/mhkang//vrps/daily-tsv/')

    parser.add_argument('--hdfs_dir', default='/user/mhkang/irredicator/overviews/')
    parser.add_argument('--local_dir', default='/home/mhkang/rpki-irr/outputs/irredicator/overviews/')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    getOverview(args.record_dir, args.irr_dir, args.vrp_dir, args.hdfs_dir, args.local_dir)

if __name__ == '__main__':

    main()



