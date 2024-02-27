import os
import sys
import time
import shutil
import argparse
import random
import math

import numpy as np
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

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from utils.utils import add2dict, date_diff, get_date
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records


def originToInt(origin):
    intOrigin = -1
    try: 
        intOrigin = int(origin)
    except Exception as e: 
        try:
            upper, lower = origin.split('.')
            intOrigin = int((int(upper) << 16) + int(lower))
        except:
            pass
    return intOrigin

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

    origins = list(filter(lambda x: x != -1, map(originToInt, origins)))
    if len(origins) <= 0: return []

    records.append( ((date, prefix_addr, prefix_len), (origins, totalCnt, rir)) )

    return records

def addTotalCnt(valueA, valueB):
    unique_cntA, total_cntA = valueA
    unique_cntB, total_cntB = valueB
    return (unique_cntA, total_cntA + total_cntB)

def addBothCnt(valueA, valueB):
    unique_cntA, total_cntA = valueA
    unique_cntB, total_cntB = valueB
    return (unique_cntA + unique_cntB, total_cntA + total_cntB)

def softmax(x):
    e_x = np.exp(x - np.max(x))
    return e_x / e_x.sum()

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")
    
    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    try:
        if date == source: source = "RADB"
        date2 = datetime.strptime(date, "%Y%m%d")
        date = int(date)

        
        try: origin = originToInt(origin.replace("AS", ""))
        except:
            try: origin = originToInt(origin.split('#')[0])
            except: return []
        prefix_addr, prefix_len = prefix.split('/')
        prefix_len = int(prefix_len)
        source = source.upper()
    except: return []

    results = []
    results.append( (date, (prefix_addr, prefix_len, origin, source)) )
            
    return results

def parseIRR2(line, ip_version='ipv4'):
    tokens = line.split("\t")
    try:
        date, rir, prefix, origin, isp, sumRel, validation, source, _type, is_test, score0, score1 = tokens
    except:
        raise Exception("line: {}\ntokens: {}\nlen(tokens): {}".format(line, tokens, len(tokens)))

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    try: 
        date2 = datetime.strptime(date, "%Y%m%d")
        date = int(date)
        if '#' in origin: return []        
        origin = int(origin)
        prefix_addr, prefix_len = prefix.split('/')
        prefix_len = int(prefix_len)
        if validation == 'valid': y_true = 1
        elif validation == 'invalid': y_true = 0
        else: y_true = -1
        
        if _type == 'inactive':
            y_pred = 0.0
        elif _type == 'excluded':
            y_pred = 1.0
        else:
            # y_pred = y_pred.replace(']', '').replace('[', '').strip().split(' ')
            # y_pred = list(filter(lambda x: x != '', y_pred))
            # y_pred = softmax(list(map(float, y_pred)))[1]    
            y_pred = softmax(list(map(float, [score0, score1])))[1]

    except Exception as e:
        print(e)
        print(line)
        raise Exception(e)
        # return []

    results = []
    results.append( (date, (prefix_addr, prefix_len, origin, y_true, y_pred, source)) )

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
        origin = originToInt(origin)
        prefix_len = int(prefix_len)
        max_len = int(max_len) if max_len != None and max_len != "None" else prefix_len

    except Exception as e:  
        return []
    
    return [ (date, (prefix_addr, prefix_len, max_len, origin, 'VRP')) ]
    
def getOrigins(date, binary_prefix, irr_dict, ip_version='ipv4'):
    if binary_prefix == None: return [], [], []
    bgp_length = len(binary_prefix)

    tree, record_set = irr_dict.get(date, ({}, {}))
    irr_records = get_records(tree, record_set, binary_prefix)
    irr_records = list(filter(lambda x: x[1] <= bgp_length, irr_records))
    irr_records += list(map(lambda x: tuple(list(x)[:-1] + ['ALL-IRRs']), irr_records))

    irrd4_records = list(filter(lambda x: x[3] != 0, irr_records))
    irrml_records = list(filter(lambda x: x[3] == 1 or x[4] > 0.0, irr_records))
    
    return set(irr_records), set(irrd4_records), set(irrml_records)



def getActiveRecords(row, irr_dict, filterTooSpecific=True, ip_version='ipv4'):
    key, value  = row
    date, prefix_addr, prefix_len = key 
    BGPs = value
    
    if filterTooSpecific and prefix_len > 24: return []

    binary_prefix = ip2binary(prefix_addr, prefix_len)
    if binary_prefix == None: return []

    irr_records, irrd4_records, irrml_records = getOrigins(date, binary_prefix, irr_dict.value)
    
    irr_dict, irrd4_dict, irrml_dict = {}, {}, {}

    for prefix_addr, prefix_len, origin, y_true, y_pred, source in irr_records:
        add2dict(irr_dict, origin, (prefix_addr, prefix_len, origin, source))

    for prefix_addr, prefix_len, origin, y_true, y_pred, source in irrd4_records:
        add2dict(irrd4_dict, origin, (prefix_addr, prefix_len, origin, source))
    
    # thresholds = [0.00001, 0.0001, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    t = 0.01
    for prefix_addr, prefix_len, origin, y_true, y_pred, source in irrml_records:
        # for t in thresholds:
        if t not in irrml_dict:
            irrml_dict[t] = {}
        if y_pred >= t:
            add2dict(irrml_dict[t], origin, (prefix_addr, prefix_len, origin, source))
        
    results = []

    for BGPorigins, totalCnt, rir in BGPs:
        if len(BGPorigins) == 0: continue

        if len(BGPorigins) == 1: 
            bgpOrigin = BGPorigins[0]
            
            irrRecords = irr_dict.get(bgpOrigin, [])
            jobRecords = irrd4_dict.get(bgpOrigin, [])

            for prefix_addr, prefix_len, origin, source in irrRecords:
                if origin == bgpOrigin:
                    results.append( (source, 'IRR', prefix_addr, prefix_len, origin))
            
            for prefix_addr, prefix_len, origin, source in jobRecords:
                if origin == bgpOrigin:
                    results.append( (source, 'IRRd4', prefix_addr, prefix_len, origin))

            # for t in thresholds:
            if t not in irrml_dict: continue
            ourRecords = irrml_dict[t].get(bgpOrigin, [])
            for prefix_addr, prefix_len, origin, source in ourRecords:
                if origin == bgpOrigin:
                    results.append( (source, 'IRR-ML', prefix_addr, prefix_len, origin))

    return results


def toCSV(row):
    key, cnt = row
    source, _type = key
    return [source, _type, cnt]

def addCount(valA, valB):
    return list(map(lambda x: x[0] + x[1], zip(valA, valB)))

def getRecord(sc, files, func):
    records = sc.textFile(','.join(files))\
                .flatMap(func)
    return records  

def toDict(sc, recrods):
    dic = recrods.groupByKey()\
                .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                .collectAsMap()
    
    dic = sc.broadcast(dic)
    return dic


def getTotalRecords(row):
    date, value = row
    prefix_addr, prefix_len, origin, y_true, y_pred, source = value

    # thresholds = [0.00001, 0.0001, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    t = 0.01
    results = []
    results.append( (source, 'IRR', prefix_addr, prefix_len, origin))
    results.append( ("ALL-IRRs", 'IRR', prefix_addr, prefix_len, origin))
    if y_true != 0:
        results.append( (source, 'IRRd4', prefix_addr, prefix_len, origin))
        results.append( ("ALL-IRRs", 'IRRd4', prefix_addr, prefix_len, origin))

    if y_pred >= t or y_true == 1:
        results.append( (source, 'IRR-ML', prefix_addr, prefix_len, origin))
        results.append( ("ALL-IRRs", 'IRR-ML', prefix_addr, prefix_len, origin))

    return results
    
def getTotalPrefix(row):
    date, value = row
    prefix_addr, prefix_len, origin, y_true, y_pred, source = value

    # thresholds = [0.00001, 0.0001, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

    t = 0.01
    results = []
    results.append( (source, 'IRR', prefix_addr, prefix_len))
    results.append( ("ALL-IRRs", 'IRR', prefix_addr, prefix_len))
    if y_true != 0:
        results.append( (source, 'IRRd4', prefix_addr, prefix_len))
        results.append( ("ALL-IRRs", 'IRRd4', prefix_addr, prefix_len))
    
    if y_pred >= t or y_true == 1:
        results.append( (source, 'IRR-ML', prefix_addr, prefix_len))
        results.append( ("ALL-IRRs", 'IRR-ML', prefix_addr, prefix_len))

    return results


def getValue(dic, k1, k2):
    return dic.get(k1, {}).get(k2, 0)

def getActiveNTotal(activeDict, totalDict, source, _type):
    return activeDict.get(source, {}).get(_type, 0), totalDict.get(source, {}).get(_type, 0)

activeToday(args.bgp_dir, args.roa_dir, args.irr_files, args.local_dir)

def activeToday(date, bgp_dir, roa_dir, irr_files, local_dir):

    bgp_files = get_files(bgp_dir, extension='.tsv')
    roa_files = get_files(roa_dir, extension='.tsv')
    
    
    bgp_files = sorted(list(filter(lambda x: 0 <= date_diff(get_date(x), date) < 14, bgp_files)))
    roa_files = sorted(list(filter(lambda x: get_date(x) == targetDate, roaFiles)))
    
    conf = SparkConf(
            ).setAppName(
                "BGP active: {}".format(targetDate)
            ).set(
                "spark.kryoserializer.buffer.max", "512m"
            ).set(
                "spark.kryoserializer.buffer", "1m"
            )

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)

    sc.setLogLevel("WARN")

    irr_records = get_records(sc, irr_files, parseIRR2)
    bgp_records = get_records(sc, bgp_files, parseBGP)
    
    irr_dict = irr_records.groupByKey()\
                .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                .collectAsMap()
    
    irr_dict = sc.broadcast(irr_dict)

    active_results = bgp_records.groupByKey()\
                        .flatMap(lambda row: getActiveRecords(row, irr_dict, filterTooSpecific=True))\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1]), 1))\
                        .reduceByKey(add)\
                        .map(toCSV)\
                        .collect()
    
    print(active_results)
    activeDict = {}
    for source, _type, cnt in active_results:
        if source not in activeDict:
            activeDict[source] = {}
        activeDict[source][_type] = cnt
    
    totalResults = irr_records.flatMap(getTotalRecords)\
                        .map(lambda row: ((row[0], row[1]), 1))\
                        .reduceByKey(add)\
                        .map(toCSV)\
                        .collect()

    totalDict = {}
    for source, _type, cnt in totalResults:
        if source not in totalDict:
            totalDict[source] = {}
        totalDict[source][_type] = cnt
    

    totalPrefix = irr_records.flatMap(getTotalPrefix)\
                        .distinct()\
                        .map(lambda row: ((row[0], row[1]), 1))\
                        .reduceByKey(add)\
                        .map(toCSV)\
                        .collect()

    prefixDict = {}
    for source, _type, cnt in totalPrefix:
        if source not in prefixDict:
            prefixDict[source] = {}
        prefixDict[source][_type] = cnt

    types = ['IRR', "IRRd4", 'IRR-ML']
    
    outfile = localPath + '{}-ALL-IRR-ALL-IRR-lgb.csv'.format(date)

    with open(outfile, 'w') as fout:
        fout.write("source, _type, activeCnt, totalCnt, totalPrefixCnt\n")
        for source in activeDict.keys():
            for _type in types:
                activeCnt = getValue(activeDict, source, _type)
                totalCnt = getValue(totalDict, source, _type)
                totalPrefixCnt = getValue(prefixDict, source, _type)
                fout.write('{},{},{},{},{}\n'.format(source, _type, activeCnt, totalCnt, totalPrefixCnt))

    sc.stop()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='summarize as relationship\n')
    parser.add_argument('--date', default='20230301')
    parser.add_argument('--bgp_dir', default='/user/mhkang/bgp/routeview-reduced/')

    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
    
    parser.add_argument('--irr_files', default=[
            '/user/mhkang/radb/sanitized-tsv-final/20230301-ALL-IRR-ALL-IRR-lgb.tsv',
            '/user/mhkang/radb/sanitized-tsv-final/20230301-ALL-IRR-ALL-IRR-lgb.tsv'
            ]
        )  
    parser.add_argument('--local_dir', default='/home/mhkang/bgp/active-today/')
    parser.parse_args()
    args = parser.parse_args()
    print(args)

    activeToday(args.date, args.bgp_dir, args.roa_dir, args.irr_files, args.local_dir)


