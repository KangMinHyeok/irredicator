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
from utils.utils import write_result, ip2binary, get_date, date_diff, get_files, origin2int, add2dict
from utils.loss import softmax
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

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

    origins = list(filter(lambda x: x != -1, map(origin2int, origins)))
    if len(origins) <= 0: return []

    records.append( ((date, prefix_addr, prefix_len), (origins, totalCnt, rir)) )

    return records

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.split("\t")
    
    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    try:
        if date == source: source = "RADB"
        date2 = datetime.strptime(date, "%Y%m%d")
        date = int(date)

        
        try: origin = origin2int(origin.replace("AS", ""))
        except:
            try: origin = origin2int(origin.split('#')[0])
            except: return []
        prefix_addr, prefix_len = prefix.split('/')
        prefix_len = int(prefix_len)
        source = source.upper()
    except: return []

    results = []
    results.append( (date, (prefix_addr, prefix_len, origin, source)) )
            
    return results

def parseIRR2(line, ip_version='ipv4'):
    try:
        tokens = line.split("\t")
        date, rir, prefix, origin, isp, sumRel, validation, source, _type, isTest, score0, score1 = tokens
    except:
        raise Exception("line: {}\ntokens: {}\nlen(tokens): {}".format(line, tokens, len(tokens)))

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    date = int(date)
    if '#' in origin: origin = origin.split('#')[0]
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
        y_pred = softmax(list(map(float, [score0, score1])))[1]

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
        origin = origin2int(origin)
        prefix_len = int(prefix_len)
        max_len = int(max_len) if max_len != None and max_len != "None" else prefix_len

    except Exception as e:  
        return []
    
    return [ (date, (prefix_addr, prefix_len, max_len, origin, 'VRP')) ]

def get_entries(date, binary_prefix, irr_dict, ip_version='ipv4'):
    if binary_prefix == None: return [], [], []
    bgp_length = len(binary_prefix)

    tree, record_set = irr_dict.get(date, ({}, {}))
    irr_records = get_records(tree, record_set, binary_prefix)
    irr_records = list(filter(lambda x: x[1] <= bgp_length, irr_records))
    irr_records += list(map(lambda x: tuple(list(x)[:-1] + ['ALL-IRRs']), irr_records))

    irrd4_records = list(filter(lambda x: x[3] != 0, irr_records))
    irrml_records = list(filter(lambda x: x[3] == 1 or x[4] > 0.0, irr_records))
    
    return set(irr_records), set(irrd4_records), set(irrml_records)


def withinLimit(start, end, limit):
    try:
        return (datetime.strptime(str(end), "%Y%m%d") - datetime.strptime(str(start), "%Y%m%d")).days <= limit
    except:
        return True

def initDict(dic, keys):
    for key in keys:
        dic[key] = set()
        dic[key+'FRESH'] = set()
        dic['JOB-'+key] = set()

def getCounts(bgpOrigin, origins, vrpCover, vrpValid):

    cover = len(origins) > 0
    numCover = 1 if cover else 0
    numBothCover = 1 if cover and vrpCover else 0

    valid = bgpOrigin != -1 and bgpOrigin in origins
    numValid = 1 if valid else 0
    numBothValid = 1 if valid and vrpValid else 0

    return [numCover, numValid, numBothCover, numBothValid]

def getCoverValid(origin, coverOriginSet, validOriginSet=None):
    cover = 1 if len(coverOriginSet) > 0 else 0
    origins = validOriginSet if validOriginSet != None else coverOriginSet
    valid = 1 if (origin != -1) and (origin in origins) else 0

    return cover, valid

def getResults(row, irr_dict, filterTooSpecific=True, ip_version='ipv4'):
    key, value  = row
    date, prefix_addr, prefix_len = key 
    BGPs = value
    
    if filterTooSpecific and prefix_len > 24: return []
    binary_prefix = ip2binary(prefix_addr, prefix_len)
    if binary_prefix == None: return []
    entryDict = {}


    irr_records, irrd4_records, irrml_records = get_entries(date, binary_prefix, irr_dict.value)
    
    
    irr_dict, irrd4_dict, irrml_dict = {}, {}, {}
    for prefix_addr, prefix_len, origin, y_true, y_pred, source in irr_records:
        add2dict(irr_dict, source, origin)

    for prefix_addr, prefix_len, origin, y_true, y_pred, source in irrd4_records:
        add2dict(irrd4_dict, source, origin)
    
    # thresholds = [0.00001, 0.0001, 0.001, 0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    t = 0.01
    for prefix_addr, prefix_len, origin, y_true, y_pred, source in irrml_records:
        # for t in thresholds:
        if t not in irrml_dict:
            ourDict[t] = {}
        if y_pred >= t:
            add2dict(ourDict[t], source, origin)

    sources = ['ALL-IRRs', 'AFRINIC', 'APNIC', 'ARIN', 'JPIRR', 'LACNIC', 'RADB', 'RIPE', ]

    results = []
    for BGPorigins, totalCnt, rir in BGPs:
        if len(BGPorigins) == 0: continue

        if len(BGPorigins) > 1: 
            bgpOrigin = -1
        else:
            bgpOrigin = BGPorigins[0]

        for source in sources:
            irrC, irrV = getCoverValid(bgpOrigin, irr_dict.get(source, set()))
            results.append( ((source, "IRR"), (1, irrC, irrV)))

            irrd4C, irrd4V = getCoverValid(bgpOrigin, irrd4_dict.get(source, set()))
            results.append( ((source, "IRRd4"), (1, irrd4C, irrd4V)))
            
            # for t in thresholds:
            results.append( ((source, 'IRR-ML'), (1, 0, 0)) )
            if t not in irrml_dict: continue
            irrmlC, irrmlV = getCoverValid(bgpOrigin,  ourDict[t].get(source, set()))
            results.append( ((source, 'IRR-ML'), (0, irrmlC, irrmlV)) )

    return results


def toCSV(row):
    key, cnts = row
    source, _type = key
    record = [source, _type] + list(cnts)
    return ','.join(list(map(str, record)))

def getRecord(sc, files, func):
    records = sc.textFile(','.join(files))\
                .flatMap(func)
    return records  

def addCnts(a, b):
    cnts = list(map(lambda x: x[0] + x[1], zip(a, b)))
    return cnts

def evalBGPCoverage(date, bgp_dir, roa_dir, irr_files, hdfs_dir, local_dir):

    bgp_files = get_files(bgp_dir, extension='.tsv')
    roa_files = get_files(roa_dir, extension='.tsv')


    bgp_files = sorted(list(filter(lambda x: get_date(x) == date, bgp_files)))
    roa_files = sorted(list(filter(lambda x: get_date(x) == date, roa_files)))
    currIrrFiles = irrFiles
    currIrrFiles2 = irrFiles2
    

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

    files = [currIrrFiles2, currRoaFiles, currBgpFiles]
    funcs = [parseIRR2, parseVRP, parseBGP]
    irrRecords, roaRecords, BGPRecords = list(map(lambda x: getRecord(sc, *x), zip(files, funcs)))


    irr_dict = irrRecords.groupByKey()\
                .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                .collectAsMap()
    
    irr_dict = sc.broadcast(irr_dict)

    results = BGPRecords.groupByKey()\
                        .flatMap(lambda row: getResults(row, irrDict, filterTooSpecific=True))

    results = results.reduceByKey(addCnts)

    results = results.map(toCSV)

    currSavePath = "{}{}-{}{}-exclude-w-rpki".format(savePath, targetDate, train_source, train_model)
    currLocalPath = "{}{}-{}{}-exclude-w-rpki".format(localPath, targetDate, train_source, train_model)

    write_result(results, currSavePath, currLocalPath, extension='.csv')

    sc.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='summarize as relationship\n')
    date, bgp_dir, roa_dir, irr_files, hdfs_dir, local_dir
    parser.add_argument('--date', default='20230301')
    parser.add_argument('--bgp_dir', default='/user/mhkang/bgp/routeview-reduced/')

    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
    
    parser.add_argument('--irr_files', default='/user/mhkang/radb/sanitized-tsv-final/20230301-ALL-IRR-ALL-IRR-lgb.tsv')  
    
    parser.add_argument('--hdfs_dir', default='/user/mhkang/bgp/covered-our/')
    parser.add_argument('--local_dir', default='/home/mhkang/bgp/covered-our/')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    evalBGPCoverage(args.date, args.bgp_dir, args.roa_dir, args.irr_files, args.hdfs_dir, args.local_dir)


