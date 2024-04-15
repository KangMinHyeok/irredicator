import os
import sys
import time
import shutil
import operator
import argparse
import json
import pydoop.hdfs as hdfs
import numpy as np
import subprocess

# from ipaddress import IPv6Address
# from ipaddress import IPv4Address
from datetime import datetime
from datetime import date, timedelta
from pyspark.sql import SQLContext, Row, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import input_file_name
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
from multiprocessing import Process

# cwd = os.getcwd().split('/')
# sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from as_info.as2isp import AS2ISP
from utils.utils import write_result, append2dict, get_files, get_dates, get_date

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
    
        if len(record) < 2:
            raise Exception("len record = {}\n{}".format(len(record), record))
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

def toTSV(data):
    return "\t".join(str(d) for d in data)

def parseNRO(line):
    date, prefix_addr, prefix_len, rir, _, _, _ = line.split(",")

    return [ (date, (prefix_addr, prefix_len, rir)) ]

def getRIR(date, prefix_addr, prefix_len, nro_dict):
    if nro_dict == None: return None
    tree, record_set = nro_dict.get(date, ({}, {}))

    binary_prefix = ip2binary(prefix_addr, prefix_len)
    records = get_records(tree, record_set, binary_prefix)
    for prefix_addr, prefix_len, rir in records:
        if rir is not None:
            return rir

    return None

def parseROA(date, line, nroDict, asISPDict, extension=".csv"):
    if line.startswith("URI") or line.startswith("#"): return  [] # header 
    if extension == '.csv':

        uri, asn, prefix, maxlen, not_before, not_after = line.rstrip().split(",")
        try:
            prefix_addr, prefix_len = prefix.split('/')
        except Exception as e:
            raise Exception("{}: prefix = {}".format(e, prefix))
        origin = asn[2:]

        ip_version = "ipv4" if "." in prefix_addr else "ipv6"
        if ip_version == "ipv4": ip_length = 32
        else: ip_length = 128

        num_ip = 1 << (ip_length - int(prefix_len))

        cc = ""

        tal = uri.split('/')[2].split('.')[1]
    elif extension == '.tsv':
        date, prefix_addr, prefix_len, maxlen, origin, num_ip, cc, tal = line.split("\t")
        ip_version = "ipv4" if "." in prefix_addr else "ipv6"
    else:
        return []
    if ':' in prefix_addr: return []
    if tal == 'ripe': tal = 'ripencc'
    rirs = ['ripencc', 'arin', 'afrinic', 'apnic', 'lacnic']

    isp, country = None, None
    if origin in asISPDict:
        isp, country = asISPDict[origin]
        if(isp == ""): isp = None
        if(country == ""): country = None   
    
    if extension == '.csv':
        if ip_version == 'ipv4' and tal not in rirs:
            tal = getRIR(date, prefix_addr, prefix_len, nroDict)

        return [(date, prefix_addr, prefix_len, maxlen, origin, num_ip, cc, tal, isp, country)] 
    else:
        binary_prefix = ip2binary(prefix_addr, prefix_len)
        results = []
        for l in range(8, int(prefix_len) + 1):
            results.append( ( (date, binary_prefix[:l]), (prefix_addr, prefix_len, maxlen, origin, num_ip, cc, tal, isp, country) ) )
        return results

def parseZiggyVRP(date, line, nroDict, asISPDict, RIR=None):
    if line.startswith('URI'): return []

    uri, asn, prefix, max_len, _, _ = line.split(',')
    
    origin = asn.replace('AS', '')
    prefix_addr, prefix_len = prefix.split('/')

    ip_version = "ipv4" if "." in prefix_addr else "ipv6"
    if ip_version == "ipv4": ip_length = 32
    else: ip_length = 128
    
    num_ip = 1 << (ip_length - int(prefix_len))
    nroDict = nroDict.value
    asISPDict = asISPDict.value

    if RIR != None:
        rir = RIR
    else:
        rir = uri.split('//')[1].split('.')[1]
        tals = ['ripencc', 'arin', 'afrinic', 'apnic', 'lacnic']
        if rir == 'ripe': rir = 'ripencc'
        if rir not in tals: 
            rir = getRIR(date, prefix_addr, prefix_len, nroDict)

    isp, country = None, None
    if origin in asISPDict:
        isp, country = asISPDict[origin]
        if(isp == ""): isp = None
        if(country == ""): country = None   
    
    return [(date, prefix_addr, prefix_len, max_len, origin, num_ip, country, rir, isp, country)]   

def ziggy_date(x):
    return ''.join(x.split('/')[-1].split('.')[0].split('-')[1:])

def preprocessZiggyVRP(ziggyPath, nroPath, asISPPath, savePath, localPath):
    
    try: hdfs.mkdir(savePath)
    except: pass
    try: os.mkdir(localPath)
    except: pass
    
    ziggyFiles = get_files(ziggyPath, extension='.csv')
    nroFiles = get_files(nroPath, extension='.csv')

    currFiles = os.listdir(localPath)
    currdates = get_dates(currFiles)
    # currdates = list(map(lambda x: x.split('_')[0], get_dates(currFiles) ) )
    newdates = list(map(ziggy_date, ziggyFiles))
    targetdates = sorted(list(set(newdates) - set(currdates)))
    targetdates = list(filter(lambda x: x > '20230301', targetdates))
    print(targetdates)
    
    if len(targetdates) == 0: 
        print("up to date")
        exit()
    
    print("target dates: {} ~ {}".format(targetdates[0], targetdates[-1]))

    asISP = AS2ISP(asISPPath)

    batchSize = 30
    targetBatch = [targetdates[i:i + batchSize] for i in range(0, len(targetdates), batchSize )]

    for targetdates in targetBatch:
        appname = "Preprocessing Ziggy VRP {} - {}".format(targetdates[0], targetdates[-1])
        conf = SparkConf().setAppName(appname)
        print("process: {} ~ {}".format(targetdates[0], targetdates[-1]))
        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)
        
        sc.setLogLevel("WARN")
        
        for date in targetdates:
            currNroFiles = list(filter(lambda x: get_date(x) == date, nroFiles))
            currZiggyFiles = list(filter(lambda x: ziggy_date(x) == date, ziggyFiles))
            
            if len(currZiggyFiles) == 0:
                print("skip {}: len(currZiggyFiles) == 0".format(date))
                continue
            if len(currNroFiles) == 0:
                diffs = list(map(lambda x: abs(datetime.strptime(get_date(x), "%Y%m%d") - datetime.strptime(get_date(date), "%Y%m%d")), nroFiles))
                currNroFiles.append(nroFiles[diffs.index(min(diffs))])

            nroDict  = sc.textFile(','.join(currNroFiles))\
                            .flatMap(lambda line: parseNRO(line))\
                            .groupByKey()\
                            .map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
                            .collectAsMap()
            
            nroDict = sc.broadcast(nroDict)
            
            asISPDict = asISP.getASISPDict(date)

            asISPDict = sc.broadcast(asISPDict)
            
            records  = sc.textFile(','.join(currZiggyFiles))\
                        .flatMap(lambda line: parseZiggyVRP(date, line, nroDict, asISPDict))
            
            records = records.distinct()\
                            .map(lambda row: toTSV(row))
            
            currSavePath  = savePath + date 
            currLocalPath  = localPath + date

            write_result(records, currSavePath, currLocalPath, extension='.tsv')
        
        sc.stop()

def main():
    parser = argparse.ArgumentParser(description='preprocess vrp\n')
    parser.add_argument('--asISPPath', default='/home/mhkang/caida/as-isp/data/')
    parser.add_argument('--nroPath', type=str, default='/user/mhkang/nrostats/ipv4-w-date/')
    parser.add_argument('--ziggyPath', type=str, default='/user/mhkang/vrps/ziggy-vrps/')

    parser.add_argument('--savePath', type=str, default='/user/mhkang/vrps/daily-tsv/raw/')
    parser.add_argument('--localPath', type=str, default='/net/data/vrps/daily-tsv/')

    args = parser.parse_args()
    
    preprocessZiggyVRP(args.ziggyPath, args.nroPath, args.asISPPath, args.savePath, args.localPath)

if __name__ == '__main__':
    main()



