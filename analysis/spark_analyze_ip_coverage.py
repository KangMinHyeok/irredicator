import os
import sys
import shutil
import argparse
import traceback
import numpy as np
import json
from ipaddress import IPv4Address
from multiprocessing import Process
import subprocess
import pydoop.hdfs as hdfs

import time
from datetime import *
from ipaddress import *
from operator import add
from operator import itemgetter
from pyspark.sql import SQLContext, Row
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast

# cwd = os.getcwd().split('/')
# sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from utils.utils import write_result, get_date, get_files, make_dirs

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

def parseIRR(line, ip_version='ipv4'):
    date, rir, prefix, origin, isp, country, source, changed = line.replace('\n', '').split("\t")

    if ip_version == 'ipv4' and ':' in prefix: return []
    elif ip_version == 'ipv6' and '.' in prefix: return []

    if source == date: source = "RADB"
    source = source.upper()

    try: date2 = datetime.strptime(date, "%Y%m%d")
    except: return []

    prefix_addr, prefix_len = prefix.split('/')
    date, prefix_len = int(date), int(prefix_len)

    results = []
    results.append( ((date, "total", source), (prefix_addr, prefix_len)) )
    results.append( ((date, rir, source), (prefix_addr, prefix_len)) )
    results.append( ((date, "total", "ALL-IRR"), (prefix_addr, prefix_len)) )
    results.append( ((date, rir, "ALL-IRR"), (prefix_addr, prefix_len)) )

    return results


def parseVRP(line, ip_version='ipv4'):
    if line.startswith('#'): return []
    tokens = line.split('\t')
    date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir = tokens[:8]
    
    if ip_version == 'ipv4' and ':' in prefix_addr: return []
    elif ip_version == 'ipv6' and '.' in prefix_addr: return []

    date, prefix_len = int(date), int(prefix_len)

    rirs = ['apnic', 'ripencc', 'afrinic', 'lacnic', 'arin', 'ripe']
    if rir == 'ripe': rir = 'ripencc'
    if rir not in rirs: rir = "None"
    
    results = []
    results.append( ((date, "total", "VRP"), (prefix_addr, prefix_len)) )
    results.append( ((date, rir, "VRP"), (prefix_addr, prefix_len)) )

    return results


def parseNRO(line, ip_version='ipv4'):
    date, prefix_addr, prefix_len, rir, added_date, cc, status = line.split(',')
    
    if ip_version == 'ipv4' and ':' in prefix_addr: return []
    elif ip_version == 'ipv6' and '.' in prefix_addr: return []

    date, prefix_len = int(date), int(prefix_len)
    results = []
    results.append( ((date, 'total', 'NRO'), (prefix_addr, prefix_len)) )
    results.append( ((date, rir, 'NRO'), (prefix_addr, prefix_len)) )

    return results

def collapse_and_count(listPrefixes, ip_version='ipv4'):
    s = []
    for prefix_addr, prefix_len in listPrefixes:
        try:
            if ip_version == "ipv4":
                binary_prefix = ip2binary(prefix_addr, prefix_len)
                if len(binary_prefix) != 32:
                    binary_prefix = binary_prefix + ("0"*(32-len(binary_prefix)))
                    val = int(binary_prefix, 2)
                    prefix_addr = str(IPv4Address(val))
                prefix = IPv4Network( prefix_addr + "/" + str(prefix_len))
            else:
                prefix = IPv6Network("{}/{}".format(prefix_addr, prefix_len))
            s.append(prefix)
        except:
            continue
    
    return sum(map(lambda v: v.num_addresses, collapse_addresses(s)))

def count_IP(row, ip_version='ipv4'):
    key, value = row
    
    date, rir, source = key
    return (date, (rir, source, collapse_and_count(value, ip_version)))
    

def get_result(row):
    key, value = row
    currIP, totalIP = value
    date = key

    dic = {}
    for rir, source, cnt in currIP:
        if source not in dic: dic[source] = {}
        dic[source][rir] = cnt

    totalDic = {}
    if totalIP is not None:
        for rir, source, cnt in totalIP:
            totalDic[rir] = cnt

    rirs = ['total', 'ripencc', 'apnic', 'arin', 'afrinic', 'lacnic', 'None']

    results = []
    for source in dic:
        currResults = [date, source]
        for rir in rirs:
            cnt = dic[source].get(rir, 0)
            totalCnt = totalDic.get(rir, 0)
            currResults.append(cnt)
            currResults.append(totalCnt)

        results.append(','.join(list(map(lambda x: str(x), currResults))))
    return results


def IP_coverage(ip_version, nro_dir, roa_dir, irr_dir, hdfs_dir, local_dir):

    make_dirs(hdfs_dir, local_dir)

    hdfs_dir = hdfs_dir + 'raw/'
    
    make_dirs(hdfs_dir, local_dir)

    conf = SparkConf().setAppName("IP Coverage "
                ).set(
                    "spark.kryoserializer.buffer.max", "512m"
                ).set(
                    "spark.kryoserializer.buffer", "1m"
                )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    files = list(filter(lambda x: x.startswith('ip-coverage'), os.listdir(local_dir)))
    start = max(list(map(lambda x: x.split('.')[0].split('-')[-1], files)))

    nro_files = get_files(nro_dir, extension='.csv')
    irr_files = get_files(irr_dir, extension='.tsv')
    roa_files = get_files(roa_dir, extension='.tsv')
    
    nro_dates = list(map(get_date, nro_files))
    irr_dates = list(map(get_date, irr_files))
    roa_dates = list(map(get_date, roa_files))
    end = min([max(nro_dates), max(irr_dates), max(roa_dates)])
    
    if end <= start:
        print("no new data available")
        print("end date of previpus analysis: {}".format(start))
        print("latest dates of nro, irr, and roa: {}, {}, and {}".format(max(nro_dates), max(irr_dates), max(roa_dates)))
        exit()

    nro_files = list(filter(lambda x: start <= get_date(x) <= end, nro_files))
    irr_files = list(filter(lambda x: start <= get_date(x) <= end, irr_files))
    roa_files = list(filter(lambda x: start <= get_date(x) <= end, roa_files))

    print("target dates: {} ~ {}".format(start, end))
    
    total_IPs = sc.textFile(','.join(nro_files))\
                    .flatMap(lambda line: parseNRO(line, ip_version))\
                    .distinct()\
                    .groupByKey()\
                    .map(lambda row: count_IP(row, ip_version))\
                    .groupByKey()
    
    roa_IPs  = sc.textFile(','.join(roa_files))\
                    .flatMap(lambda line: parseVRP(line, ip_version))\
                    .distinct()\
                    .groupByKey()\
                    .map(lambda row: count_IP(row, ip_version))\
                    .groupByKey()

    irr_IPs  = sc.textFile(','.join(irr_files))\
                    .flatMap(lambda line: parseIRR(line, ip_version))\
                    .distinct()\
                    .groupByKey()\
                    .map(lambda row: count_IP(row, ip_version))\
                    .groupByKey()

    IPs = roa_IPs.union(irr_IPs)

    results =   IPs.leftOuterJoin(total_IPs)\
                    .flatMap(get_result)

    filename = "ip-coverage-ipv4-{}".format(end)

    write_result(results, hdfs_dir + filename, local_dir + filename, extension='.csv')
    sc.stop()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ip percent\n')
    parser.add_argument('--ip_version', default='ipv4')
    parser.add_argument('--nro_dir', default='/user/mhkang/nrostats/ipv4-w-date/')

    parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
    
    parser.add_argument('--irr_dir', nargs='+', default=['/user/mhkang/irrs/daily-tsv/'])
    
    parser.add_argument('--hdfs_dir', default='/user/mhkang/rpki-irr/outputs/analysis/')
    parser.add_argument('--local_dir', default='/home/mhkang/rpki-irr/outputs/analysis/')

    args = parser.parse_args()
    print(args)
    
    IP_coverage(args.ip_version, args.nro_dir, args.roa_dir, args.irr_dir, args.hdfs_dir, args.local_dir)




