import os
import sys
import time as t
import json
import calendar
import random
import shutil
import argparse
import numpy as np
import pydoop.hdfs as hdfs

from datetime import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from utils.utils import write_result, ip2binary, get_date, get_dates, get_files, make_dirs

def make_bit_vector(bit_size):
    fill = 0
    num_records = bit_size >> 5                   # number of 8 bit integers
    if (bit_size & 31):                      # if bitSize != (32 * n) add
        num_records += 1                        #    a record for stragglers

    bitArray = [0] * num_records
    return bitArray

def shift_bit_vector(bitvector, delay, bit_size=32, bitval=0):
    new_vector = make_bit_vector(bit_size)
    for i in range(bit_size):
        if bitval == 1 and i < delay: set_bit(new_vector, i)
        if i + delay > bit_size: break
        if testBit(bitvector, i): set_bit(new_vector, i + delay)
    return new_vector


# testBit() returns a nonzero result, 2**offset, if the bit at 'bit_num' is set to 1.
def test_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = 1 << offset
    return(bitvector[record] & mask)

# setBit() returns an integer with the bit at 'bit_num' set to 1.
def set_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = 1 << offset
    bitvector[record] |= mask
    return(bitvector[record])

# clearBit() returns an integer with the bit at 'bit_num' cleared.
def clear_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = ~(1 << offset)
    bitvector[record] &= mask
    return(bitvector[record])

# toggleBit() returns an integer with the bit at 'bit_num' inverted, 0 -> 1 and 1 -> 0.
def toggle_bit(bitvector, bit_num):
    record = bit_num >> 5
    offset = bit_num & 31
    mask = 1 << offset
    bitvector[record] ^= mask
    return(bitvector[record])

def print_bit(bitvector):
    for record in bitvector:
        for offset in range(32):
            mask = 1 << offset
            if record & mask:
                sys.stdout.write('1')
            else:
                sys.stdout.write('0')
    sys.stdout.write('\n')

def bitvector2str(bitvector, size=32):
    bstring = ''
    for record in bitvector:
        for offset in range(32):
            mask = 1 << offset
            if record & mask:
                bstring = '1' + bstring
            else:
                bstring = '0' + bstring

    return bstring[32-size:]

def tobitvector(bstring):
    bitvector = []

    bitvectors = [bstring[i:i + 32] for i in range(0, len(bstring), 32)]
    return list(map(lambda x: int(x, 2), bitvectors))


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


def parseBitVector(line, ip_version='ipv4'):
    date, prefix_addr, prefix_len, origin, bitvector = line.split('\t')
    
    results = []
    results.append( ((prefix_addr, prefix_len, origin), (date, bitvector)) )
    return results


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


def getRelFunc(row, vrpDict, relDict):
    key, value = row
    date, prefix_addr, prefix_len = key
    
    prefix_len = int(prefix_len)
    vrpDict = vrpDict.value
    relDict = relDict.value
    origin, isp, rir, source = value
    binary_prefix = ip2binary(prefix_addr, prefix_len)

    recordSets, smtree_v4, smtree_v6 = vrpDict.get(date, ({}, {}, {}))

    entries = getCovered(binary_prefix, smtree_v4, smtree_v6)
    new_key = (str(date), str(prefix_addr), str(prefix_len), str(origin))

    results = []
    if len(entries) == 0:
        results.append((new_key, (isp, rir, 'unknown', 'not-covered', source)))
    else:
        validation, rels = 'invalid', []

        for entry in entries:
            for _prefix_addr, _prefix_len, _max_len, _origin, _rir, _isp in recordSets[entry]:
                sameOrigin = (_origin == origin)
                matched = (_prefix_len <= prefix_len <= _max_len)
                if matched and sameOrigin:  
                    validation = 'valid'
                    rels.append('same')
                    break
                
                rels.append(getRelationship(relDict, origin, _origin, isp, _isp))
        
        results.append((new_key, (isp, rir, validation, sumRels(rels), source) ))
    return results

def calcMetrics(bitvector, size):
    monWnds = [1,3,5,7,14,21,30,30*3, 30*6, 30*9, 365, 365*2,365*3, 365*4, 365*5, 365*6, 365*7, 365*8, 365*9, 365*10]

    monWnds = list(filter(lambda x: x <= size, monWnds))

    prev = (test_bit(bitvector, 0) > 0)
    
    monWnds = sorted(monWnds, reverse=True)
    windows = {}
    windows['ups'] = {}
    windows['downs'] = {}
    windows['onlines'] = {}
    windows['offlines'] = {}
    windows['updays'] = {}

    for monWnd in monWnds:
        windows['ups'][monWnd] = 0
        windows['downs'][monWnd] = 0
        windows['onlines'][monWnd] = []
        windows['offlines'][monWnd] = []
        windows['updays'][monWnd] = []

    offline, online = 0, 0 

    for i in range(365):

        curr = (test_bit(bitvector, i) > 0)
        if curr:
            for monWnd in monWnds:
                if i < monWnd:
                    windows['updays'][monWnd].append(i)

        if curr: online += 1
        else: offline += 1

       
        addOnline = (not curr) and (online > 0)
        addOffline = curr and (offline > 0)

        drop = 1 if curr != prev and (not prev) else 0
        up = 1 if curr != prev and prev else 0
        

        for monWnd in monWnds:
            if i < monWnd-1:
                windows['downs'][monWnd] += drop
                windows['ups'][monWnd] += up
                if addOnline: windows['onlines'][monWnd].append(online)
                if addOffline: windows['offlines'][monWnd].append(offline)
            elif i == monWnd-1:
                windows['downs'][monWnd] += drop
                windows['ups'][monWnd] += up
                if online > 0: windows['onlines'][monWnd].append(online)
                if offline > 0: windows['offlines'][monWnd].append(offline)
            else:
                break
        
        if curr: offline = 0
        else: online = 0

        prev = curr

    uptimes, lifespans, relUptimes = [], [], []
    ups, downs = [], []
    minonlines, maxonlines, meanonlines, stdonlines = [], [], [], []
    minofflines, maxofflines, meanofflines, stdofflines = [], [], [], []
    monWnds = sorted(monWnds)
    for monWnd in monWnds:
        updays = windows['updays'][monWnd]
        uptime = len(updays)
        lifespan = uptime if uptime == 0 or uptime == 1 else max(updays) - min(updays) + 1
        uptimes.append(float(uptime) / float(monWnd))
        lifespans.append(float(lifespan) / float(monWnd))
        relUptimes.append(float(len(updays)) / float(lifespan) if lifespan != 0 else 0.0)

        if len(windows['onlines'][monWnd]) == 0: windows['onlines'][monWnd].append(0)
        if len(windows['offlines'][monWnd]) == 0: windows['offlines'][monWnd].append(0)

        ups.append(windows['ups'][monWnd])
        downs.append(windows['downs'][monWnd])
        minonlines.append(np.min(list(windows['onlines'][monWnd])))
        maxonlines.append(np.max(list(windows['onlines'][monWnd])))
        meanonlines.append(np.mean(list(windows['onlines'][monWnd])))
        stdonlines.append(np.std(list(windows['onlines'][monWnd])))
        
        minofflines.append(np.min(list(windows['offlines'][monWnd])))
        maxofflines.append(np.max(list(windows['offlines'][monWnd])))
        meanofflines.append(np.mean(list(windows['offlines'][monWnd])))
        stdofflines.append(np.std(list(windows['offlines'][monWnd])))

    return uptimes, lifespans, relUptimes, ups, downs, minonlines, maxonlines, meanonlines, stdonlines, minofflines, maxofflines, meanofflines, stdofflines


def getStats(values):
    return [np.min(values), np.max(values), np.mean(values), np.std(values)]

def toBitVector(row, target):
    key, value = row

    prefix_addr, prefix_len, origin = key
    bitvectors = value
    
    if bitvectors == None: bitvectors = []
    
    bitvectorDict = {}
    for date, bitvector in bitvectors:
        bitvectorDict[date] = bitvector


    year, month = int(target[:4]), int(target[4:6])
    bitvector = ''
    for i in range(12):
        _, num_days = calendar.monthrange(year, month)

        curr_bitvector = bitvectorDict.get('{}{:02}01'.format(year, month), '0'*num_days)
        bitvector = curr_bitvector + bitvector
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    bitvector = tobitvector(bitvector)

    results = []
    new_key = (target, prefix_addr, prefix_len, origin)
    results.append((new_key, bitvector))
    
    return results

def toBitVectorResults(row):
    key, value = row
    date, prefix_addr, prefix_len, origin = key
    bitvector = value
    bitvector = bitvector2str(bitvector)
    result = [date, prefix_addr, prefix_len, origin, bitvector]
    return '\t'.join(list(map(str, result)))

def getFeatures(row):
    key, value = row

    date, prefix_addr, prefix_len, origin = key
    bitvector = value

    size = 3650
    metrics = calcMetrics(bitvector, 3650)

    feature = []
    stats = []
    for metric in metrics:
        feature += metric
        stats += getStats(metric)
    
    feature += stats
    feature = tuple(feature)

    results = []
    results.append( (key, feature) )
    return results

def toFeatureResult(row):
    key, value = row
    date, prefix_addr, prefix_len, origin = key
    feature = value

    result = [date, prefix_addr, prefix_len, origin] + list(feature)
    return '\t'.join(list(map(str, result)))

def toIRRFeatureResult(row):
    key, value = row
    date, prefix_addr, prefix_len, origin = key
    relRecord, bgpFeature = value

    bgpFeature = list(bgpFeature) if bgpFeature is not None else []

    isp, rir, validation, sumRel, source = relRecord

    result = [date, prefix_addr, prefix_len, origin] + [isp, rir, validation, sumRel, source] + list(bgpFeature)
    return '\t'.join(list(map(str, result)))

def makedirs(savePath, localPath):
    try: hdfs.mkdir(savePath)
    except: pass

    try: os.makedirs(localPath)
    except: pass

    savePath = savePath + 'raw/'
    try: hdfs.mkdir(savePath)
    except: pass

def saveBitvector(date, results, hdfsRoot, localRoot):
    savePath = '{}/bgp/bitvectors/'.format(hdfsRoot)
    localPath = '{}/bgp/bitvectors/'.format(localRoot)

    makedirs(savePath, localPath)

    print("[{}] write bitvectors".format(date))
    writeResult(results, savePath + date, localPath + date, extension='.tsv')

def saveFeatures(date, results, hdfsRoot, localRoot):
    savePath = '{}/bgp/features/'.format(hdfsRoot)
    localPath = '{}/bgp/features/'.format(localRoot)

    makedirs(savePath, localPath)

    print("[{}] write features".format(date))
    writeResult(results, savePath + date, localPath + date, extension='.tsv')

def saveIRRFeatures(date, results, hdfsRoot, localRoot, source):
    savePath = '{}/{}/features/'.format(hdfsRoot, source)
    localPath = '{}/{}/features/'.format(localRoot, source)

    makedirs(savePath, localPath)

    print("[{}] write IRR features".format(date))

    writeResult(results, savePath + date, localPath + date, extension='.tsv')

def is_invalid_targets(targets):

    prev_year, prev_month = None, None
    for target in targets:
        yearmonth = target
        year, month = int(yearmonth[:4]), int(yearmonth[4:])
            
        if prev_year is not None:
            diff_year = year - prev_year
            diff_month = month - prev_month
            if month == 1:
                if diff_year != 1 or diff_month != -11:
                    return True
            else:
                if diff_year != 0 or diff_month != 1:
                    return True
        
        prev_year = year
        prev_month = month

    return False


def extractBGPFeatures(bitvector_dir, hdfs_dir, local_dir):

    hdfs_dir = hdfs_dir + 'raw/'    
    
    make_dirs(hdfs_dir, local_dir)

    bitvector_files = get_files(bitvector_dir, extension='.tsv')

    if len(bitvector_files) < 120: 
        print("not enough files: len(bitvector_files) = {}".format(len(bitvector_files)))
        exit()
    
    today = str(datetime.today()).split(' ')[0]
    print(today)

    end_year, end_month, _ = list(map(int, str(today).split('-')))
    start_year, start_month = 2022, 4

    currfiles = sorted(os.listdir(local_dir))
    if len(currfiles) > 0:
        date = currfiles[-1].split('.')[0]
        start_year, start_month = list(map(int, [date[:4], date[4:]]))

        start_month += 1
        if start_month > 12:
            start_month = 1
            start_year += 1
    start = str(start_year) + str(start_month)
    end = str(end_year) + str(end_month)
    print("start", start_year, start_month)
    print("end", end_year, end_month)
    # exit()

    bitvector_files = sorted(bitvector_files)
    targets = list(map(lambda x: x.split('/')[-1].split('.')[0], bitvector_files))
    for i, target in enumerate(targets):
        if i < 120: continue
        if target < start: continue
        if target >= end: continue
        print("{} {}".format(i, target))
        conf = SparkConf().setAppName(
                    "extract BGP features {}".format(target)
                    ).set(
                        "spark.kryoserializer.buffer.max", "512m"
                    ).set(
                        "spark.kryoserializer.buffer", "1m"
                    )
    
        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")

        end = i
        start = end - 119
        
        if start < 0:
            print("invalid index: {} - {}".format(start, end))
            print("target: {}".format(target))
            print(bitvector_files)
            exit()

        curr_bitvector_files = bitvector_files[start:end]
        curr_targets = targets[start:end]
        if is_invalid_targets(curr_targets):
            print("invalid targets")
            print(curr_targets)
            exit()

        bitvectorRecords = sc.textFile(','.join(curr_bitvector_files))\
                        .flatMap(parseBitVector)\
                        .groupByKey()\
                        .flatMap(lambda row: toBitVector(row, target))

        featureRecords = bitvectorRecords.flatMap(lambda row: getFeatures(row))
        
        featureResults = featureRecords.map(toFeatureResult)

        write_result(featureResults, hdfs_dir + target, local_dir + target, extension='.tsv')

        sc.stop()


def main():
    parser = argparse.ArgumentParser(description='extract BGP features\n')

    parser.add_argument('--bitvector_dir', default='/user/mhkang/routeviews/bitvector/')

    parser.add_argument('--hdfs_dir', default='/user/mhkang/routeviews/feature/')
    parser.add_argument('--local_dir', default='/net/data/routeviews/feature/')


    parser.parse_args()
    args = parser.parse_args()
    print(args)

    extractBGPFeatures(args.bitvector_dir, args.hdfs_dir, args.local_dir)

if __name__ == '__main__':

    main()
