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


cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))

from caida import as2rel
from utils.utils import *
from utils.prefix import *
from utils.bitvector import *


def parseBGPReduced(line, ip_version='ipv4'):
    date, prefix, origin = line.split('|')
    prefix_addr, prefix_len = prefix.split('/')

    results = []
    results.append( ((prefix_addr, prefix_len), (date, origin)) )
    return results


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

def calcMetrics(dates):
    monWnds = [1,3,5,7,14,21,30,30*3, 30*6, 30*9, 365, 365*2,365*3, 365*4, 365*5, 365*6, 365*7, 365*8, 365*9, 365*10]

    prev = (testBit(dates, 0) > 0)
    
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

    for i in range(3650):

        curr = (testBit(dates, i) > 0)
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

def toBitVector(row, date):
    key, value = row

    prefix_addr, prefix_len = key
    bgps = value

    originDict = {}
    
    if bgps == None: 
        # print("BGPs is None")
        bgps = []

    for _date, origin, _total_cnt in sorted(list(bgps), key=lambda x: x[0]):
        diff = dateDiff(_date, date)
        if diff < 0 or diff >= 3650: continue
        if origin not in originDict:
            originDict[origin] = makeBitVector(7)
        
        setBit(originDict[origin], diff)

    results = []
    for origin, bitvector in originDict.items():
        key = (date, prefix_addr, prefix_len, origin)
        results.append( (key, bitvector) )

    return results

def toBitVectorResults(row):
    key, value = row
    date, prefix_addr, prefix_len, origin = key
    bitvector = value
    bitvector = bitvector2str(bitvector)
    result = [date, prefix_addr, prefix_len, origin, bitvector]
    return '\t'.join(list(map(str, result)))

def getFeatures(row, date):
    key, value = row

    date, prefix_addr, prefix_len, origin = key
    bitvector = value

    metrics = calcMetrics(bitvector)

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


def getFiles(path, extension='.tsv'):
    files = hdfs.ls(path)
    
    files = list(filter(lambda x: x.endswith(extension), files))
    return files

def getBGPfiles(path):
    subdirs = hdfs.ls(path)
    
    files = []
    for subdir in subdirs:
        files += hdfs.ls(subdir)
    
    return files

def extractBGPFeatures(hdfsRoot, localRoot):
    vrpPath = '{}/vrps/daily-tsv/'.format(hdfsRoot)
    relPath = '{}/caida/as-rel/data/'.format(localRoot)
    bgpReducedPath = '{}/bgp/routeview-reduced/'.format(hdfsRoot)

    radbPath = '{}/radb/daily-tsv-w-changed/'.format(hdfsRoot)
    irrsPath = '{}/irrs/daily-tsv-w-changed/'.format(hdfsRoot)

    radbFiles = getFiles(radbPath)
    irrsFiles = getFiles(irrsPath)
    vrpFiles = getFiles(vrpPath)
    bgpReducedFiles = getFiles(bgpReducedPath)

    targetdates = []
    # targetdates += ['20230301']
    # targetdates += ['20230228', '20230226', '20230224'] # 1,3,5 days
    # targetdates += ['20230222', '20230215', '20230208'] # 1,2,3 weeks
    # targetdates += ['20230201', '20230101', '20221201', '20221101'] # 1,2,3,4 months
    # targetdates += ['20220901', '20220601'] # 6, 9 months
    # targetdates += ['20220301'] # 1 year
    # targetdates += ['20220602'] # 6, 9 months
    targetdates += ['20220301'] # 1 year

    print(targetdates)

    print("extract BGP features: {} ~ {}".format(targetdates[0], targetdates[-1]))
    
    asRel = as2rel.AS2Rel(path=relPath)
    targetdatesDic = {}

    for date in targetdates:
        dbdate = asRel.getDBDates(date)
        appendDic(targetdatesDic, dbdate, date)

    asRelDic = asRel.db
    items = sorted(targetdatesDic.items(), key=lambda x: x[0])

    for dbdate, targetdates in targetdatesDic.items():
        targetdates = sorted(targetdates)
        conf = SparkConf().setAppName(
                    "extract BGP features {}-{}".format(targetdates[0], targetdates[-1])
                    ).set(
                        "spark.kryoserializer.buffer.max", "512m"
                    ).set(
                        "spark.kryoserializer.buffer", "1m"
                    )
    
        sc = SparkContext(conf=conf)

        spark = SparkSession(sc)

        sc.setLogLevel("WARN")

        sc.addPyFile(root_dir + "/caida.zip")
        sc.addPyFile(root_dir + "/utils.zip")

        relDict = sc.broadcast(asRelDic[dbdate])

        mindate = targetdates[0]
        maxdate = targetdates[-1]

        bgpReducedFiles = list(filter(lambda x: 
                dateDiff(getDate(x), mindate) <= 3650
                and
                dateDiff(getDate(x), maxdate) >= 0
            , bgpReducedFiles)
        )
        # bgpReducedFiles = sorted(bgpReducedFiles, reverse=True)[:10]
        
        bgpFeatures = sc.textFile(','.join(bgpReducedFiles))\
                        .flatMap(parseBGPReduced)

        # print("bgpFeatures: {}".format(bgpFeatures.count()))
        # print("bgpFeatures: {}".format(bgpFeatures.top(1)))

        for date in targetdates:
            print("date: {}".format(date))
            start = t.time()
            currRadbFiles = list(filter(lambda x: getDate(x) == date, radbFiles))
            currIrrsFiles = list(filter(lambda x: getDate(x) == date, irrsFiles))
            currVrpFiles = list(filter(lambda x: getDate(x) == date, vrpFiles))\

            irrsRecords = sc.textFile(','.join(currIrrsFiles))\
                            .flatMap(parseIRR)

            radbRecords = sc.textFile(','.join(currRadbFiles))\
                            .flatMap(parseIRR)
            
            vrpDict = sc.textFile(','.join(currVrpFiles))\
                            .flatMap(parseVRP)\
                            .groupByKey()\
                            .map(lambda row: (row[0], makeBinaryPrefixDict(row[1])) )\
                            .collectAsMap()

            vrpDict = sc.broadcast(vrpDict)

            irrsRecords = irrsRecords.flatMap(lambda row: getRelFunc(row, vrpDict, relDict))
            radbRecords = radbRecords.flatMap(lambda row: getRelFunc(row, vrpDict, relDict))

            bitvectorRecords = bgpFeatures.groupByKey()\
                            .flatMap(lambda row: toBitVector(row, date))

            # print("bitvectorRecords: {}".format(bitvectorRecords.count()))
            # print("bitvectorRecords: {}".format(bitvectorRecords.top(1)))
            featureRecords = bitvectorRecords.flatMap(lambda row: getFeatures(row, date))
            # print("featureRecords: {}".format(featureRecords.count()))
            # print("featureRecords: {}".format(featureRecords.top(1)))
                            
            bitvectorResults = bitvectorRecords.map(toBitVectorResults)

            featureResults = featureRecords.map(toFeatureResult)

            irrsResults = irrsRecords.leftOuterJoin(featureRecords)\
                                .map(toIRRFeatureResult)

            radbResults = radbRecords.leftOuterJoin(featureRecords)\
                                .map(toIRRFeatureResult)

            saveBitvector(date, bitvectorResults, hdfsRoot, localRoot)

            saveFeatures(date, featureResults, hdfsRoot, localRoot)

            saveIRRFeatures(date, irrsResults, hdfsRoot, localRoot, 'irrs')

            saveIRRFeatures(date, radbResults, hdfsRoot, localRoot, 'radb')
            end = t.time()
            elapsed_time = end - start
            s = elapsed_time % 60
            
            elapsed_time = elapsed_time // 60

            m = elapsed_time % 60
            h = elapsed_time // 60
            print('elapsed_time: {}:{}:{}'.format(h, m, s))

        sc.stop()

def main():
    parser = argparse.ArgumentParser(description='extract BGP features\n')
    parser.add_argument('--localRoot', default='/home/mhkang')
    parser.add_argument('--hdfsRoot', default='/user/mhkang')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    extractBGPFeatures(args.hdfsRoot, args.localRoot)

if __name__ == '__main__':

    main()
