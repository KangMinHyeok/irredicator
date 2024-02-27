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


class AS2Rel:
    def __init__(self, path=None):
        if path is not None:
            path = path if path.endswith('/') else path + '/'

            self.path     = path #"/net/data-backedup/rpki/caida-as-rel/data"
            self.export_path  = path + 'caida-as-rel.json' #"/net/data-backedup/rpki/caida-as-rel/data/caida-as-rel.json"
            # self.export_ever_path  = path + 'caida-as-rel-ever.json' # "/net/data-backedup/rpki/caida-as-rel/data/caida-as-rel-ever.json"

            self.date = []
            self.db   = None

            if not self.hasDB(): self.saveDB()
            self.loadDate()
            self.loadDB()

            self.ddos_protection = [    20940, 16625, 32787,        # akamai
                                        209, 3561,                  # CenturyLink
                                        13335,                      # CloudFlare
                                        19324,                      # DOSarrest
                                        55002,                      # F5 Networks
                                        19551,                      # Incapsula
                                        3549, 3356, 11213, 10753,   # Level 3
                                        7786, 12008, 19905,         # Neustar
                                        26415, 30060,               # Verisign
                                        200020                      # Nawas
                                    ]

    def copy(self, asRel, timestamp):
        self.path = asRel.path
        self.export_path = asRel.export_path
        # self.export_ever_path = asRel.export_ever_path
        self.date = asRel.date
        
        self.ddos_protection = asRel.ddos_protection

        diff = list(map(lambda v: abs( (datetime.strptime(v, "%Y%m%d") - datetime.strptime(timestamp, "%Y%m%d")).days), self.date))
        dbdate = self.date[diff.index(min(diff))]

        self.db = {}
        self.db[dbdate] = asRel.db[dbdate]

    def loadDate(self): 
        for fname in os.listdir(self.path):
            if not fname.endswith("as-rel.txt"): continue
            date = fname.split(".")[0]
            self.date.append(date)

    def hasDB(self):
        files = os.listdir(self.path)
        return 'caida-as-rel.json' in files

    def loadDB(self):
        
        self.db  = json.load(open(self.export_path))
        print('caida as2rel loaded done')

    def getASRelDic(self, timestamp):
        diff = list(map(lambda v: abs( (datetime.strptime(v, "%Y%m%d") - datetime.strptime(timestamp, "%Y%m%d")).days), self.date))
        dbdate = self.date[diff.index(min(diff))]

        asRelDic = self.db[dbdate]
        return asRelDic
    
    def getRelationship(self, date, as1, as2, isp1=None, isp2=None):
        if(as1 == as2): return "self"
        
        diff = list(map(lambda v: abs( (datetime.strptime(v, "%Y%m%d") - datetime.strptime(date, "%Y%m%d")).days), self.date))
        dbdate = self.date[diff.index(min(diff))]

        key = str(as1) + "+" + str(as2)
        if isp1 is not None and isp2 is not None and isp1 == isp2:
            return 'sameISP'
        rel = self.db[dbdate].get(key, "none")
        if rel == 'none':
            if self.isDDoS(as1) or self.isDDoS(as2): return 'ddos'
        return rel

    def isDDoS(self, asn):
        return (int(asn) in self.ddos_protection)

    def saveDB(self):
        db = {}
        db_rel = set() 

        for fname in os.listdir(self.path):
            if not fname.endswith("as-rel.txt"): continue
            date = fname.split(".")[0]

            if date not in db:
                db[date] = {}

            for line in open(os.path.join(self.path, fname)):
                if("#" in line):
                    continue

                as1, as2, rel = line.rstrip().split("|")

                if (rel == "-1"): #provider, customer

                    db[date][ as1 + "+" + as2 ] = "provider"  # as1 is the provider
                    db[date][ as2 + "+" + as1 ] = "customer" # as2 is the customer

                else: # peer
                    db[date][ as1 + "+" + as2 ] = "peer"  
                    db[date][ as2 + "+" + as1 ] = "peer" 
                
                db_rel.add(as1 + "+" + as2)
                db_rel.add(as2 + "+" + as1)
        
        json.dump( db, open(self.export_path, "w"), indent = 4)

def isDDoS(asn):
    ddos_protection = [    20940, 16625, 32787,        # akamai
                            209, 3561,                  # CenturyLink
                            13335,                      # CloudFlare
                            19324,                      # DOSarrest
                            55002,                      # F5 Networks
                            19551,                      # Incapsula
                            3549, 3356, 11213, 10753,   # Level 3
                            7786, 12008, 19905,         # Neustar
                            26415, 30060,               # Verisign
                            200020                      # Nawas
                        ]
    return (int(asn) in ddos_protection)

def smInsert(tree, binary):
    if( len(binary) == 0):
        return
    subtree=tree
    for i in range(len(binary)):
        if not (binary[i] in subtree):
            subtree[binary[i]]={}
        subtree=subtree[binary[i]]
        if (i==len(binary)-1):
            subtree['l']=True

def smGetAllKeys(tree, parent):
    result=[]
    subtree=tree
    subp=""
    for t in parent:
        if not(t in subtree):
            break
        subtree=subtree[t]
        subp=subp+t
        if 'l' in subtree:
            result.append(subp)
    return result
    
def mergeAndSort(path, extension='.csv', label=None):
    try: os.mkdirs(path)
    except: pass

    cmdMerge = """find %s -name "*" -print0 | xargs -0 cat >> /tmp/tmp-spark""" % path
    os.system(cmdMerge)

    cmdSort  = "sort -k1,1 /tmp/tmp-spark > {0}".format(path + extension)
    os.system(cmdSort)
    
    if(label is not None):
        cmd = "sed -i '1s/^/%s\\n/' %s" % (label, path +  extension)
        os.system(cmd)

    cmdErase = "rm /tmp/tmp-spark"
    os.system(cmdErase)

def merge(path, extension='.csv'):
    cmdErase = "rm {}".format(path + extension)
    os.system(cmdErase)

    cmdMerge = 'find {} -name "*" -print0 | xargs -0 cat >> {}'.format(path, path + extension)
    os.system(cmdMerge)


def fetchAndMerge(savePath, localPath, extension, needSort):

    try: shutil.rmtree(localPath)
    except: pass

    try: os.makedirs(localPath)
    except: pass        

    print("fetch {}".format(savePath))
    os.system("hdfs dfs -get {} {}".format(savePath, localPath))
    
    # hdfs.get(savePath, localPath)
    print("merge {}".format(localPath))
    if needSort:
        mergeAndSort(localPath, extension=extension)
    else:
        merge(localPath, extension=extension)
    print("done {}".format(localPath))
    try: shutil.rmtree(localPath)
    except: pass
    

writeProcesses = []
def saveResult(result, savePath, localPath, extension='.csv', needSort=True, useMultiprocess=False):
    if result != None:
        try: hdfs.rmr(savePath)
        except: pass
        print("save {}".format(savePath))
        result.saveAsTextFile(savePath)

        if not useMultiprocess:
            fetchAndMerge(savePath, localPath, extension, needSort)
        else:
            p = Process(target=fetchAndMerge, args=(savePath,localPath, extension, needSort))
            p.start()
            writeProcesses.append(p)
        return True
    else:
        return False

def parseRel(line):
    tokens = line.replace('\n', '').split('\t')
    if len(tokens) == 6:
        date, source, prefix, sumRel, vrpRecords, irrRecords = tokens
    elif len(tokens) == 5:
        date, prefix, sumRel, vrpRecords, irrRecords = tokens
        source = "RADB"
    else:
        return []

    irrRecords = irrRecords.split('|')
    if len(irrRecords) == 0: return []
    results = []
    
    if sumRel == 'partial': 
        results.append( ((date, 'ALL-IRR', 'total'), 1) )
        results.append( ((date, source, 'total'), 1) )

    else:
        results.append( ((date, 'ALL-IRR', 'total'), 1) )
        results.append( ((date, source, 'total'), 1) )
        results.append( ((date, 'ALL-IRR', 'overalp'), 1) )
        results.append( ((date, source, 'overalp'), 1) )
        discrepancy = 'same' if sumRel == 'same' else 'discrepant'
        results.append( ((date, 'ALL-IRR', discrepancy), 1) )
        results.append( ((date, source, discrepancy), 1) )
    
    return results

def parseRelObject(line):
    tokens = line.replace('\n', '').split('\t')
    if len(tokens) == 6:
        date, source, prefix, sumRel, vrpRecords, irrRecords = tokens
    elif len(tokens) == 5:
        date, prefix, sumRel, vrpRecords, irrRecords = tokens
        source = "RADB"
    else:
        return []

    irrRecords = irrRecords.split('|')
    if sumRel == 'partial': return []
    discrepancy = 'same' if sumRel == 'same' else 'discrepant'
    #date,total,consistent,discrepant,sameISP,C/P,DDoS,None
    results = []
    for irrRecord in irrRecords:
        prefix, origin, isp, cc, rir = irrRecords.split(',')
        results.append( ((date, 'ALL-IRR', 'total'), 1) )
        results.append( ((date, source, 'total'), 1) )
        
        discrepancy = 'same' if sumRel == 'same' else 'discrepant'
        results.append( ((date, 'ALL-IRR', discrepancy), 1) )
        results.append( ((date, source, discrepancy), 1) )

    
    return results

def getDate(filename):
    date = filename
    if '/' in date: date = date.split('/')[-1]
    if '.' in date: date = date.split('.')[0]
    if '-' in date: date = date.split('-')[0]
    return date

def getDates(files):
    files = map(lambda x: x.split('/')[-1], files)
    files = filter(lambda x: x.endswith('.csv') or x.endswith('.tsv') or x.endswith('.json'), files)
    dates = list(map(lambda x: getDate(x), files))
    return sorted(dates)

def getFiles(path, path2=None, extension='.tsv'):
    files = hdfs.ls(path)
    if path2 is not None:
        files += hdfs.ls(path2)
    files = list(filter(lambda x: x.endswith(extension), files))
    return files

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
    # , 'sameISP', 'customer-provider', 'ddos', 'none'
    rels = ['overalp', 'same', 'discrepant', 'total']
    for source in dic.keys():
        record = [date, source]
        record += [dic[source].get(rel, 0) for rel in rels]
        # record += [objectDic[source].get(rel, 0) for rel in rels]
        records.append(','.join(list(map(str, record))))

    return records

def countEntry(relPath, relPath2, savePath, localPath, irrPath, irrPath2, patchPath=None):
    try: hdfs.mkdir(savePath)
    except: pass

    try: os.makedirs(localPath)
    except: pass

    extradates = []
    relFiles = getFiles(relPath, relPath2, extension='.tsv')
    irrFiles = getFiles(irrPath, irrPath2, extension='.tsv')

    start, end = '20160801', '20230301'
    relFiles =  list(filter(lambda x: start <= getDate(x) and getDate(x) <= end, relFiles))
    

    irrFiles =  list(filter(lambda x: start <= getDate(x) and getDate(x) <= end, irrFiles))

    if len(relFiles) == 0:
        print("up to date")
        exit()

    patch = patchPath != None
    if patch:
        dates = getDates(os.listdir(patchPath))
        relFiles = list(filter(lambda x:  getDate(x) in dates, relFiles))

    print("count discrepancy")

    conf = SparkConf(
            ).setAppName(
                "count discrepancy"
            ).set(
                "spark.kryoserializer.buffer.max", "512m"
            ).set(
                "spark.kryoserializer.buffer", "1m"
            )

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)

    sc.setLogLevel("WARN")
    
    prefixes = sc.textFile(','.join(relFiles))\
                    .flatMap(parseRel)\
                    .reduceByKey(addCount)\
                    .map(lambda row: (row[0][0], (row[0][1], row[0][2], row[1]) ) ) \
                    .groupByKey()\
                    .flatMap(toCSV)

    # objects = sc.textFile(','.join(relFiles))\
    #               .flatMap(parseRelObject)\
    #               .reduceByKey(addCount)\
    #               .map(lambda row: (row[0][0], (row[0][1], row[0][2], row[1]) ) ) 
                    # .groupByKey()\
                    # .flatMap(toCSV)

    # results = prefixes.cogroup(objects)\
    #               .flatMap(toCSV)

    results = prefixes

    filename = 'entry-ipv4-{}-with-total'.format(end)
    if patch: filename += '-patch'
    saveResult(results, savePath + filename, localPath + filename, extension='.csv')

    sc.stop()
    
    for p in writeProcesses:
        p.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='irr rpki as relationship\n')
    parser.add_argument('--relPath', default='/user/mhkang/radb/as-relationship-reduced/')
    parser.add_argument('--relPath2', default='/user/mhkang/irrs/as-relationship-reduced/')
    parser.add_argument('--irrPath', default='/user/mhkang/radb/daily-tsv-w-changed/')
    parser.add_argument('--irrPath2', default='/user/mhkang/irrs/daily-tsv-w-changed/')

    parser.add_argument('--patchPath', default='/net/data/vrps/patch-vrps/')

    parser.add_argument('--savePath', default='/user/mhkang/radb/discrepancy/entry/raw/')
    parser.add_argument('--localPath', default='/net/data/radb/discrepancy/entry/')
    
    parser.parse_args()
    args = parser.parse_args()
    print(args)
    patchPath = args.patchPath
    patchPath = None
    countEntry(args.relPath, args.relPath2, args.savePath, args.localPath, args.irrPath, args.irrPath2,
        patchPath=patchPath)


