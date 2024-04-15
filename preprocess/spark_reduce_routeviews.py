import os
import sys
import bz2
import copy
import json
import pickle
import shutil
import codecs
import argparse
import itertools
import pandas
import threading

import time as t
from tqdm import tqdm
from datetime import *
from datetime import timedelta
# from datetime import date, timedelta
from mrtparse import *
from operator import add
from operator import itemgetter
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
from multiprocessing import Process, Pool
import subprocess
import pydoop.hdfs as hdfs

# parent_dir = os.path.dirname(os.path.realpath(__file__))
# root_dir = '/'.join(parent_dir.split('/')[:-1])
# sys.path.append(root_dir)

class BgpDump:
    __slots__ = [
        'vpName', 'verbose', 'output', 'ts_format', 'pkt_num', 'type', 'num', 'ts',
        'org_time', 'flag', 'peer_ip', 'peer_as', 'nlri', 'withdrawn',
        'as_path', 'origin', 'next_hop', 'local_pref', 'med', 'comm',
        'atomic_aggr', 'aggr', 'as4_path', 'as4_aggr', 'old_state', 'new_state',
    ]

    def __init__(self, vpName='', verbose=False, output=sys.stdout, ts_format='dump', pkt_num=False):
        # self.vpName = args.vpName
        # self.verbose = args.verbose
        # self.output = args.output
        # self.ts_format = args.ts_format
        # self.pkt_num = args.pkt_num
        self.vpName = vpName
        self.verbose = verbose
        self.output = output
        self.ts_format = ts_format
        self.pkt_num = pkt_num
        self.type = ''
        self.num = 0
        self.ts = 0
        self.org_time = 0
        self.flag = ''
        self.peer_ip = ''
        self.peer_as = 0
        self.nlri = []
        self.withdrawn = []
        self.as_path = []
        self.origin = ''
        self.next_hop = []
        self.local_pref = 0
        self.med = 0
        self.comm = ''
        self.atomic_aggr = 'NAG'
        self.aggr = ''
        self.as4_path = []
        self.as4_aggr = ''
        self.old_state = 0
        self.new_state = 0

    def get_routes(self):
        lines = []
        # for withdrawn in self.withdrawn:
        #     if self.type == 'BGP4MP':
        #         self.flag = 'W'
        #     lines.append(self.get_line(withdrawn, ''))
        for nlri in self.nlri:
            if self.type == 'BGP4MP':
                self.flag = 'A'
            for next_hop in self.next_hop:
                lines.append(self.get_line(nlri, next_hop))
        return lines

    def bgp4mp(self, m, count):
        self.type = 'BGP4MP'
        self.ts = datetime.utcfromtimestamp(m['timestamp'][0]).strftime('%Y%m%d')
        
        if (m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_AS4']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_LOCAL']
            or m['subtype'][0] == BGP4MP_ST['BGP4MP_MESSAGE_AS4_LOCAL']):
            if m['bgp_message']['type'][0] != BGP_MSG_T['UPDATE']:
                return
            for attr in m['bgp_message']['path_attributes']:
                self.bgp_attr(attr)
            
            lines = []
            for nlri in m['bgp_message']['nlri']:
                prefix = '{}/{}'.format(nlri['prefix'], nlri['prefix_length'])
                if self.type == 'BGP4MP' or self.type == 'BGP4MP_ET':
                    self.flag = 'A'
                
                if self.flag == 'A':
                    lines.append('{}|{}|{}'.format(self.ts, prefix, self.get_origin_as()))

            return lines

        return []
            # return  self.get_routes()

    def bgp_attr(self, attr):
        self.as4_path = []

        if attr['type'][0] == BGP_ATTR_T['ORIGIN']:
            self.origin = ORIGIN_T[attr['value']]
        
        
        elif attr['type'][0] == BGP_ATTR_T['AS_PATH']:
            self.as_path = []
            for seg in attr['value']:
                if seg['type'][0] == AS_PATH_SEG_T['AS_SET']:
                    self.as_path.append('{%s}' % ','.join(seg['value']))
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as_path.append('(' + seg['value'][0])
                    self.as_path += seg['value'][1:-1]
                    self.as_path.append(seg['value'][-1] + ')')
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as_path.append('[%s]' % ','.join(seg['value']))
                else:
                    self.as_path += seg['value']
        
        elif attr['type'][0] == BGP_ATTR_T['AS4_PATH']:
            for seg in attr['value']:
                if seg['type'][0] == AS_PATH_SEG_T['AS_SET']:
                    self.as4_path.append('{%s}' % ','.join(seg['value']))
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SEQUENCE']:
                    self.as4_path.append('(' + seg['value'][0])
                    self.as4_path += seg['value'][1:-1]
                    self.as4_path.append(seg['value'][-1] + ')')
                elif seg['type'][0] == AS_PATH_SEG_T['AS_CONFED_SET']:
                    self.as4_path.append('[%s]' % ','.join(seg['value']))
                else:
                    self.as4_path += seg['value']
        
    def merge_as_path(self):
        if len(self.as4_path):
            n = len(self.as_path) - len(self.as4_path)
            return ' '.join(self.as_path[:n] + self.as4_path)
        else:
            return ' '.join(self.as_path)

    def get_origin_as(self):
        origin = ''
        if len(self.as4_path) > 0:
            origin = self.as4_path[-1]
        elif len(self.as_path):
            origin = self.as_path[-1]

        return origin

    def merge_aggr(self):
        if len(self.as4_aggr):
            return self.as4_aggr
        else:
            return self.aggr


def mergeAndSort(path, extension='.csv', label=None):
    try: os.mkdirs(path)
    except: pass

    cmdMerge = 'find {} -name "*" -print0 | xargs -0 cat >> /tmp/tmp-spark'.format(path)
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
    

def saveResult(result, savePath, localPath, extension='.csv', needSort=False, useMultiprocess=False):
    if result != None:
        try: hdfs.rmr(savePath)
        except: pass
        print("save {}".format(savePath))
        result.saveAsTextFile(savePath)

        fetchAndMerge(savePath, localPath, extension, needSort)
        
        return True
    else:
        return False

def readBGPUpdate(arg):
   
    vpName, infile = arg
    b = BgpDump(vpName=vpName)

    print("start read BGP update: {}".format(infile))

    lines = set()
    count = 0

    num_lines = 0
    entries = None
    try:
        entries = Reader(infile)
    except:
        return lines

    # for m in tqdm(entries):
    for m in entries:
        if m.err:
            continue
        if m.data['type'][0] == MRT_T['BGP4MP']:
            for line in b.bgp4mp(m.data, count):
                lines.add(line)
        elif m.data['type'][0] == MRT_T['BGP4MP_ET']:
            for line in b.bgp4mp(m.data, count):
                lines.add(line)

        else:
            continue
    print("{} done".format(infile))
    return lines


def setPath(savePath, localPath):
    try: hdfs.mkdir(savePath)
    except: pass
    
    try: os.mkdir(localPath)
    except: pass

def parseBGPUpdate(bgpPath, savePath, localPath):

    setPath(savePath, localPath)
    
    savePath = savePath + '/raw/'
    setPath(savePath, localPath)
    
    sources = os.listdir(bgpPath)
    startDates = []
    for source in sources:
        dates = os.listdir(localPath + '/' + source)
        dates = list(map(lambda x: x.split('.')[0], dates))
        if len(dates) == 0: continue
        startDates.append(sorted(dates)[-1])
    
    if len(startDates) == 0:
        startDate = today()
    else:
        startDate = sorted(startDates)[0]
    print(startDate)

    startDate = datetime.strptime(startDate, '%Y%m%d')
    startDate = datetime.strptime('20240301', '%Y%m%d')
    endDate = datetime.today()
    targetdates = pandas.date_range(startDate,endDate-timedelta(days=1),freq='d')
    targetdates = list(map(lambda x: x.strftime('%Y%m%d'), targetdates))

    # currdates = list(map(lambda x: x.split('.')[0], os.listdir(localPath)))
    # targetdates = sorted(list(set(targetdates) - set(currdates)))#, reverse=True)
    print(targetdates)

    start = t.time()

    # conf = SparkConf().setAppName(
    # "parse BGP update"
    # ).set(
    #     "spark.kryoserializer.buffer.max", "512m"
    # ).set(
    #     "spark.kryoserializer.buffer", "1m"
    # )

    

    # sc = None
    # cnt = 0
    with Pool(12) as p:
        
        for date in targetdates:
            # if sc is None:
            #     sc = SparkContext(conf=conf)

            #     spark = SparkSession(sc)

            #     sc.setLogLevel("WARN")

            #     sc.addPyFile("/home/mhkang/mrtparse/mrtparse.zip")
            # cnt += 1            

            for source in sources:
                vpName = source

                currSavePath = "{}/{}".format(savePath, source)
                currLocalPath = "{}/{}".format(localPath, source)
                setPath(currSavePath, currLocalPath)

                if source == "bgpdata":
                    sourcePath = "{}/{}".format(bgpPath, source)
                else:
                    sourcePath = "{}/{}/bgpdata".format(bgpPath, source)

                year = date[:4]
                month = date[4:6]


                arg_list = []
                file_path = "{}/{}.{}/UPDATES/updates.{}".format(sourcePath, year, month, date)
                for i in range(0, 24, 2):
                    filename = file_path + ".{0:02}00.bz2".format(i)
                    arg_list.append((vpName, filename))

                result = set()

                # # lock = threading.Lock()
                
                # def collector(res):
                #     lock.acquire()

                #     for lines in res:
                #         result = result.union(lines)
                #         # result.extend(list(lines))
                #     lock.release()

                ret = p.map_async(readBGPUpdate, arg_list)

                results = ret.get()
                for r in results:
                    result = result.union(r)
                    
                # result = set(result)
                # result = sc.parallelize(result)\
                #             .distinct()

                saveFile = "{}/{}".format(currSavePath, date)
                localFile = "{}/{}".format(currLocalPath, date)
                with open(localFile + '.txt', 'w') as fout:
                    for line in result:
                        fout.write(line + '\n')
                # saveResult(result, saveFile, localFile, extension='.txt')

                end = t.time()
            # if cnt >= 10:
            #     sc.stop()
            #     sc = None
            #     cnt = 0

    
def main():
    parser = argparse.ArgumentParser(description='parse BGP update\n')
    parser.add_argument('--routeviewPath', default='/net/data/routeviews/raw-datasets')
    parser.add_argument('--savePath', default='/user/mhkang/routeviews/update')
    parser.add_argument('--localPath', default='/net/data/routeviews/update')
    
    parser.parse_args()
    args = parser.parse_args()
    print(args)

    parseBGPUpdate(args.routeviewPath, args.savePath, args.localPath)

if __name__ == '__main__':
    main()
    
# check what happend: done
# remove, run again, put it again
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240302.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240303.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240304.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240305.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240306.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240307.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240308.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240309.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240310.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240311.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240312.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240313.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240314.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240315.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240316.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240317.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240318.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240319.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240320.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240321.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240322.txt
# -rw-r--r--   2 mhkang mhkang          0 2024-03-29 04:15 /user/mhkang/routeviews/update/bgpdata/20240323.txt

# /net/data/routeviews/raw-datasets/route-views.amsix/bgpdata/2024.02/UPDATES/updates.20240229.0000.bz2
