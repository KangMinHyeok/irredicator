import os
import sys
import time
import shutil
import argparse
import random

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
from utils.utils import write_result, ip2binary, get_date, date_diff
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

def parseRel(line, ip_version='ipv4'):
	tokens = line.replace('\n', '').split('\t')
	if len(tokens) == 6:
		date, source, prefix, sumRel, roaRecordStr, irrRecordStr = tokens
	elif len(tokens) == 5:
		date, prefix, sumRel, roaRecordStr, irrRecordStr = tokens
		source = "RADB"
	else:
		return []


	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []

	key = (date, source)
	allkey = (date, 'ALL-IRR')
	roaRecords = tuple(map(lambda recordStr: recordStr.split(','), roaRecordStr.split('|')))
	allroaRecords = tuple(map(lambda record: (allkey, tuple(record[:3])), roaRecords))
	roaRecords = tuple(map(lambda record: (key, tuple(record[:3])), roaRecords))

	irrRecords = tuple(map(lambda recordStr: recordStr.split(','), irrRecordStr.split('|')))
	allirrRecords = tuple(map(lambda record: (allkey, tuple(record[:2])), irrRecords))
	irrRecords = tuple(map(lambda record: (key, tuple(record[:2])), irrRecords))

	if sumRel == 'partial' or len(roaRecords) == 0 or len(irrRecords) == 0:
		return []

	records = []
	records.append( (key, roaRecords, irrRecords) )
	records.append( (allkey, allroaRecords, allirrRecords) )
	
	return records

def parseIRR(line, ip_version='ipv4'):
	date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []
	if date == source:
		source = 'RADB'
		
	return [((date, source), 1), ((date, 'ALL-IRR'), 1)]

def parseVRP(line, ip_version='ipv4'):
	if line.startswith('#'): return []
	tokens = line.split('\t')
	date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir, isp  = tokens[:9]
	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	return [(date, 1)]


def dupVRP(row):
	date, cnt = row
	sources = ["APNIC", "ARIN", "RIPE", "LACNIC", "AFRINIC", "RADB", "ALL-IRR"]
	return [((date, source), cnt) for source in sources]


def getDate(filename):
	date = filename.split('/')[-1]
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
	return sorted(files)

def filterFiles(files, date):
	return list(filter(lambda x: getDate(x) >= date, files))

def toResult(row):
	key, value = row
	date, source = key
	
	try:
		a, irrTotalCount = value
		b, irrBothCount = a
		c, roaTotalCount = b
		bothCoveredCount, roaBothCount = c
	except:
		try:
			bothCoveredCount, roaBothCount, roaTotalCount, irrBothCount, irrTotalCount = row
		except:
			raise Exception("row: {}".format(list(row)))

	record = list(map(str, [date, source, bothCoveredCount, roaBothCount, roaTotalCount, irrBothCount, irrTotalCount]))
	return ','.join(record)

def countOverlap(relPath, irrPath, roaPath, roaPath2, savePath, localPath, patchPath, relPath2=None, irrPath2=None):
	try: hdfs.mkdir(savePath)
	except: pass
	try: os.mkdir(localPath)
	except: pass

	relFiles =  getFiles(relPath, path2=relPath2, extension='.tsv')
	irrFiles =  getFiles(irrPath, path2=irrPath2, extension='.tsv')
	roaFiles =  getFiles(roaPath, path2=roaPath2, extension='.tsv')
	
	start, end = '20191222', '20230301'
	allrelFiles =  list(filter(lambda x: start <= getDate(x) <= end, relFiles))
	allroaFiles =  list(filter(lambda x: start <= getDate(x) <= end, roaFiles))
	allirrFiles =  list(filter(lambda x: start <= getDate(x) <= end, irrFiles))
	
	patch = patchPath != None
	if patch:
		dates = list(set(getDates(os.listdir(patchPath))))
	else:
		dates = getDates(allrelFiles)


	print(dates)
	# exit()
	conf = SparkConf(
			).setAppName(
				"count as rel"
			).set(
				"spark.kryoserializer.buffer.max", "512m"
			).set(
				"spark.kryoserializer.buffer", "1m"
			)

	sc = SparkContext(conf=conf)

	spark = SparkSession(sc)

	sc.setLogLevel("WARN")

	allresult = None
	batchSize = 30
	dateBatch = [dates[i:i + batchSize] for i in range(0, len(dates), batchSize)]
	# relFileBatch = [allrelFiles[i:i + batchSize] for i in range(0, len(allrelFiles), batchSize)]
	# roaFileBatch = [allroaFiles[i:i + batchSize] for i in range(0, len(allroaFiles), batchSize)]
	# irrFileBatch = [allirrFiles[i:i + batchSize] for i in range(0, len(allirrFiles), batchSize)]
	for dates in dateBatch:
		relFiles =  list(filter(lambda x:  getDate(x) in dates, allrelFiles))
		roaFiles =  list(filter(lambda x:  getDate(x) in dates, allroaFiles))
		irrFiles =  list(filter(lambda x:  getDate(x) in dates, allirrFiles))
		# print(relFiles)
		# exit()
		# print(','.join(relFiles))
		relRecords = sc.textFile(','.join(relFiles))\
								.flatMap(parseRel)

		bothCoveredCount = relRecords.map(lambda row: (row[0], 1))\
								.reduceByKey(add)
		
		roaBothCount = relRecords.flatMap(lambda row: row[1])\
							.distinct()\
							.map(lambda row: (row[0], 1))\
							.reduceByKey(add)

		irrBothCount = relRecords.flatMap(lambda row: row[2])\
							.distinct()\
							.map(lambda row: (row[0], 1))\
							.reduceByKey(add)

		irrTotalCount = sc.textFile(','.join(irrFiles))\
								.flatMap(parseIRR)\
								.reduceByKey(add)

		roaTotalCount = sc.textFile(','.join(roaFiles))\
								.flatMap(parseVRP)\
								.reduceByKey(add)\
								.flatMap(dupVRP)

		results = bothCoveredCount.leftOuterJoin(roaBothCount)\
									.leftOuterJoin(roaTotalCount)\
									.leftOuterJoin(irrBothCount)\
									.leftOuterJoin(irrTotalCount)\
									.map(toResult)
		if allresult == None:
			allresult = results
		else:
			allresult = allresult.union(results)
	filename = "overlaps-ipv4-{}".format(end)
	if patch: filename += '-patch'

	write_result(allresult, savePath + filename, localPath + filename, extension='.csv')

	sc.stop()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='summarize as relationship\n')
	parser.add_argument('--relPath', default='/user/mhkang/radb/as-relationship-reduced/')
	parser.add_argument('--relPath2', default='/user/mhkang/irrs/as-relationship-reduced/')
	
	parser.add_argument('--irrPath',default='/user/mhkang/radb/daily-tsv-w-changed/')
	parser.add_argument('--irrPath2',default='/user/mhkang/irrs/daily-tsv-w-changed/')
	
	parser.add_argument('--roaPath', default='/user/mhkang/vrps/daily-tsv/')
	parser.add_argument('--roaPath2', default='/user/mhkang/vrps/daily-tsv2/')
	parser.add_argument('--patchPath', default='/net/data/vrps/patch-vrps/')

	parser.add_argument('--savePath', default='/user/mhkang/radb/discrepancy/overlaps/raw/')
	parser.add_argument('--localPath', default='/net/data/radb/discrepancy/overlaps/')

	parser.parse_args()
	args = parser.parse_args()
	print(args)
	patchPath=args.patchPath
	patchPath=None

	countOverlap(args.relPath, args.irrPath, args.roaPath, args.roaPath2, args.savePath, args.localPath,
		patchPath=patchPath, relPath2=args.relPath2, irrPath2=args.irrPath2)

	

