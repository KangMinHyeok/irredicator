import os
import sys
import time
import shutil
import argparse
import random

import pandas as pd
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

def mergeActive(row):
	key, value = row
	source, prefix_addr, prefix_len, origin, validataion = key
	prev, curr = value

	dates = []
	if prev == None and curr == None: return []
	elif prev == None: dates = list(curr)
	elif curr == None: dates = list(prev)
	else: dates = list(prev) + list(curr)

	return [((source, prefix_addr, prefix_len, origin, validataion), max(dates))]

def parseVRP(line, ip_version='ipv4'):
	date, prefix_addr, prefix_len, max_len, origin, numIP, cc, rir, isp, cc2 = line.split('\t')
	
	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	prefix_len, max_len = int(prefix_len), int(max_len)
	origin = int(origin)
	return [(date, (prefix_addr, prefix_len, max_len, origin))]

def parseIRR(line, vrp_dict, ip_version='ipv4'):
	date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []

	if date == source: source = "RADB"
	try:
		origin = int(origin)
	except:
		return []
	prefix_addr, prefix_len = prefix.split('/')
	prefix_len = int(prefix_len)
	

	binary_prefix = ip2binary(prefix_addr, prefix_len)
	if binary_prefix == None: return []

	tree, record_set = vrp_dict.value.get('date', ({}, {}))
	records = get_records(tree, record_set, binary_prefix)
	
	validation = 'unknown' if len(records) == 0 else 'invalid'

	for _prefix_addr, _prefix_len, _max_len, _origin, _rir, _isp in records:
		if _origin != origin: continue
		if _prefix_len > prefix_len or prefix_len > _max_len: continue
	
		validation = 'valid'
		break
	results = []

	if validation != 'unknown':
		results.append( (source, validation, prefix, origin) )
		results.append( ("ALL-IRR", validation, prefix, origin) )

	results.append( (source, 'total', prefix, origin) )
	results.append( ("ALL-IRR", 'total', prefix, origin))

	return results


def parseActiveIRR(line):
	tokens = line.split('\t')
	if len(tokens) != 5 and len(tokens) != 6: return []
	date, source, prefix_addr, prefix_len, origin, validataion = None, None, None, None, None, None

	if len(tokens) == 5:
		prefix_addr, prefix_len, origin, validataion, date = tokens
		source = "RADB"
	else:
		date, source, prefix_addr, prefix_len, origin, validataion = tokens

	return ((source, prefix_addr, prefix_len, origin, validataion), date)

def toResult(row):
	key, latestDate = row
	source, prefix_addr, prefix_len, origin, validataion = key
	results = []
	if validataion != 'unknown':
		results.append( ( (source, validataion, latestDate), 1) )
	results.append( ( (source, 'total', latestDate), 1) )
	return results

def toCSV(row):
	key, cnt = row
	source, validataion, latestDate = key
	return '{},{},{},{}'.format(source, validataion, latestDate, cnt)

def mergeIRRActive(targetDate, activePath, savePath, localPath):

	try: hdfs.mkdir(savePath)
	except: pass

	savePath = savePath + 'raw/'
	try: hdfs.mkdir(savePath)
	except: pass

	try: os.makedirs(localPath)
	except: pass
	activePath += targetDate + '/'
	infiles = hdfs.ls(activePath)
	
	infiles = sorted(list(filter(lambda x: x.endswith(extension), infiles)), reverse=True)

	print("infiles: {}-{}".format(infiles[0], infiles[-1]))

	conf = SparkConf().setAppName(
				"analyze Activeness"
				).set(
					"spark.kryoserializer.buffer.max", "512m"
				).set(
					"spark.kryoserializer.buffer", "1m"
				)

	sc = SparkContext(conf=conf)

	spark = SparkSession(sc)

	sc.setLogLevel("WARN")

	results = None

	for infile in infiles:

		currResults = sc.textFile(','.join([infile]))\
								.map(parseActiveIRR)\
								.distinct()
								
		if results == None:
			results = currResults
		else:
			results = results.cogroup(currResults)\
							.flatMap(mergeActive)

		print(results.count())
		currResults.unpersist()

	results = results.flatMap(toResult)\
					.reduceByKey(add)\
					.map(toCSV)
	
	saveResult(results, savePath + targetDate, localPath + targetDate, extension='.csv')
	write_result(results, savePath + targetDate, localPath + targetDate, extension='.csv')

	sc.stop()


def getTotalCounts(targetDate, vrpPath, irrPath, irrPath2, relPath, relPath2):
	conf = SparkConf().setAppName(
				"get total count"
				).set(
					"spark.kryoserializer.buffer.max", "512m"
				).set(
					"spark.kryoserializer.buffer", "1m"
				)

	sc = SparkContext(conf=conf)

	spark = SparkSession(sc)

	sc.setLogLevel("WARN")

	results = None
	paths = [vrpPath, irrPath, irrPath2, relPath, relPath2]
	vrpFile, irrFile, irrFile2, relFile, relFile2 = list(map(lambda path: path + targetDate + '.tsv', paths))

	vrp_dict = sc.textFile(','.join([vrpFile]))\
						.flatMap(parseVRP)\
						.groupByKey()\
						.map(lambda row: ('date', make_binary_prefix_tree(row[1])) )\
						.collectAsMap()

	vrp_dict = sc.broadcast(vrp_dict)

	irrDict = sc.textFile(','.join([irrFile, irrFile2]))\
			.flatMap(lambda line: parseIRR(line, vrp_dict))\
			.distinct()\
			.map(lambda row: ((row[0], row[1]), 1))\
			.reduceByKey(add)\
			.collectAsMap()

	totalCountDict = {}

	for k, v in irrDict.items():
		source, validation = k
		if source not in totalCountDict: totalCountDict[source] = {}
		totalCountDict[source][validation] = v
	
	sc.stop()

	return totalCountDict

def getActiveCDF(targetDate, vrpPath, irrPath, irrPath2, relPath, relPath2, recordPath, cdfPath):
	totalCountDict = getTotalCounts(targetDate, vrpPath, irrPath, irrPath2, relPath, relPath2)
	
	irrFile = recordPath + '{}.csv'.format(targetDate)
	outfile = cdfPath + '{}.csv'.format(targetDate)
	dic = {}

	with open(irrFile, 'r') as fin:
		for line in fin:
			tokens = line.replace('\n','').split(',')
			source, validation, date, cnt = tokens
			cnt = int(cnt)
			
			if source not in dic: dic[source] = {}
			if date not in dic[source]: dic[source][date] = {}
			
			dic[source][date][validation] = cnt

	with open(outfile, 'w') as fout:
		for source, _dic in dic.items():
			irr, valid, invalid = 0, 0, 0
			_totalCountDict = totalCountDict.get(source, {})
			for date, dateDic in sorted(_dic.items(), key=lambda x: x[0], reverse=True):
				irr += dateDic.get('total', 0)
				valid += dateDic.get('valid', 0)
				invalid += dateDic.get('invalid', 0)
				
				irrTotal = _totalCountDict.get('total')
				validTotal = _totalCountDict.get('valid')
				invalidTotal = _totalCountDict.get('invalid')
				
				record = [source, date, irr, irrTotal, valid, validTotal, invalid, invalidTotal]
				fout.write(','.join(list(map(str, record))) + '\n')
				
if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='summarize as relationship\n')

	parser.add_argument('--targetDate', default='20230301')
	parser.add_argument('--vrpPath', default='/user/mhkang/vrps/daily-tsv/')
	parser.add_argument('--irrPath', default='/user/mhkang/radb/daily-tsv-w-changed/')
	parser.add_argument('--irrPath2', default='/user/mhkang/irrs/daily-tsv-w-changed/')

	parser.add_argument('--relPath', default='/user/mhkang/radb/as-relationship-reduced/')
	parser.add_argument('--relPath2', default='/user/mhkang/irrs/as-relationship-reduced/')

	parser.add_argument('--activePath', default='/user/mhkang/radb/active-records/')
	
	parser.add_argument('--savePath', default='/user/mhkang/radb/active-records-merged/')
	parser.add_argument('--recordPath', default='/net/data/radb/active-records-merged/')

	parser.add_argument('--cdfPath', default='/net/data/radb/active-records-cdf/')

	parser.parse_args()
	args = parser.parse_args()
	print(args)

	getActiveCDF(args.targetDate, args.vrpPath, args.irrPath, args.irrPath2, args.relPath, args.relPath2, 
		args.recordPath, args.cdfPath)
