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
from utils.utils import write_result, get_dates, ip2binary, get_files, origin2int
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

def parseBGP4MP(line):
	# try:
	# see mrtparse
	# https://github.com/t2mune/mrtparse

	# routeview dump: basic BGP4MP dump
	# | 0				| 
	# | vantage 		| 
	# | 1				| 2 		| 3 			| 4			| 5			|
	# | MRT type 		| timestamp | flag	 		| peer ip	| peer asn 	| 
	# | 6 				| 7 		| 8				|			|			|
	# | prefix 			| as path 	| protocol 		|			|			|

	# akamai dump: verbose BGP4MP dump
	# | 0				| 1			| 2 			| 3 		| 4			|
	# | MRT type 		| timestamp | flag	 		| peer ip	| peer asn 	| 
	# | 5				| 6 		| 7 			| 8			| 9			|
	# | prefix 			| as path 	| protocol 		| next_hop	| localpref |
	# | 10				| 11		| 12 			| 13		|			| 
	# | multi exit disc | community | atomic aggr	| (as4)aggr |			|
	try:
		
		tokens = line.split("|")
		if len(tokens) < 8: return []
		date, vantage_point, mrt_type, timevalue, peerIP, peerASN, prefix_addr, prefix_len, as_path, origin  = [None] * 10
	
		try:
			if tokens[0] == 'BGP4MP':
				vantage_point = 'akamai'
			else:
				vantage_point = tokens[0]
				tokens = tokens[1:]
			mrt_type, timevalue, flag, peerIP, peerASN, prefix, as_path, protocol = tokens[:8]
		except Exception as e:
			raise Exception("error: {}\nline: {}".format(e, line))


		if '/' not in timevalue: date = datetime.utcfromtimestamp(int(timevalue)).strftime("%Y%m%d")
		else: date = datetime.strptime(timevalue, "%m/%d/%y  %H:%M:%S").strftime("%Y%m%d")
		
		
		prefix_addr, prefix_len = prefix.split("/")
		

		as_path = as_path.split(' ')
		origin  = as_path[-1]
		if '{' in origin: return []
		
		date = int(date)
		origin = origin2int(origin)
		prefix_len = int(prefix_len)
		results = []
		results.append( ((prefix_addr, prefix_len, origin), date) )
		return results
	except Exception as e:
		return []

def parseBGP(line, bdates, ip_version='ipv4'):
	bdates = bdates.value
	try:
		date, rir, prefix_addr, prefix_len, origins, ISPs, countries, totalCnt = line.split('\t')
	except:
		return []

	if date not in bdates: return []
	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	origins = origins.split('|')

	if len(origins) != 1: return []

	try:
		prefix_len = int(prefix_len)
		origin = int(origins[0])
		# date = int(date)
	except:
		return []

	records = []
	records.append( ((prefix_addr, prefix_len, origin), date) )

	return records

def parseIRR(line, vrp_dict, ip_version='ipv4'):
	date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []

	if date == source: source = "RADB"
	try:
		origin = int(origin.replace("AS", ""))
	except:
		return []
	prefix_addr, prefix_len = prefix.split('/')
	prefix_len = int(prefix_len)
	max_len = prefix_len
	if rir == 'ripe': rir = 'ripencc'
	source = source.upper()

	vrp_dict = vrp_dict.value

	binary_prefix = ip2binary(prefix_addr, prefix_len)
	if binary_prefix == None: return []

	tree, record_set = vrp_dict.get('a', ({}, {}, {}))

	records = get_records(tree, record_set, binary_prefix)
	results = []
	validation = 'unknown'
	if len(entries) != 0:
		validation = 'invalid'
		for _prefix_addr, _prefix_len, _max_len, _origin, _rir, _isp in records:
			if _origin != origin: continue
			if _prefix_len > prefix_len or prefix_len > _max_len: continue
		
			validation = 'valid'
			break

	return [((prefix_addr, prefix_len, origin), (source, validation))]

def parseVRP(line, ip_version='ipv4'):
	date, prefix_addr, prefix_len, max_len, origin, numIP, cc, rir, isp, cc2 = line.split('\t')
	
	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	prefix_len, max_len = int(prefix_len), int(max_len)
	origin = int(origin)
	return [(date, (prefix_addr, prefix_len, max_len, origin, rir, isp))]

def toResult(row):
	key, value = row
	prefix_addr, prefix_len, origin = key
	irr, date = value

	source, validation = irr

	results = []
	results.append( (date, source, prefix_addr, prefix_len, origin, validation) )
	results.append( (date, 'ALL-IRR', prefix_addr, prefix_len, origin, validation) )

	results = list(map(lambda row: '\t'.join(list(map(str, list(row)))), results))
	return results

get_active_records(args.date, args.bgp_dir, roa_dir, irr_dir, hdfs_dir, local_dir)

def get_active_records(date, bgp_dir, roa_dir, irr_dir, hdfs_dir, local_dir):
	make_dirs(hdfs_dir, local_dir)

	hdfs_dir = hdfs_dir + 'raw/'
	make_dirs(hdfs_dir, local_dir)

	bgp_files = []
	subdirs = hdfs.ls(bgp_dir)
	for subdir in subdirs:
		bgp_files += get_files(subdir)

	bgp_files = sorted(bgp_files, reverse=True)
	
	dates = sorted(list(set(get_dates(bgp_files))), reverse=True)
	years = sorted(list(set(map(lambda x: x[:4], dates))), reverse=True)
	start_year = int(years[0])
	end_year = int(years[-1])
	batch_size = 10
	for year in range(start_year, end_year, -1):
		conf = SparkConf().setAppName(
				"get active records"
				).set(
					"spark.kryoserializer.buffer.max", "256m"
				).set(
					"spark.kryoserializer.buffer", "512k"
				)

		sc = SparkContext(conf=conf)

		spark = SparkSession(sc)

		sc.setLogLevel("WARN")
		
		irrRecords = None
		target_dates = sorted(list(filter(lambda x: x.startswith(str(year)), dates)))

		batch = [sorted(target_dates[i:i + batch_size]) for i in range(0, len(target_dates), batch_size )]
		for batch_dates in batch:
			todayBGPFiles = list(filter(lambda x: getDate(x) in batch_dates, bgpFiles))
			filename = '{}-{}'.format(batch_dates[0], batch_dates[-1])
			
			if irrRecords == None:
				vrpDict = sc.textFile(','.join([vrpFile]))\
						.flatMap(parseVRP)\
						.groupByKey()\
						.map(lambda row: ('a', make_binary_prefix_tree(row[1])) )\
						.collectAsMap()

				vrpDict = sc.broadcast(vrpDict)

				irrRecords = sc.textFile(','.join([irrFile, irrFile2]))\
						.flatMap(lambda line: parseIRR(line, vrpDict))
						
			bdates = sc.broadcast(batch_dates)
			currbgpRecords = sc.textFile(','.join(todayBGPFiles))\
								.flatMap(lambda line: parseBGP(line, bdates))\
								.distinct()
								

			results = irrRecords.join(currbgpRecords)\
									.flatMap(toResult)
	
			currSavePath = "{}{}".format(savePath, filename)
			currLocalPath = "{}{}".format(localPath, filename)

			write_result(results, currSavePath, currLocalPath, extension='.tsv')
			
			currbgpRecords.unpersist()
			results.unpersist()

		
		sc.stop()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='preprocess active\n')
	parser.add_argument('--date', default='20230301')
	parser.add_argument('--bgp_dir', default='/user/mhkang/bgp/routeview-reduced/')

	parser.add_argument('--roa_dir', default='/user/mhkang/vrps/daily-tsv/')
	parser.add_argument('--irr_dir', nargs='+', default=['/user/mhkang/radb/daily-tsv-w-changed/', '/user/mhkang/irrs/daily-tsv-w-changed/'])
	
	parser.add_argument('--hdfs_dir', default='/user/mhkang/radb/active-records/{}/')
	parser.add_argument('--local_dir', default='/net/data/radb/active-records/{}/')

	parser.parse_args()

	args = parser.parse_args()
	print(args)

	get_active_records(args.date, args.bgp_dir, roa_dir, irr_dir, hdfs_dir, local_dir)


