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

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from caida.as2isp import AS2ISP
from utils.utils import write_result, ip2binary, append2dict, get_files, get_dates
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

def toTSV(data):
	return "\t".join(str(d) for d in data)

def parseNRO(line):
	date, prefix_addr, prefix_len, rir, _, _, _ = line.split(",")

	return [ (date, (prefix_addr, prefix_len, rir)) ]

def getRIR(date, prefix_addr, prefix_len, nro_dict):
	if nroDict == None: return None
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

def preprocessZiggyVRP(ziggyPath, nroPath, asISPPath, savePath, localPath):
	
	try: hdfs.mkdir(savePath)
	except: pass
	try: os.mkdir(localPath)
	except: pass
	
	ziggyFiles = get_files(ziggyPath, extension='.csv')
	nroFiles = get_files(nroPath, extension='.csv')
	targetdates = get_dates(ziggyFiles)
	
	if len(targetdates) == 0: 
		print("up to date")
		exit()
	
	print("target dates: {} ~ {}".format(targetdates[0], targetdates[-1]))

	asISP = AS2ISP(asISPPath)

	targetdatesDic = {}

	for date in targetdates:
		dbdate = asISP.getDBdate(date)
		append2dict(targetdatesDic, dbdate, date)

	items = sorted(targetdatesDic.items(), key=lambda x: x[0])

	for dbdate, targetdates in items:
		appname = "Preprocessing Ziggy VRP {} - {}".format(targetdates[0], targetdates[-1])
		conf = SparkConf().setAppName(appname)
		print("process: {} ~ {}".format(targetdates[0], targetdates[-1]))
		sc = SparkContext(conf=conf)

		spark = SparkSession(sc)
		
		sc.setLogLevel("WARN")
		
		for date in targetdates:
			currNroFiles = list(filter(lambda x: get_date(x) == date, nroFiles))
			currZiggyFiles = list(filter(lambda x: get_date(x) == date, ziggyFiles))
			
			if len(currZiggyFiles) == 0:
				print("skip {}: len(currZiggyFiles) == 0".format(date))
				continue
			if len(currNroFiles) == 0:
				diffs = list(map(lambda x: abs(datetime.strptime(get_date(x), "%Y%m%d") - datetime.strptime(get_date(date), "%Y%m%d")), nroFiles))
				currNroFiles.append(nroFiles[diffs.index(min(diffs))])

			nroDict  = sc.textFile(','.join(currNroFiles))\
							.flatMap(lambda line: parseNRO(line))\
							.groupByKey()\
							.map(lambda x: (x[0], makeBinaryPrefixDict(x[1])))\
							.collectAsMap()
			
			nroDict = sc.broadcast(nroDict)

			asISPDict = sc.broadcast(asISP.as2isp[dbdate])
			
			records  = sc.textFile(','.join(currZiggyFiles))\
						.flatMap(lambda line: parseZiggyVRP(date, line, nroDict, asISPDict))
			
			records = records.distinct()\
							.map(lambda row: toTSV(row))
			
			currSavePath  = savePath + date 
			currLocalPath  = localPath + date

			write_result(records, currSavePath, currLocalPath, extension='.tsv')
		
		sc.stop()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='preprocess vrp\n')
	parser.add_argument('--asISPPath', default='/home/mhkang/caida/as-org/data/')
	parser.add_argument('--nroPath', type=str, default='/user/mhkang/nrostats/ipv4-w-date/')
	parser.add_argument('--ziggyPath', type=str, default='/user/mhkang/vrps/ziggy-vrps/')

	parser.add_argument('--savePath', type=str, default='/user/mhkang/vrps/daily-tsv/raw/')
	parser.add_argument('--localPath', type=str, default='/net/data/vrps/daily-tsv/')

	args = parser.parse_args()
	
	preprocessZiggyVRP(args.ziggyPath, args.nroPath, args.asISPPath, args.savePath, args.localPath)


