import os
import sys
import time
import shutil
import operator
import argparse
import json
import pydoop.hdfs as hdfs

from datetime import datetime
from datetime import date, timedelta
from pyspark.sql import SQLContext, Row, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import input_file_name
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
from multiprocessing import Process
import subprocess

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from caida.as2isp import AS2ISP
from utils.utils import write_result, get_dates, get_date
#, ip2binary, append2dict, get_files, get_dates
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

def parseNRO(line):	
	date, prefixAddr, prefixLen, rir, _, _, _ = line.split(",")
	
	return [ (date, (prefixAddr, prefixLen, rir)) ]

def getRIR(date, prefix_addr, prefix_len, nro_dict):
	if nro_dict == None: return None
	tree, record_set = nro_dict.get(date, ({}, {}))

	binary_prefix = ip2binary(prefix_addr, prefix_len)
	records = get_records(tree, record_set, binary_prefix)
	for prefix_addr, prefix_len, rir in records:
		if rir is not None:
			return rir

	return None

def parseAttribute(lines, RADBObject):
	try:
		prev_key = None
		prev_value = ''
		multiple_lines = False
		for line in lines:
			if len(line) <= 0: continue
			if line.strip().startswith('#'): continue
			if line.strip().startswith('%'): continue

			if line.startswith((' ', '\t', '+')):
				if prev_key == None:
					continue
					# raise Exception('No previous key: ', line)
				value = line.strip()
				prev_value = prev_value + '\n' +  value
				multiple_lines = True
				continue
			tokens = line.split(':')

			key, value = None, None
			
			if len(tokens) < 2:
				if line.strip().startswith('#'): continue
				if line.strip().startswith('%'): continue
				if str(line) == "EOF": continue
				# print('Not a key-value pair: ' + line)
			else:
				if multiple_lines:
					if prev_key == None:
						raise Exception('multiple line attr has no prev key: ' + line)
					RADBObject[prev_key] = prev_value
					prev_value = ''
					prev_key = None
				key = tokens[0]
				value = ':'.join(tokens[1:]).strip()
				multiple_lines = False

			if key in RADBObject:
				old_value = RADBObject[key]
				if type(old_value) == list: RADBObject[key].append(value)
				else:  RADBObject[key] = [old_value, value]
			else:
				RADBObject[key] = value

			prev_key = key
			prev_value = value
	except:
		pass

def parseIRRObject(date, lines, nro_dict, as2isp_dict, source):
	lines_str = lines
	nro_dict =  nro_dict.value
	as2isp_dict = as2isp_dict.value
	
	# try:
	try: lines_str = str(lines)
	except: lines_str = lines
	if lines == '\n# EOF': return []
	lines = lines_str.split('\n')

	RADBObject = {}

	parseAttribute(lines, RADBObject)
	prefix = RADBObject.get('route', None)
	_prefix = RADBObject.get('*xxte', None)
	prefix6 = RADBObject.get('route6', None)
	_prefix6 = RADBObject.get('*xxte6', None)

	origin = RADBObject.get('origin', None)
	if prefix == None and _prefix == None and prefix6 == None and _prefix6 == None: return []
	if origin is None: return []
	
	rir = None
	isp = None
	country = None
	try: 
		origin = origin[2:]
		if origin in as2isp_dict:
			isp, country = as2isp_dict[origin]
			if(isp == ""): isp = None
			if(country == ""): country = None
		
	except Exception as e:
		pass

	
	if prefix is None: 
		if _prefix is not None: prefix = _prefix
		else:
			if prefix6 is None and _prefix6 is not None: prefix6 = _prefix6
			prefix = prefix6
		
	if prefix is not None: 
		# try:
		prefix = prefix.replace('\n', '').replace('+', '')
		try:
			prefix_addr, prefix_len = prefix.split('/')
		except:
			print("invalid prefix: " + prefix)
			return []
		_prefix_len = 0
		try:
			_prefix_len = int(prefix_len)
		except:
			if '#' in prefix_len: 
				try:
					_prefix_len = int(prefix_len.split('#')[0])
				except:
					return []
		prefix_len = _prefix_len
		rir = getRIR(date, prefix_addr, prefix_len, nro_dict)
		# except Exception as e:
		# 	pass
	changed = RADBObject.get('changed', None)
	if changed != None:
		last_changed = None

		if type(changed) is not list:
			changed = [changed]

		for c in changed:
			try:
				tokens = c.split(' ')
				if len(tokens) >= 2:
					curr_changed = tokens[1]
					if last_changed is None or curr_changed > last_changed:
						last_changed = curr_changed
			except: pass
		changed = last_changed
	else:
		changed = RADBObject.get('last-modified', None)
		if changed is not None:
			try: changed = ''.join(changed.split('T')[0].split('-'))
			except: changed = None
		
		if changed is None:
			changed = RADBObject.get('created', None)
			if changed is not None:
				try: changed = ''.join(changed.split('T')[0].split('-'))
				except: changed = None

	return ['\t'.join(list(map(str, [date, rir, prefix, origin, isp, country, source, changed])))]
	# except Exception as err:
	# 	raise Exception('{}\n{}'.format(lines, err))
	# 	return []



def get_source(filename):
	if filename.startswith('/user/mhkang/radb/'):
		return 'RADB'
	
	source = filename
	
	if '/' in source: source = source.split('/')[-1]
	if '.' in source: source = source.split('.')[0]
	if '_' in source: source = source.split('_')[-1]

	return source.upper()

def preprocessingIRRs(irrPath, nrov4Path, nrov6Path, asISPPath, savePath, localPath):


	args = [irrPath, nrov4Path, nrov6Path, asISPPath, savePath, localPath]
	ars = list(map(lambda x: x if x.endswith('/') else x + '/', args))
	
	try: hdfs.mkdir(savePath)
	except: pass
	try: os.mkdir(localPath)
	except: pass
	
	# try: hdfs.put(irrLocalPath + "*.csv", )
	# except: pass	
	irrFiles = hdfs.ls(irrPath)

	irrFiles = list(filter(lambda x: x.endswith('db') or x.endswith('db.gz') or x.endswith('route.gz'), irrFiles))
	
	nroFiles = hdfs.ls(nrov4Path)
	nroFiles += hdfs.ls(nrov6Path)
	
	currdates = get_dates(os.listdir(localPath))
	newdates = get_dates(irrFiles)
	
	targetdates = sorted(list(set(newdates) - set(currdates)))
	targetdates = ['20230520']
	nrodates = get_dates(nroFiles)
	if len(targetdates) == 0: 
		print("up to date")
		exit()
	
	print("target dates: {} ~ {}".format(targetdates[0], targetdates[-1]))

	asISP = AS2ISP(asISPPath)

	irrFiles = hdfs.ls(irrPath)

	batchSize = 30
	targetBatch = [targetdates[i:i + batchSize] for i in range(0, len(targetdates), batchSize )]

	for targetdates in targetBatch:
		print("start {} - {}".format(targetdates[0], targetdates[-1]))
		appName = "Preprocessing IRR datasets: {} - {}".format(targetdates[0], targetdates[-1])
		conf = SparkConf().setAppName(
				appName
			).set(
				"spark.kryoserializer.buffer.max", "256m"
			).set(
				"spark.kryoserializer.buffer", "512k"
			)

		sc = SparkContext(conf=conf)	

		spark = SparkSession(sc)
		
		sc.setLogLevel("WARN")

		sc.addPyFile("/home/mhkang/irredicator/utils.zip")

		for i, date in enumerate(targetdates):
			
			currIrrFiles = list(filter(lambda x: get_date(x).split('_')[0] == date, irrFiles))
			nrodate = date
			currNroFile = list(filter(lambda x: get_date(x) == date, nroFiles))

			if len(currIrrFiles) == 0:
				print("skip {} since len(irrFile) == {}".format(date, len(currIrrFiles)))
				continue
			if len(currNroFile) == 0:
				diff = list(map(lambda v: abs( (datetime.strptime(v, "%Y%m%d") - datetime.strptime(date, "%Y%m%d")).days), nrodates))
				nrodate = nrodates[diff.index(min(diff))]
				currNroFile = list(filter(lambda x: get_date(x) == nrodate, nroFiles))
			print("start preprocessing irr {} using nro {}".format(date, nrodate))
		
			nro_dict  = sc.textFile(','.join(currNroFile))\
						.flatMap(lambda line: parseNRO(line))\
						.groupByKey()\
						.map(lambda x: (x[0], make_binary_prefix_tree(x[1])))\
						.collectAsMap()
			
			nro_dict = sc.broadcast(nro_dict)
		
			as2isp_dict = asISP.getASISPDict(date)
			as2isp_dict = sc.broadcast(as2isp_dict)
			
			spark = SparkSession.builder.appName(appName).getOrCreate()

			results = None
			for irrFile in currIrrFiles:
				source = get_source(irrFile)
				currIrrs = spark.read.option('lineSep', '\n\n')\
										.option("wholeFile", True)\
										.option("multiline",True).text(irrFile).rdd\
										.flatMap(lambda row: parseIRRObject(date, row.value, nro_dict, as2isp_dict, source))\
										.distinct()
				
				# print(source)
				if results == None: results = currIrrs
				else: results = results.union(currIrrs)

			currSavePath  = savePath + date 
			currLocalPath  = localPath + date

			write_result(results, currSavePath, currLocalPath, extension='.tsv')

		sc.stop()
	
def preprocessIRRs():
	parser = argparse.ArgumentParser(description='preprocess irrs\n')
	parser.add_argument('--asISPPath', default='/home/mhkang/caida/as-isp/data/')
	parser.add_argument('--nrov4Path', type=str, default='/user/mhkang/nrostats/ipv4-w-date/')
	parser.add_argument('--nrov6Path', type=str, default='/user/mhkang/nrostats/ipv6-w-date/')
	parser.add_argument('--irrPath', type=str, default='/user/mhkang/irrs/rawdata/')
	parser.add_argument('--radbPath', type=str, default='/user/mhkang/radb/rawdata/')

	parser.add_argument('--savePath', type=str, default='/user/mhkang/irrs/daily-tsv-w-changed/raw/')
	parser.add_argument('--localPath', type=str, default='/net/data/irrs/daily-tsv-w-changed/')

	# parser.add_argument('--savePath2', type=str, default='/user/mhkang/radb/daily-tsv-w-changed/raw/')
	# parser.add_argument('--localPath2', type=str, default='/net/data/radb/daily-tsv-w-changed/')
	
	
	args = parser.parse_args()
	preprocessingIRRs(args.irrPath, args.nrov4Path, args.nrov6Path, args.asISPPath, args.savePath, args.localPath)
	# preprocessingIRRs(args.radbPath, args.nrov4Path, args.nrov6Path, args.asISPPath, args.savePath2, args.localPath2)

if __name__ == '__main__':

	preprocessIRRs()


