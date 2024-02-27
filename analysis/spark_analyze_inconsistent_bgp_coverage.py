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

def mergeAndSort(path, extension='.csv', label=None):
	try: os.mkdirs(path)
	except: pass

	filename = path.split('/')[-1]
	cmdMerge = 'find {} -name "*" -print0 | xargs -0 cat >> /tmp/tmp-spark-mhkang-{}'.format(path, filename)
	os.system(cmdMerge)

	cmdSort  = "sort -k1,1 /tmp/tmp-spark-mhkang-{} > {}".format(filename, path + extension)

	os.system(cmdSort)
	
	if(label is not None):
		cmd = "sed -i '1s/^/%s\\n/' %s" % (label, path +  extension)
		os.system(cmd)

	cmdErase = "rm /tmp/tmp-spark-mhkang-{}".format(filename)
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
			p = Process(target=fetchAndMerge, args=(savePath, localPath, extension, needSort))
			p.start()
			writeProcesses.append(p)
		return True
	else:
		return False

def correctPrefix(prefix_addr):
	tokens = prefix_addr.split('.')
	newTokens = []
	for token in tokens:
		prevIsZero = True
		if len(token) == 3 and token.startswith("00"):
			newTokens.append(token[2])
		elif len(token) >= 2 and token.startswith("0"):
			newTokens.append(token[1:])
		else:
			newTokens.append(token)
	return ".".join(newTokens)

def ip2binary(prefix_addr, prefix_len):
	try:
		octets = None
		if "." in prefix_addr: # IPv4
			octets = map(lambda v: int(v), prefix_addr.split("."))
			octets = map(lambda v: format(v, "#010b")[2:], octets)
		elif ':' in prefix_addr: # IPv6
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
		else: 
			return None

		return "".join(octets)[:int(prefix_len)]
	except:
		return None
	

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

def makeBinaryPrefixDict(records):
	recordSets = {}
	smtree_v4 = {}
	smtree_v6 = {}

	for record in records:
		prefix_addr, prefix_len = record[0], record[1]
		binary_prefix = ip2binary(prefix_addr, prefix_len)

		if(binary_prefix not in recordSets):
			recordSets[binary_prefix] = set()
		recordSets[binary_prefix].add( record )
		if(":" in prefix_addr):
			smInsert(smtree_v6, binary_prefix)
		else:
			smInsert(smtree_v4, binary_prefix)

	return recordSets, smtree_v4, smtree_v6

def getCovered(binary_prefix, smtree_v4, smtree_v6, ip_version):
	parent = binary_prefix
	keys = []
	if ip_version == 'ipv4':
		keys = smGetAllKeys(smtree_v4, parent)
	else:
		keys = smGetAllKeys(smtree_v6, parent)
	keys = sorted(keys, key=lambda x: len(x))
	return keys

def getEntries(date, binary_prefix, entryDict, ip_version='ipv4'):
	if entryDict == None: return [], []
	
	recordSets, smtree_v4, smtree_v6 = entryDict.get(date, [None, None, None])

	if recordSets is None:
		return [], []
	entries = getCovered( binary_prefix, smtree_v4, smtree_v6, ip_version)

	if entries is None or len(entries) == 0:
		return [], []

	return recordSets, entries

def getVRPOrigins(date, binary_prefix, vrpDict, ip_version='ipv4'):
	if binary_prefix == None: return [], [], []
	length = len(binary_prefix)

	vrpRecordSets, vrpEntries = getEntries(date, binary_prefix, vrpDict, ip_version)
	
	vrpOrigins = set()
	vrpRecords = set()
	for _binary_prefix in vrpEntries:
		
		for record in vrpRecordSets[_binary_prefix]:
			prefix_addr, prefix_len, max_len, origin = record

			if prefix_len <=  length <= max_len:
				vrpOrigins.add( origin )
				vrpRecords.add(tuple(record))
	
	return vrpOrigins, vrpRecords

def parseIRR(line, ip_version='ipv4'):
	date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []

	try: 
		date2 = datetime.strptime(date, "%Y%m%d")
		prefix_addr, prefix_len = prefix.split('/')
		prefix_len = int(prefix_len)
		origin = int(origin)
		date = int(date)
	except:	return []

	return [((date, prefix_addr, prefix_len), origin)]


def parseIRR4Dict(line, ip_version='ipv4'):
	date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []

	try: 
		date2 = datetime.strptime(date, "%Y%m%d")
		date = int(date)
		
		try: origin = int(origin.replace("AS", ""))
		except:
			try: origin = int(origin.split('#')[0])
			except: return []
		prefix_addr, prefix_len = prefix.split('/')
		prefix_len = int(prefix_len)
		# source = source.upper()
	except:	return []

	results = []
	results.append( (date, (prefix_addr, prefix_len, origin)) )
	
	return results

def parseVRP(line, ip_version='ipv4'):
	if line.startswith('#'): return []
	tokens = line.split('\t')
	date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir  = tokens[:8]

	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	try: 
		date2 = datetime.strptime(date, "%Y%m%d")
		date = int(date)
		origin = int(origin)
		prefix_len = int(prefix_len)
		max_len = int(max_len) if max_len != None and max_len != "None" else prefix_len

	except Exception as e:	
		return []
	
	return [ (date, (prefix_addr, prefix_len, max_len, origin)) ]

def parseIRRTotal(line, ip_version='ipv4'):
	date, rir, prefix, origin, isp, country, source, changed = line.split("\t")

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []

	try: 
		date2 = datetime.strptime(date, "%Y%m%d")
		date = int(date)
	except:	return []
		
	return [(date, 1)]

def parseVRPTotal(line, ip_version='ipv4'):
	if line.startswith('#'): return []
	tokens = line.split('\t')
	date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, rir, isp  = tokens[:9]
	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	try: 
		date2 = datetime.strptime(date, "%Y%m%d")
		date = int(date)
	except Exception as e:	
		return []
	
	return [(date, 1)]

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

def getResults(row, roaDict, filterTooSpecific=True, ip_version='ipv4'):
	key, value  = row
	date, prefix_addr, prefix_len = key 
	origins = value
	
	if filterTooSpecific and prefix_len > 24: return []
	binary_prefix = ip2binary(prefix_addr, prefix_len)
	if binary_prefix == None: return []

	vrpOrigins, vrpRecords = getVRPOrigins(date, binary_prefix, roaDict.value, ip_version=ip_version)
	if len(vrpRecords) == 0: return []

	results = []
	consistent = False
	for origin in set(origins):
		consistent = (origin in vrpOrigins) or consistent
		results.append( ((date, 'RADB') , 1))
	
	numConsistent = 1 if consistent else 0
	numDiscrepant = 0 if consistent else 1
	results.append( ((date, 'BOTH') , (1, numConsistent, numDiscrepant)))
	
	for prefix_addr, prefix_len, max_len, origin in vrpRecords:
		results.append( ((date, prefix_addr, prefix_len, max_len, origin, 'VRP') , 1))

	return results

def originToInt(origin):
	intOrigin = -1
	try: 
		intOrigin = int(origin)
	except Exception as e: 
		try:
			upper, lower = origin.split('.')
			intOrigin = int((int(upper) << 16) + int(lower))
		except:
			pass
	return intOrigin
	
def parseBGP(line, ip_version='ipv4'):
	try:
		date, rir, prefix_addr, prefix_len, origins, ISPs, countries, totalCnt = line.split('\t')
	except:
		return []

	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	totalCnt = int(totalCnt)
	origins = origins.split('|')

	prefix_len = int(prefix_len)
	
	records = []

	try:
		date = int(date)
		totalCnt = int(totalCnt)
	except:
		return []

	origins = list(filter(lambda x: x != -1, map(originToInt, origins)))
	if len(origins) != 1: return []

	records.append( ((date, prefix_addr, prefix_len), (origins, totalCnt, rir)) )

	return records

def toResult(row):
	key, value = row
	date = key
	
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

	record = list(map(str, [date, bothCoveredCount, roaBothCount, roaTotalCount, irrBothCount, irrTotalCount]))
	return ','.join(record)

def getEntries(date, binary_prefix, entryDict, ip_version='ipv4'):
	if entryDict == None: return [], []
	
	recordSets, smtree_v4, smtree_v6 = entryDict.get(date, [None, None, None])

	if recordSets is None:
		return [], []
	entries = getCovered( binary_prefix, smtree_v4, smtree_v6, ip_version)

	if entries is None or len(entries) == 0:
		return [], []

	return recordSets, entries

def getBothOrigins(date, binary_prefix, vrpDict, irrDict, ip_version='ipv4'):
	if binary_prefix == None: return [], [], []
	bgp_length = len(binary_prefix)

	vrpRecordSets, vrpEntries = getEntries(date, binary_prefix, vrpDict, ip_version)
	
	vrpOrigins = set()
	for _binary_prefix in vrpEntries:
		for record in vrpRecordSets[_binary_prefix]:
			prefix_addr, prefix_len, max_len, origin = record

			if prefix_len <=  bgp_length:# and int(bgp_length) <= int(max_len):
				vrpOrigins.add( (origin, max_len) )

	irrRecordSets, irrEntries = getEntries(date, binary_prefix, irrDict, ip_version)
	
	irrOrigins = set()
	for _binary_prefix in irrEntries:
		for record in irrRecordSets[_binary_prefix]:
			prefix_addr, prefix_len, origin = record
			if prefix_len <=  bgp_length:
				irrOrigins.add( origin )

	return list(vrpOrigins), list(irrOrigins)

def getBgpResults(row, roaDict, irrDict, filterTooSpecific=True, ip_version='ipv4'):
	key, value  = row
	date, prefix_addr, prefix_len = key 
	BGPs = value
	
	if filterTooSpecific and prefix_len > 24: return []
	binary_prefix = ip2binary(prefix_addr, prefix_len)
	if binary_prefix == None: return []

	vrpOrigins, irrOrigins = getBothOrigins(date, binary_prefix, roaDict.value, irrDict.value, ip_version=ip_version)
	
	
	vrpCoverOrigins = set()
	vrpValidOrigins = set()

	for origin, max_len in vrpOrigins:
		vrpCoverOrigins.add(origin)
		if prefix_len <= max_len:
			vrpValidOrigins.add(origin)
		
	results = []

	total = 1
	vrpCover = len(vrpCoverOrigins) > 0
	irrCover = len(irrOrigins) > 0
	bothCovered = 1 if vrpCover and irrCover else 0

	for BGPorigins, totalCnt, rir in BGPs:
		if len(BGPorigins) <= 0: continue
		consistent = 0
		discrepant = 0
			
		if bothCovered == 1:
			if len(BGPorigins) > 1:
				consistent = 1
			else:
				bgpOrigin = BGPorigins[0]
				irrValid = bgpOrigin in irrOrigins 
				vrpValid = bgpOrigin in vrpValidOrigins
				if (irrValid and vrpValid) or (not irrValid and not vrpValid):
					consistent = 1
				else:
					discrepant = 1
		results.append( (date, (total, bothCovered, consistent, discrepant)) )

	return results

def addBGP(valA, valB):
	totalA, bothCoveredA, consistentA, discrepantA = valA
	totalB, bothCoveredB, consistentB, discrepantB = valB
	return (totalA+totalB, bothCoveredA+bothCoveredB, consistentA+consistentB, discrepantA+discrepantB)

def setPath(savePath, localPath):
	try: hdfs.mkdir(savePath)
	except: pass
	
	try: os.mkdir(localPath)
	except: pass

def toCSV(row):
	date, value = row
	total, bothCovered, consistent, discrepant = value
	return ','.join(list(map(str, [date, total, bothCovered, consistent, discrepant])))

def addEntry(valA, valB):
	totalA, conA, disA = valA
	totalB, conB, disB = valB
	return (totalA + totalB, conA + conB, disA + disB)

def toEntry(row):
	key, value = row
	date, _ = key
	total, con, dis = value
	return ','.join(list(map(str, [date, total, con, dis])))

def countDiscrepancy(ip_version, bgpPath, irrPath, roaPath, roaPath2, savePath, localPath):
	setPath(savePath, localPath)

	savePath = savePath + 'raw/'
	setPath(savePath, localPath)

	bgpFiles = getFiles(bgpPath)
	irrFiles =  getFiles(irrPath, extension='.tsv')
	roaFiles =  getFiles(roaPath, roaPath2, extension='.tsv')
	
	bgpFiles = list(filter(lambda x: '20160801' <= getDate(x) <= '20210320', bgpFiles))
	roaFiles = list(filter(lambda x: '20160801' <= getDate(x) <= '20210320', roaFiles))
	irrFiles = list(filter(lambda x: '20160801' <= getDate(x) <= '20210320', irrFiles))
	
	targetDates = sorted(list(set(getDates(bgpFiles) + getDates(roaFiles) + getDates(irrFiles))))

	latestDate = sorted(os.listdir(localPath))
	
	currDates = []
	if len(latestDate) > 0:
		delta = timedelta(days=1)
		for dates in latestDate:
			tokens = dates.split('.')[0].split('-')
			if len(tokens) != 2: continue
			print(tokens)
			start, end = tokens
			start, end = datetime.strptime(start, "%Y%m%d"), datetime.strptime(end, "%Y%m%d")
			while start <= end:
				currDates.append(start.strftime("%Y%m%d"))
				start += delta
	
	newDates = list(set(targetDates) - set(currDates))
	targetDates = sorted(list(set(newDates)))

	# targetDates = ['20210320']

	conf = SparkConf(
			).setAppName(
				"count discrepant bgp " + ip_version
			).set(
				"spark.kryoserializer.buffer.max", "512m"
			).set(
				"spark.kryoserializer.buffer", "1m"
			)

	sc = SparkContext(conf=conf)

	spark = SparkSession(sc)

	sc.setLogLevel("WARN")

	bgpResults = None
	entryResults = None
	overlapResults = None

	batchSize = 5
	batch = [targetDates[i:i + batchSize] for i in range(0, len(targetDates), batchSize)]
	
	for i, dates in enumerate(batch):
		currBgpFiles = list(filter(lambda x: getDate(x) in dates, bgpFiles))
		currRoaFiles = list(filter(lambda x: getDate(x) in dates, roaFiles))
		currIrrFiles = list(filter(lambda x: getDate(x) in dates, irrFiles))

		if len(currRoaFiles) == 0:
			roaDict = sc.broadcast({})
		else:
			roaDict = sc.textFile(','.join(currRoaFiles))\
						.flatMap(lambda line: parseVRP(line, ip_version=ip_version))\
						.groupByKey()\
						.map(lambda x: (x[0], makeBinaryPrefixDict(x[1])))\
						.collectAsMap()
			
			roaDict = sc.broadcast(roaDict)
						
		BGPRecords	= sc.textFile(','.join(currBgpFiles))\
							.flatMap(parseBGP)\
							.groupByKey()

		if len(currIrrFiles) == 0:
			irrDict = sc.broadcast({})
		else:
			irrDict = sc.textFile(','.join(currIrrFiles))\
						.flatMap(lambda line: parseIRR4Dict(line, ip_version=ip_version))\
						.groupByKey()\
						.map(lambda x: (x[0], makeBinaryPrefixDict(x[1])))\
						.collectAsMap()
			
			irrDict = sc.broadcast(irrDict)

		bgpResult = BGPRecords.flatMap(lambda row: getBgpResults(row, roaDict, irrDict, filterTooSpecific=True))\
								.reduceByKey(addBGP)\
								.map(toCSV)
		
		# if bgpResults == None:
		# 	bgpResults = bgpResult
		# else:
		# 	bgpResults = bgpResults.union(bgpResult)

		dates = sorted(dates)
		filename = '{}-{}-{}'.format(dates[0], dates[-1], ip_version)

		saveResult(bgpResult, savePath + filename, localPath + filename, extension='.csv')
		
		bgpResult.unpersist()
		BGPRecords.unpersist()
		irrDict.unpersist()
		roaDict.unpersist()
	
	
	sc.stop()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='summarize as relationship\n')
	parser.add_argument('--irrPath',default='/user/mhkang/radb/daily-tsv-w-changed/')
	parser.add_argument('--irrPath2',default='/user/mhkang/irrs/daily-tsv-w-changed/')
	parser.add_argument('--bgpPath', default='/user/mhkang/bgp/routeview-reduced/')
	
	parser.add_argument('--roaPath', default='/user/mhkang/vrps/daily-tsv/')
	parser.add_argument('--roaPath2', default='/user/mhkang/vrps/daily-tsv2/')

	parser.add_argument('--savePath', default='/user/mhkang/radb/discrepancy/bgp/')
	parser.add_argument('--localPath', default='/net/data/radb/discrepancy/bgp/')
	
	parser.parse_args()
	args = parser.parse_args()
	print(args)

	ip_version = 'ipv4'
	countDiscrepancy(ip_version, args.bgpPath, args.irrPath, args.roaPath, args.roaPath2, args.savePath, args.localPath)
	
	# ip_version = 'ipv6'
	# countDiscrepancy(ip_version, args.bgpPath, args.irrPath, args.roaPath, args.roaPath2, args.savePath, args.localPath)
	

