o#!/usr/bin/env python3
#
# Copyright (c) 2018 NLnet Labs
# Licensed under a 3-clause BSD license, see LICENSE in the
# distribution
#
# This script retrieves the NRO statistics from the RIPE FTP server
# and merges the statistics for the individual RIRs into a single
# output file. It will only process ipv4 and ipv6 prefixes and
# will output data in the same format as the NRO stats
#
# This script may require you to install the following dependencies:
# - py-radix
#
# All of these dependencies are available through 'pip'

import os
import sys
import bz2
import math
import gzip
import radix
import ftplib
import os.path
import argparse
import requests
import datetime
import ipaddress
import time as timer
import dateutil.parser

from datetime import *
from io import StringIO
from multiprocessing import Pool, Lock
if sys.version_info[0] == 2: from urllib import urlretrieve
elif sys.version_info[0] == 3: from urllib.request import urlretrieve
else: raise Exception("python version {} is not supported: urlretrieve need to be imported".format(sys.version_info[0]))

ripeBaseURL	= 'https://ftp.ripe.net/pub/stats'
baseURL	= 'https://ftp.ripe.net/pub/stats'

tmpdir = '/home/mhkang/nrostats/rawdata'

startDates = {}
startDates["afrinic"] = "20050303"
startDates["arin"] = "20031120"
startDates["apnic"] = "20010501"
startDates["lacnic"] = "20040101"
startDates["ripencc"] = "20031126"

missingDates = {}
exceptionDates = {}

def addMissingDates(rir, ranges):
	global missingDates
	dates = []
	for start, end in ranges:
		numdays = (end - start).days
		dates = dates + [( (start + timedelta(days=x)).strftime('%Y%m%d'), end.strftime('%Y%m%d')) for x in range(numdays)]

	missingDates[rir] = {}
	for missing_date, end in dates:
		missingDates[rir][missing_date] = end

def initMissingDates():
	rir = 'afrinic'
	ranges = [	(date(2011,1,1), date(2011,5,16)), 
				(date(2014,12,31), date(2015,1,7)),
				(date(2015,12,31), date(2016,1,5)),
				(date(2016,12,31), date(2017,1,1)),
				(date(2017,12,31), date(2018,1,4))]
	addMissingDates(rir, ranges)
	
	rir = 'arin'
	ranges = [	(date(2019,8,25), date(2019,8,26)) ]
	addMissingDates(rir, ranges)

	rir = 'apnic'
	ranges = [	(date(2001,5,2), date(2001,6,1)),
				(date(2001,6,2), date(2001,9,1)),
				(date(2001,9,2), date(2001,10,1)),
				(date(2001,10,2), date(2001,11,1)),
				(date(2001,11,2), date(2001,12,1)),
				(date(2001,12,2), date(2002,1,1)),
				(date(2002,1,2), date(2002,2,1)),
				(date(2002,2,2), date(2002,3,1)),
				(date(2002,3,2), date(2002,4,1)),
				(date(2002,4,2), date(2002,5,1)),
				(date(2002,5,2), date(2002,6,1)),
				(date(2002,6,2), date(2002,7,1)),
				(date(2002,7,2), date(2002,8,1)),
				(date(2002,8,2), date(2002,9,1)),
				(date(2002,9,2), date(2002,10,1)),
				(date(2002,10,2), date(2002,11,1)),
				(date(2002,11,2), date(2002,12,1)),
				(date(2002,12,2), date(2003,1,1)),
				(date(2003,1,2), date(2003,2,1)),
				(date(2003,2,2), date(2003,3,1)),
				(date(2003,3,2), date(2003,4,1)),
				(date(2003,4,2), date(2003,5,1)),
				(date(2003,5,2), date(2003,5,8)),]
	addMissingDates(rir, ranges)

	rir = 'lacnic'
	ranges = [	(date(2018,9,26), date(2018,9,27)),
				(date(2018,11,10), date(2018,11,11)),
				(date(2019,12,21), date(2019,12,22)),
				(date(2020,4,22), date(2020,4,23)),]
	addMissingDates(rir, ranges)
	
	rir = 'ripencc'
	ranges = []
	addMissingDates(rir, ranges)

def addExceptionDates(rir, dates):
	global exceptionDates
	exceptionDates[rir] = {}
	for exception_date, year in dates:
		exceptionDates[rir][exception_date.strftime('%Y%m%d')] = year

def initExceptionDates():
	rir = 'afrinic'
	dates = [ (date(2012,12,31), 2013) ]
	addExceptionDates(rir, dates)

	rir = 'arin'
	dates = []
	addExceptionDates(rir, dates)

	rir = 'apnic'
	dates = [	(date(2010,12,31), 2011),
				(date(2012,12,31), 2013),
				(date(2013,12,31), 2014),
				(date(2014,12,31), 2015),
				(date(2015,12,31), 2016),
				(date(2016,12,31), 2017),
				(date(2017,12,31), 2018),
				(date(2018,12,31), 2019),
				]
	addExceptionDates(rir, dates)

	rir = 'lacnic'
	dates = []
	addExceptionDates(rir, dates)

	rir = 'ripencc'
	dates = [ (date(2004,1,1), 2003) ]
	addExceptionDates(rir, dates)

def adjustDate(rir, date):
	global missingDates
	global exceptionDates
	if date in missingDates[rir]: date = missingDates[rir][date]

	year = date[:4]
	if date in exceptionDates[rir]: year = exceptionDates[rir][date]

	return year, date

def getUrl(rir, date):
	year = date[:4]
	url = ""
	suffix = ''
	year, date = adjustDate(rir, date)
	if rir == "afrinic":
		url = '{baseURL}/{rir}/{year}/delegated-{rir}-{date}{suffix}'\
				.format(baseURL=baseURL, rir=rir, year=year, date=date, suffix=suffix)
	if rir == "arin":
		subdir = 'archive/{}/'.format(year) if year < '2017' else ''
		extended = '-extended' if date >= '20130305' else ''

		url = '{baseURL}/{rir}/{subdir}delegated-{rir}{extended}-{date}{suffix}'\
				.format(baseURL=baseURL, rir=rir, subdir=subdir, extended=extended, date=date, suffix=suffix)
	if rir == "apnic":
		suffix = '.gz'
		url = '{baseURL}/{rir}/{year}/delegated-{rir}-extended-{date}{suffix}'\
				.format(baseURL=baseURL, rir=rir, year=year, date=date, suffix=suffix)
	if rir == "lacnic":
		url = '{baseURL}/{rir}/delegated-{rir}-{date}{suffix}'.format(baseURL=baseURL, rir=rir, date=date, suffix=suffix)
	if rir == "ripencc":
		suffix = '.bz2'
		url = '{baseURL}/{rir}/{year}/delegated-{rir}-{date}{suffix}'.format(baseURL=baseURL, rir=rir, year=year, date=date, suffix=suffix)
	return url

def getTmp(tmpdir, rir, day, url):
	tmpfile = '{}/{}-{}'.format(tmpdir, rir, day)
	if url.endswith('.gz'): tmpfile += '.gz'
	elif url.endswith('.bz2'): tmpfile += '.bz2'
	
	return tmpfile

def fetchNROstats(date, rir):
	# try:
	# Where do the archives at the RIPE NCC
	# start_date = datetime.strptime(startDates[rir],"%Y%m%d")

	if date < startDates[rir]:
		# Write an empty file if the day requested
		# is not in the archives
		tmpfile = '{}/{}-empty'.format(rir, tmpdir)

		fd = open(tmpfile, 'w')
		fd.close()

		return tmpfile

	url = getUrl(rir, date)
	
	

	tokens = url.replace('https://', '').split('/')
	domain, directory, file = tokens[0], '/' + '/'.join(tokens[1:-1]) + '/', tokens[-1]
	ftp = ftplib.FTP(domain)
	ftp.login()
	ftp.cwd(directory)

	tmpfile = getTmp(tmpdir, rir, date, url)
	with open(tmpfile, 'wb') as fout:
		ftp.retrbinary('RETR ' + file, fout.write)

	# ftp.ripe.net/pub/stats/apnic/2021/delegated-apnic-extended-20210323.gz

	# sys.stdout.write('Fetching {} data from {} ... '.format(rir, url))
	# sys.stdout.flush()

	

	# os.system("wget -O {} {}".format(tmpfile, url))
	# urlretrieve(url, tmpfile)

	# print('OK ({})'.format(tmpfile))

	return tmpfile
	# except Exception as e:
	# 	_, _ , tb = sys.exc_info()
	# 	print('file {}, line {}: {}'.format(__file__, tb.tb_lineno, e))
	# 	raise Exception(e)

	# return None


# Added the specified prefix to the radix tree and check if
# it might already be present
def addPrefix(radix, prefix, prefixInfo):
	# Find out if the prefix is already there
	rnode = radix.search_exact(prefix)

	if rnode is None:
		rnode = radix.add(prefix)
	else:
		pass
		# print('Warning: {} already in radix tree'.format(prefix))

	infos = rnode.data.get('info', [])
	infos.append(prefixInfo)
	rnode.data['info'] = infos

# Adding an IPv4 address to the radix tree is tricky, since the
# number of IP addresses in the block in the NRO stats is not
# necessarily a power of two. We first test if it is a power of
# 2, and if it is not, we need to factorize it to get the
# subprefixes
def addIPv4Block(v4radix, prefixAddr, numIPs, prefixInfo):
	prefixAddr = ipaddress.ip_address(prefixAddr)

	while numIPs > 0:
		prefixSize = 32 - int(math.log(numIPs, 2))

		prefixFound = False
		# print("addrcount: {}".format(addrcount))
		while not prefixFound and prefixSize <= 32:
			try:
				isValidNetwork = ipaddress.ip_network(u'{}/{}'.format(prefixAddr, prefixSize))
				
				prefixFound = True
			except Exception as e:
				prefixSize += 1
		# exit()
		prefix = '{}/{}'.format(prefixAddr, prefixSize)

		currNumIPs = int(math.pow(2, 32 - prefixSize))
		addPrefix(v4radix, prefix, prefixInfo)

		numIPs -= currNumIPs
		prefixAddr += currNumIPs

def openNROStatsFile(filename):
	if filename.endswith('.gz'):
		return gzip.open(filename, 'r')
	elif filename.endswith('.bz2'):
		return bz2.BZ2File(filename, 'r')
	else:
		return open(filename, 'r')

# This is a very crude parser that will only process NRO stat lines
# that are for IPv4 or IPv6 prefixes and the contain exactly 7 fields
# separated by a '|' sign.
def parseNROStats(filename, v4radix, v6radix, asnDict, doIPv4, doIPv6, doASN):
	start = timer.time()
	
	totalASN = set()
	singleASN = set()
	multiASN = set()
	with openNROStatsFile(filename) as fin:
		lines = fin.readlines()
		for i, line in enumerate(lines):
			try:			
				if type(line) is bytes:
					line = line.decode('utf8')
				line = line.strip('\r').strip('\n')
				fields = line.split('|')
				# registry|cc|type|start|value|date|status|opaque-id
				if len(fields) < 7: continue

				rir, cc, recordType, start, value, date, status = fields[:7]
				
				if doIPv4 and recordType == 'ipv4':
					prefixAddr, numIPs = start, int(value)
					prefixInfo = (rir, cc, date, status)
					addIPv4Block(v4radix, prefixAddr, numIPs, prefixInfo)
					
				if doIPv6 and recordType == 'ipv6':
					prefixAddr, prefixLen = start, int(value)
					prefix = '{}/{}'.format(prefixAddr, prefixLen)
					prefixInfo = (rir, cc, date, status)
					addPrefix(v6radix, prefix, prefixInfo)

				if doASN and recordType == 'asn':
					if '.' in start:
						upper, lower = start.split('.')
						asn = int((int(upper) << 16) + int(lower))
					else:
						asn = int(start)
					numASN = int(value)
					if rir not in asnDict: asnDict[rir] = []
					for asnumber in range(asn, asn + numASN):
						asnInfo = (cc, date, status, asnumber)
						asnDict[rir].append(asnInfo)

			except Exception as e:
				print("error: {}\n line: {}".format(e, line))
				continue

			# We put the following information about the
			# prefix in the tuple:
			#  - RIR name
			#  - country
			#  - date of modification/assignment
			#  - prefix status
			#  - original starting block
			#  - original block IP count

##
# Main entry point
##

def downloadNRO(args):
	initMissingDates()
	initExceptionDates()
	day, outdir, needIPv4, needIPv6, needASN = args
	v4radix = radix.Radix()
	v6radix = radix.Radix()
	asnDict = {}

	rirs = ["apnic", "ripencc", "afrinic", "arin", "lacnic"]
	
	date = "{:04d}{:02d}{:02d}".format(day.year, day.month, day.day)

	v4File = '{}/ipv4-w-date/{}.csv'.format(outdir, date)
	v6File = '{}/ipv6-w-date/{}.csv'.format(outdir, date)
	asnFile = '{}/asn/{}.csv'.format(outdir, date)
	existIPv4 = os.path.exists(v4File)
	existIPv6 = os.path.exists(v6File)
	existASN = os.path.exists(asnFile)
	
	doIPv4, doIPv6, doASN = needIPv4 and not existIPv4, needIPv6 and not existIPv6, needASN and not existASN
	if not doIPv4 and not doIPv6 and not doASN:
		return 

	for rir in rirs:
		try:
			filename = fetchNROstats(date, rir)
		except: 
			return

		parseNROStats(filename, v4radix, v6radix, asnDict, doIPv4, doIPv6, doASN)

		if os.path.isfile(filename): os.remove(filename)

	# Write the consolidated NRO statistics to file

	if doIPv4: 
		with open(v4File, 'w') as fout:
			for rnode in v4radix:
				prefix = rnode.prefix
				prefixAddr, prefixLen = prefix.split('/')
				for rir, cc, registeredDate, status in rnode.data['info']:
					fout.write('{},{},{},{},{},{},{}\n'.format(date, prefixAddr, prefixLen, rir, cc, registeredDate, status))

	if doIPv6: 
		with open(v6File, 'w') as fout:
			for rnode in v6radix:
				prefix = rnode.prefix
				prefixAddr, prefixLen = prefix.split('/')
				for rir, cc, registeredDate, status in rnode.data['info']:
					fout.write('{},{},{},{},{},{},{}\n'.format(date, prefixAddr, prefixLen, rir, cc, registeredDate, status))


	if doASN:
		with open(asnFile, 'w') as fout:
			for rir, records in asnDict.items():
				for cc, registeredDate, status, asnumber in records:
					fout.write('{},{},{},{},{},{}\n'.format(date, asnumber, rir, cc, registeredDate, status))
def getDate(filename):
	return filename.split('.')[0]

def toDate(datestr):
	return datetime.strptime(datestr, "%Y%m%d")

def toStr(date):
	return date.strftime("%Y%m%d")

def getStart(outdir, doIPv4, doIPv6, doASN):
	starts = []
	dirnames = ['ipv4-w-date', 'ipv6-w-date', 'asn']
	flags = [doIPv4, doIPv6, doASN]
	for flag, dirname in zip(flags, dirnames):
		if flag:
			start = max(list(map(lambda  x: getDate(x), os.listdir(outdir + '/' + dirname))))
			starts.append(start)
	return toDate(min(starts))

def getTargetDays(start, end):
	t = start + timedelta(days=1)
	limit = end
	
	days = []
	while (t <= limit):
		day = t
		days.append(day)
		t += timedelta(days=1)
	return days

def main():
	parser = argparse.ArgumentParser(description='download nrostats')
	parser.add_argument('--start', default=None)
	parser.add_argument('--end', default=None)
	parser.add_argument('--poolSize', default=10)
	parser.add_argument('--outdir', default='/home/mhkang/nrostats')
	parser.add_argument('--ipv4', action='store_true', default=True)
	parser.add_argument('--ipv6', action='store_true', default=True)
	parser.add_argument('--asn', action='store_true', default=True)
	
	parser.parse_args()
	args = parser.parse_args()
	
	start = toDate(args.start) if args.start != None else getStart(args.outdir, args.ipv4, args.ipv6, args.asn)
	end = toDate(args.end) if args.end != None else datetime.now()
	
	days = getTargetDays(start, end)
	if len(days) != 0:
		arglist = list(map(lambda x: (x, args.outdir, args.ipv4, args.ipv6, args.asn), days))
		p = Pool(args.poolSize)
		p.map(downloadNRO, arglist)
		p.terminate()
		sys.stdout.write('{}-{}'.format(toStr(start), toStr(end)))
	
	
if __name__ == "__main__":
	main()

