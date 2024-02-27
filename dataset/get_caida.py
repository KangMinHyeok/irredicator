import os
import sys
import argparse
import requests
import subprocess
from datetime import *
from bs4 import BeautifulSoup
from multiprocessing import Pool

def decompress(filename):
	if filename.endswith('gz'):
		os.system("gzip -d {}".format(filename))
	elif filename.endswith('bz2'):
		os.system("bzip2 -d {}".format(filename))

def download(args):
	url, filename, outdir = args
	os.system("wget -q -P {} {}".format(outdir, url + filename))
	decompress(outdir+filename)


def get_date(filename):
	return filename.split('.')[0]

def to_date(datestr):
	return datetime.strptime(datestr, "%Y%m%d")

def to_str(date):
	return date.strftime("%Y%m%d")

def get_start(outdir):
	files = list(filter(lambda x: x.endswith('.txt'), os.listdir(outdir)))
	return to_date(max(list(map(lambda  x: get_date(x), files))))

def get_target_days(start, end):
	t = start
	limit = end
	
	days = []
	while (t <= limit):
		day = t
		days.append(day)
		t += timedelta(days=1)
	return days

def get_as2isp(start, end, poolSize):
	url = 'http://data.caida.org/datasets/as-organizations/'
	outdir = '/home/mhkang/caida/as-isp/data/'
	suffix = '.gz'
	filterPattern = "jsonl"
	
	start = to_date(start) if start != None else get_start(outdir)
	end = to_date(end) if end != None else datetime.now()

	req = requests.get(url)
	html = req.text
	soup = BeautifulSoup(html, 'html.parser')

	links = soup.select('pre > a')
	argList = []
	for link in links:
		filename = link.get('href')
		if filename.endswith(suffix):
			date = to_date(filename.split('.')[0])
			if date <= start or date > end: continue
			if filterPattern in filename: continue
			argList.append( (url, filename, outdir))

	p = Pool(poolSize)
	p.map(download, argList)
	p.terminate()
	sys.stdout.write('{}-{}'.format(to_str(start), to_str(end)))
	
def get_as2rel(start, end, poolSize):
	url = 'http://data.caida.org/datasets/as-relationships/serial-1/'
	outdir = '/home/mhkang/caida/as-rel/data/'
	suffix = '.bz2'
	filterPatterns = ['ppdc-ases', 'stable']
	
	start = to_date(start) if start != None else get_start(outdir)
	end = to_date(end) if end != None else datetime.now()
	
	req = requests.get(url)
	html = req.text
	soup = BeautifulSoup(html, 'html.parser')

	links = soup.select('pre > a')
	argList = []
	for link in links:
		filename = link.get('href')
		if filename.endswith(suffix):
			date = to_date(filename.split('.')[0])
			if date <= start or date > end: continue
			if filterPatterns[0] in filename or filterPatterns[1] in filename: continue
			argList.append( (url, filename, outdir))

	p = Pool(poolSize)
	p.map(download, argList)
	p.terminate()
	sys.stdout.write('{}-{}'.format(to_str(start), to_str(end)))

def main():
	parser = argparse.ArgumentParser(description='download caida')
	parser.add_argument('--start', default=None)
	parser.add_argument('--end', default=None)
	parser.add_argument('--poolSize', default=10)
	parser.add_argument('--outdir', default='/home/mhkang/caida/')
	parser.add_argument('--rel', action='store_true', default=False)
	parser.add_argument('--isp', action='store_true', default=False)
	
	parser.parse_args()
	args = parser.parse_args()

	if args.rel: get_as2rel(args.start, args.end, args.poolSize)
	if args.isp: get_as2isp(args.start, args.end, args.poolSize)

if __name__ == '__main__':
	main()
