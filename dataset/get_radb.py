import os
import sys
import ftplib
import argparse
from datetime import *
from multiprocessing import Pool

def download(outdir, date, domain, directory):
	ftp = ftplib.FTP(domain)
	ftp.login()
	ftp.cwd(directory)

	files = []
	files = list(filter(lambda x: x.endswith(date[2:]+'.gz'), ftp.nlst()))
	if len(files) != 0:
		file = files[0]	
		outfile = outdir + date + '.db.gz'
		with open(outfile, 'wb') as fout:
			ftp.retrbinary('RETR ' + file, fout.write)
		os.system('gzip -d {}'.format(outfile))

def get_radb(args):
	day, outdir = args
	date = "{:04d}{:02d}{:02d}".format(day.year, day.month, day.day)

	domain = 'ftp.radb.net'
	directory = '/radb/dbase/archive/'
	download(outdir, date, domain, directory)

def get_date(filename):
	return filename.split('.')[0]

def to_date(datestr):
	return datetime.strptime(datestr, "%Y%m%d")

def to_str(date):
	return date.strftime("%Y%m%d")

def get_start(outdir):
	return to_date(max(list(map(lambda  x: get_date(x), os.listdir(outdir)))))


def get_target_days(start, end):
	t = start + timedelta(days=1)
	limit = end
	
	days = []
	while (t <= limit):
		day = t
		days.append(day)
		t += timedelta(days=1)
	return days

def main():
	parser = argparse.ArgumentParser(description='download radb')
	parser.add_argument('--start', default=None)
	parser.add_argument('--end', default=None)
	parser.add_argument('--poolSize', default=10)
	parser.add_argument('--outdir', default='/net/data/radb/rawdata/')
	
	parser.parse_args()
	args = parser.parse_args()

	start = to_date(args.start) if args.start != None else get_start(args.outdir)
	end = to_date(args.end) if args.end != None else datetime.now()

	days = get_target_days(start, end)

	arglist = list(map(lambda x: (x, args.outdir), days))
	p = Pool(args.poolSize)
	p.map(get_radb, arglist)
	p.terminate()
	sys.stdout.write('{}-{}'.format(to_str(start), to_str(end)))


if __name__ == '__main__':
	main()

