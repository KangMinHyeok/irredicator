import os
import ftplib
from datetime import date

def download(path, domain, directory, rir):
	try:
		ftp = ftplib.FTP(domain)

		ftp.login()
		ftp.cwd(directory)

		files = []
		files = ftp.nlst()
		files = list(filter(lambda x: x.endswith('transfers_latest.json'), files))
		
		for f in files:
			ofilename = path + rir + '.json'
			print('\tdownloading ' + f)
			if os.path.exists(ofilename):
				continue
			with open(ofilename, 'wb') as fout:
				ftp.retrbinary('RETR ' + f, fout.write)
	except Exception as e:
		print(e)
		return False

	return True

def main():
	path = '/home/mhkang/transfers/'

	rirs = ['afrinic', 'arin', 'lacnic', 'apnic', 'ripencc']
	domain = 'ftp.ripe.net'
	for rir in rirs:
		directory = '/pub/stats/{}/transfers/'.format(rir)
		download(path, domain, directory)

if __name__ == '__main__':
	main()