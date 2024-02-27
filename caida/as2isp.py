import os
import json
from datetime import datetime

class AS2ISP:
	def __init__(self, path='/'):
		if not path.endswith('/'): path = path + '/'
		self.raw_path = path
		self.export_path = path + "as2isp.json"

		self.date = []
		self.as2isp = None

		if not self.hasDB(): self.saveDB()
		self.loadDate()
		self.loadDB()

	def hasDB(self):
		files = os.listdir(self.raw_path)
		return 'as2isp.json' in files

	def loadDate(self):
		for fname in os.listdir(self.raw_path):
			if("as-org" not in fname): continue
			date = fname.split(".")[0]
			self.date.append(date)

	def loadDB(self):
		self.as2isp = json.load(open(self.export_path))
		print('as2ISP DB loaded done')
	
	def toDate(self, dateString):
		result = None
		try:
			result = datetime.strptime(dateString, "%Y%m%d")
		except:
			result = datetime.strptime(dateString, "%Y%m")
		return result

	def getISP(self, date, asnum ):
		
		diff = map(lambda v: abs( (self.toDate(v) - self.toDate(date)).days), self.date)

		dbdate = self.date[diff.index(min(diff))]

		asnum = str(asnum)
		if asnum not in self.as2isp[dbdate]:
			return 'None', 'None'
	
		org, country = self.as2isp[dbdate][asnum]
		if(country == ""): country = 'None'
		if(org == ""): org = 'None'

		return org, country
	
	def getASISPDict(self, date):
		diff = list(map(lambda v: abs( (self.toDate(v) - self.toDate(date)).days), self.date))
		dbdate = self.date[diff.index(min(diff))]

		return self.as2isp[dbdate]

	def saveDB(self):
		ORG_NAME    = "format:org_id|changed|org_name|country|source"
		AS_ORG      = "format:aut|changed|aut_name|org_id|source"
		AS_ORG_NEW  = "format:aut|changed|aut_name|org_id|opaque_id|source"
		asnumDB = {}

		for fname in os.listdir(self.raw_path):
			if("as-org2info.txt" not in fname): continue
			date = fname.split(".")[0]
			asnumDB[date] = {}
			org_id2name = {}
			as_asnum2name = {}

			line_type = 0
			fin = open(os.path.join(self.raw_path, fname))
			lines = fin.readlines()
		
			for i, line in enumerate(lines):
				if(ORG_NAME in line):
					line_type = 1
					continue

				elif(AS_ORG in line):
					line_type = 2
					continue

				elif(AS_ORG_NEW in line):
					line_type = 3
					continue

				if(line_type == 0):
					continue
				try:
					if(line_type == 1): ## ORG_NAME
						org_id, changed, org_name, country, source = line.split("|")
						org_id2name[org_id] = (org_name, country)

					elif(line_type == 2): ## AS_ORG
						asnum, changed, aut_name, org_id, source = line.split("|")
						asnumDB[date][asnum] = org_id2name[org_id]

					elif(line_type == 3): ## AS_ORG_NEW
						asnum, changed, aut_name, org_id, opaque_id, source = line.split("|")
						asnumDB[date][asnum] = org_id2name[org_id]
				except Exception as e:
					if str(e).startswith('need more than'):
						pass
					else:
						print("file: {}\nline {}: {}".format(fname, i+1, str(e)))
						raise Exception

		json.dump(asnumDB, open(self.export_path, "w"))
