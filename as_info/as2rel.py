import os
import sys
import json
from datetime import *

class AS2Rel:
    def __init__(self, path=None):
        if path is not None:
            path = path if path.endswith('/') else path + '/'

            self.path     = path 
            self.export_path  = path + 'caida-as-rel.json' 

            self.dates = []
            self.db   = None

            if not self.hasDB(): 
                self.saveDB()
            else:
                self.loadDate()
                self.loadDB()
                if max(self.dates) not in self.db:
                    self.saveDB()
            
            self.ddos_protection = [    20940, 16625, 32787,        # akamai
                                        209, 3561,                  # CenturyLink
                                        13335,                      # CloudFlare
                                        19324,                      # DOSarrest
                                        55002,                      # F5 Networks
                                        19551,                      # Incapsula
                                        3549, 3356, 11213, 10753,   # Level 3
                                        7786, 12008, 19905,         # Neustar
                                        26415, 30060,               # Verisign
                                        200020                      # Nawas
                                    ]

    def copy(self, asRel, date):
        self.path = asRel.path
        self.export_path = asRel.export_path
        self.dates = asRel.dates
        
        self.ddos_protection = asRel.ddos_protection

        dbdate = self.getDBDates(date)
        self.db = {}
        self.db[dbdate] = asRel.db[dbdate]

    def loadDate(self): 
        for fname in os.listdir(self.path):
            if not fname.endswith("as-rel.txt"): continue
            date = fname.split(".")[0]
            self.dates.append(date)

    def hasDB(self):
        files = os.listdir(self.path)
        return 'caida-as-rel.json' in files

    def loadDB(self):
        sys.stdout.write('load caida as2rel ')
        self.db  = json.load(open(self.export_path))
        sys.stdout.write('done\n')


    def getASRelDic(self, date):
        dbdate = self.getDBDates(date)

        asRelDic = self.db[dbdate]
        return asRelDic
    
    def getDates(self):
        return self.dates

    def getDBDates(self, date):
        diffs = list(
                    map(
                        lambda d: abs( 
                                        (   datetime.strptime(d, "%Y%m%d") - 
                                            datetime.strptime(date, "%Y%m%d")
                                        ).days
                                )
                        , self.dates
                    )
                )
        dbdate = self.dates[diffs.index(min(diffs))]
        return dbdate

    def getRelationship(self, date, as1, as2, isp1=None, isp2=None):
        as1, as2 = str(as1), str(as2)
        if(as1 == as2): return "same"
        
        dbdate = self.getDBDates(date)

        key = as1 + "+" + as2
        if isp1 is not None and isp2 is not None and isp1 == isp2:
            return 'sameISP'
        rel = self.db[dbdate].get(key, "none")
    
        if rel == 'none':
            try:
                if int(as1) in self.ddos_protection\
                    or int(as2) in self.ddos_protection:
                    return 'ddos'
            except:
                pass

        return rel

    def isDDoS(self, asn):
        return (int(asn) in self.ddos_protection)

    def saveDB(self):
        self.db = {}
        # db_rel = set() 

        for fname in os.listdir(self.path):
            if not fname.endswith("as-rel.txt"): continue
            date = fname.split(".")[0]
            self.dates.append(date)

            if date not in self.db:
                self.db[date] = {}

            for line in open(os.path.join(self.path, fname)):
                if("#" in line):
                    continue

                as1, as2, rel = line.rstrip().split("|")

                if (rel == "-1"): #provider, customer

                    self.db[date][ as1 + "+" + as2 ] = "provider"  # as1 is the provider
                    self.db[date][ as2 + "+" + as1 ] = "customer" # as2 is the customer

                else: # peer
                    self.db[date][ as1 + "+" + as2 ] = "peer"  
                    self.db[date][ as2 + "+" + as1 ] = "peer" 
                
                # db_rel.add(as1 + "+" + as2)
                # db_rel.add(as2 + "+" + as1)
        
        json.dump( self.db, open(self.export_path, "w"), indent = 4)

