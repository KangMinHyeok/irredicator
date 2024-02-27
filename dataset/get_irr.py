import os
import ftplib
from datetime import date

irrs = [
            ("afrinic","ftp.afrinic.net","/pub/dbase/"),
            ("apnic","ftp.apnic.net","/pub/apnic/whois/"),
            ("arin","ftp.arin.net","/pub/rr/"),
            ("lacnic","ftp.lacnic.net","/lacnic/irr/"),
            ("tc","ftp.bgp.net.br","/dbase/"),
            ("altdb","ftp.altdb.net","/pub/altdb/"),
            ("jpirr","ftp.apnic.net","/public/apnic/whois-data/JPIRR/"),
            ("level3","rr.Level3.net","/pub/rr/"),
            ("netegg","ftp.nestegg.net","/irr/"),
            ("nttcom","rr1.ntt.net","/nttcomRR/"),
            ("openface","ftp.openface.ca","/pub/irr/"),
            ("panix","ftp.panix.com","/pub/rrdb/"),
            ("rgnet","rg.net","/rgnet/"),
            ("bboi","irr.bboi.net","/"),
            ("canarie","whois.canarie.ca","/dbase/"),
            ("radb","ftp.radb.net","/radb/dbase/"),
            ("ripe","ftp.ripe.net","/ripe/dbase/"),
            ("ripe split","ftp.ripe.net","/ripe/dbase/split/"),
            ("aoltw","ftp.newaol.com","/pub/aol-irr/dbase/"),
            ("bell","whois.in.bell.ca","/bell/"),
            ("easynet","ftp.whois.noc.easynet.net","/irr/"),
            ("epoch","whois.epoch.net","/pub/IRRd/databases/"),
            ("host","ftp.host.net","/host/dbase/"),
            ("ottlX","iskra.ottix.net","/pub/"),
            ("risq","rr.risq.net","/pub/"),
            ("rogers","whois.rogerstelecom.net","/rogers/")
        ]

def get_irr(path, currdate, domain, directory, irr):
    try:
        ftp = ftplib.FTP(domain)

        ftp.login()
        ftp.cwd(directory)

        files = []
        files = ftp.nlst()
        if irr == 'lacnic': 
            files = list(filter(lambda x: x.endswith('lacnic.db.gz'), files))
        else:
            files = list(filter(lambda x: x.endswith('.gz'), files))
        if len(files) == 0:
            files = list(filter(lambda x: x.endswith('.db'), files))

        for f in files:
            filename = path + currdate + '_' + f
            print('\tdownloading ' + f)
            if os.path.exists(filename):
                continue
            with open(path + currdate + '_' + f, 'wb') as fout:
                ftp.retrbinary('RETR ' + f, fout.write)
    
    except Exception as e:
        print(e)
        return False

    return True

def main():
    path = '/net/data/irrs/compressed/'
    currdate = date.today().strftime("%Y%m%d")

    for irr, domain, directory in irrs:
        print("Acess {} : {}{} ".format(irr, domain, directory))
        if get_irr(path, currdate, domain, directory, irr):
            print("Done")
        
if __name__ == '__main__':
    main()
    
    