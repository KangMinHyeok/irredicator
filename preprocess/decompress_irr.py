import os
import sys
import gzip
import shutil

def rir_or_jpnic(filename):
    if filename.endswith('apnic.db.route.gz'): return True
    if filename.endswith('apnic.db.route6.gz'): return True
    
    if filename.endswith('arin.db.gz'): return True

    if filename.endswith('afrinic.db.gz'): return True
    
    if filename.endswith('ripe.db.route.gz'): return True
    if filename.endswith('ripe.db.route6.gz'): return True

    if filename.endswith('ripe-noauth.db.route.gz'): return True
    if filename.endswith('ripe-noauth.db.route6.gz'): return True

    if filename.endswith('lacnic.db.gz'): return True
    
    if filename.endswith('jpirr.db.gz'): return True
    if filename.endswith('jpirr.db.long.gz'): return True

    return False

def main():
    indir = '/net/data/irrs/compressed/'
    outdir = '/net/data/irrs/rawdata/'

    

    infiles = os.listdir(indir)
    infiles = sorted(list(filter(rir_or_jpnic, infiles)))

    outfiles = os.listdir(outdir)

    total = len(infiles)
    for i, infile in enumerate(infiles):
        outfile = infile.replace('.gz', '')
        if outfile in outfiles:
            print('skip {}: file already exists'.format(outfile))
            continue
        sys.stdout.write('decompress {}/{} \r'.format(i, total))
        try:
            with gzip.open(indir + infile, 'rb') as fin:
                with open(outdir + outfile, 'wb') as fout:
                    shutil.copyfileobj(fin, fout)
        except:
            pass
            
if __name__ == '__main__':
    main()
    