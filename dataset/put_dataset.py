import os
import sys
import time as t
import json
import random
import shutil
import argparse
import numpy as np
import pydoop.hdfs as hdfs

from datetime import *

sys.path.append('/home/mhkang/rpki-irr/irredicator/')

from utils.utils import write_result, get_files, make_dirs, get_date


def put_files(hdfs_dir, local_dir, suffix, logfile):
    local_files = list(os.listdir(local_dir))
    local_files = set(map(lambda x: x.split('/')[-1], local_files))
    hdfs_files = list(hdfs.ls(hdfs_dir))
    hdfs_files = set(map(lambda x: x.split('/')[-1], hdfs_files))

    target_files = sorted(list(local_files - hdfs_files))
    target_files = list(filter(lambda x: x.endswith(suffix), target_files))

    put_fmt = '/usr/local/hadoop/bin/hdfs dfs -put {}{} {} >> /home/mhkang/rpki-irr/logs/{} 2>&1'

    for target_file in target_files:
        os.system(put_fmt.format(local_dir, target_file, hdfs_dir, logfile))

def main():
    parser = argparse.ArgumentParser(description='put dataset\n')
    
    parser.add_argument('--hdfs_dir', default='/user/mhkang/routeviews/bitvector/')
    parser.add_argument('--local_dir', default='/net/data/routeviews/bitvector/')
    parser.add_argument('--suffix', default='tsv')
    parser.add_argument('--logfile', default='log')
    parser.add_argument('--recursive', action='store_true')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    # parser.add_argument('--hdfs_dir', default='/user/mhkang/irrs/rawdata/')
    # parser.add_argument('--local_dir', default='/net/data/irrs/compressed/')

    # target_files = list(filter(lambda x: x.endswith('db.gz') or x.endswith('db.route.gz'), target_files))

    # python3.8 /home/mhkang/rpki-irr/irredicator/dataset/put_dataset.py --hdfs_dir /user/mhkang/irrs/rawdata/ --local_dir /net/data/irrs/compressed/ --suffix db.gz --logfile put-irrs.log
    # python3.8 /home/mhkang/rpki-irr/irredicator/dataset/put_dataset.py --hdfs_dir /user/mhkang/irrs/rawdata/ --local_dir /net/data/irrs/compressed/ --suffix db.route.gz --logfile put-irrs.log
    
    # (cd /net/data/routeviews/update/; for f in *; do hdfs dfs -put "/net/data/routeviews/update/$f" "/user/mhkang/routeviews/update"; done)
    if args.recursive is True:
        subdirs = os.listdir(args.local_dir)
        print(subdirs)
        for subdir in subdirs:
            local_dir = '{}{}/'.format(args.local_dir, subdir)
            hdfs_dir = '{}{}/'.format(args.hdfs_dir, subdir)
            put_files(hdfs_dir, local_dir, args.suffix, args.logfile)
    else:
        put_files(args.hdfs_dir, args.local_dir, args.suffix, args.logfile)

if __name__ == '__main__':

    main()
