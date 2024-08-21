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


def put_files(hdfs_dir, local_dir):
    local_files = list(os.listdir(local_dir))
    local_files = set(map(lambda x: x.split('/')[-1], local_files))
    hdfs_files = list(hdfs.ls(hdfs_dir))
    hdfs_files = set(map(lambda x: x.split('/')[-1], hdfs_files))

    target_files = sorted(list(local_files - hdfs_files))
    target_files = list(filter(lambda x: x.endswith('db.gz') or x.endswith('db.route.gz'), target_files))

    put_fmt = '/usr/local/hadoop/bin/hdfs dfs -put /net/data/irrs/compressed/{} /user/mhkang/irrs/rawdata/ >> /home/mhkang/rpki-irr/logs/put-irrs.log 2>&1'

    for target_file in target_files:
        os.system(put_fmt.format(target_file))

def main():
    parser = argparse.ArgumentParser(description='put irr\n')

    
    parser.add_argument('--hdfs_dir', default='/user/mhkang/irrs/rawdata/')
    parser.add_argument('--local_dir', default='/net/data/irrs/compressed/')

    parser.parse_args()
    args = parser.parse_args()
    print(args)

    put_files(args.hdfs_dir, args.local_dir)

if __name__ == '__main__':

    main()
