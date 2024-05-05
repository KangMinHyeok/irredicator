import os
import sys
import shutil
import pydoop.hdfs as hdfs

from datetime import *
from ipaddress import IPv4Address


def readNcollectAsMap(sc, files, parse_func, map_func):
    result = {}
    if len(files) > 0:
        result = sc.textFile(','.join(files))\
            .flatMap(lambda line: parse_func(line))\
            .groupByKey()\
            .map(lambda x: (x[0], map_func(x[1])))\
            .collectAsMap()
    
    return result

def merge(path, extension='.csv', sort=False):
    os.makedirs(path, exist_ok=True)

    filename = path + extension

    cmd_erase = "rm {}".format(filename)
    os.system(cmd_erase)

    merge_filename = filename 
    if sort:
        merge_filename = '/tmp/tmp-spark-mhkang-' + filename.split('/')[-1]

    cmd_merge = 'find {} -name "*" -print0 | xargs -0 cat >> {}'.format(path, merge_filename)
    os.system(cmd_merge)

    if sort:
        cmd_sort  = "sort -k1,1 {} > {}".format(merge_filename, filename)
        os.system(cmd_sort)

        cmd_erase = "rm {}".format(merge_filename)
        os.system(cmd_erase)
        

def fetch(savePath, localPath):
    print("fetch {}".format(savePath))
    
    os.system("hdfs dfs -get {} {}".format(savePath, localPath))
    
    
def write_result(result, savePath, localPath, extension='.csv', sort=True):
    if result != None:
        try:
            os.system("hdfs dfs -rm -r {}".format(savePath)) 
        except: pass

        print("save {}".format(savePath))
        result.saveAsTextFile(savePath)

        shutil.rmtree(localPath, ignore_errors=True)

        os.makedirs(localPath, exist_ok=True)

        fetch(savePath, localPath)

        merge(localPath, extension=extension, sort=sort)
        
        shutil.rmtree(localPath, ignore_errors=True)

        return True
    else:
        return False

def makeDir(path):
    # change this to os.makedirs(path, exist_ok=True)
    try: 
        os.makedirs(path)
        print("{} made".format(path))
    except: 
        print("{} already exists".format(path))
        pass

def make_dirs(hdfs_path, local_path):
    try: hdfs.mkdir(hdfs_path)
    except: pass

    os.makedirs(local_path, exist_ok=True)

def get_date(filename):
    date = filename
    if '/' in date: date = date.split('/')[-1]
    if '.' in date: date = date.split('.')[0]
    if '-' in date: date = date.split('-')[0]
    return date

def get_dates(files):
    return sorted(list(set(map(get_date, files))))

def date_diff(start, end):
    if start == None or end == None:
        return -1
    try:
        start = datetime.strptime(str(start), "%Y%m%d")
        end = datetime.strptime(str(end), "%Y%m%d")
    except:
        return -2
    return (end - start).days

def sumRels(rels):
    result = "none"
    if len(rels) == 0: return 'not-covered'
    elif len(rels) == 1: result = rels[0]
    else:
        priority = ["self", "same", "sameISP", "customer-provider", "provider", "customer", "peer", "peering", "ddos", "none", "None"]
        
        for key in priority:
            if key in rels:
                result = key
                break

    keyMap = {  "self":"same", 
                "same":"same", 
                "sameISP": "sameISP",
                "provider":"customer-provider", 
                "customer": "customer-provider",
                "customer-provider": "customer-provider",
                "peer":"peering",
                "peering":"peering",
                "ddos":"ddos", 
                "None":"none",
                "none":"none"
            }

    result = keyMap.get(result, "none") 
    
    return result

def getRelationship(relDict, as1, as2, isp1=None, isp2=None):
    as1, as2 = str(as1), str(as2)
    if as1 == as2: 
        return "same"

    if isp1 is not None and isp1 == isp2:
        return 'sameISP'
    
    key = as1 + "+" + as2

    rel = relDict.get(key, "none")

    if rel == 'none':
        try:
            as1 = int(as1)
            as2 = int(as2)
        except:
            pass

        ddos_protection = [    20940, 16625, 32787,        # akamai
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
        if as1 in ddos_protection or as2 in ddos_protection:
            return 'ddos'

    return rel

def append2dict(d, key, value):
    if key not in d: d[key] = []
    d[key].append(value)

def add2dict(d, key, value):
    if key not in d: d[key] = set()
    d[key].add(value)

def filter_rejected(predictions):
    return list(filter(lambda x: x[1][0] * x[1][1] <= 0 and max(x[1]) > 0 , predictions))


def binary2prefix(binary_prefix):
    prefix_len = len(binary_prefix)
    binary_prefix += "0" * (32 - prefix_len)
    prefix_addr = ""
    for i in range(4):
        if i != 0: prefix_addr += '.'
        prefix_addr += str(int(binary_prefix[:8], 2))
        binary_prefix = binary_prefix[8:]
    
    return '{}/{}'.format(prefix_addr, prefix_len)
    

def iprange2prefix(ip_range):
    if '-' not in ip_range: return ip_range
    start_address, end_address = ip_range.split('-')

    startBinary = ip2binary(start_address, 32)
    endBinary = ip2binary(end_address, 32)
    prefix_len = 0
    for i in range(0,32):
        if startBinary[i] != endBinary[i]:
            prefix_len = i
            break

    startBinary =  ip2binary(start_address, prefix_len)
    return binary2prefix(startBinary)
    
def ip2binary(prefix_addr, prefix_len):
    if("." in prefix_addr): # IPv4
        octets = map(lambda v: int(v), prefix_addr.split("."))
        octets = map(lambda v: format(v, "#010b")[2:], octets)
    else: # IPv6
        octets = map(lambda v: str(v), prefix_addr.split(":"))
        prefix_addrs = prefix_addr.split(":")
        for i in range( 8 - len(prefix_addrs)):
            idx = prefix_addrs.index("")
            prefix_addrs.insert(idx, "")
        prefix_addrs += [""] * (8 - len(prefix_addrs)) # 8 groups, each of them has 16 bytes (= four hexadecimal digits)
        octets = []
        for p in prefix_addrs:
            if( len(p) != 4): # 4 bytes
                p = (4 - len(p)) * '0' + p
            for bit in p:
                b = format(int(bit, 16), "04b")
                octets.append( b)
    return "".join(octets)[:int(prefix_len)]

def binary2ip(prefix_addr, prefix_len):
    prefix_addr += "0"*(32 - prefix_len)
    addr = list(map(lambda x: int(x, base=2), [prefix_addr[:8],prefix_addr[8:16],prefix_addr[16:24],prefix_addr[24:32]]))
    ip_addr = "{}.{}.{}.{}".format(*addr)
    return ip_addr
    
def get_files(paths, extension=None):
    files = []
    if type(paths) == str:
        paths = [paths]
    elif type(paths) != list:
        print("Invalid paths type: {}".format(type(paths)))
        return files

    for path in paths:
        files += hdfs.ls(path)

    if extension is not None:
        files = list(filter(lambda x: x.endswith(extension), files))

    return files

def origin2int(origin):
    int_origin = -1
    if '.' in origin:
        upper, lower = origin.split('.')
        int_origin = int((int(upper) << 16) + int(lower))
    else:
        int_origin = int(origin)

    return int_origin
    