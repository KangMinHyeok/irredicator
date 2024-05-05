import os
import sys

sys.path.append('/home/mhkang/rpki-irr/irredicator/')
from utils.utils import ip2binary

def make_binary_prefix_tree(records):
    tree = {}
    record_set = {}
    for record in records:
        prefix_addr, prefix_len = record[:1]
        binary_prefix = ip2binary(prefix_addr, prefix_len)

        insert(tree, record_set, binary_prefix, record)
        
        
    return tree, record_set

def insert(tree, record_set, binary_prefix, record):

    if binary_prefix not in record_set:
        record_set[binary_prefix] = set()
    
    record_set[binary_prefix].add(tuple(record))
    insert2tree(tree, binary_prefix)

def insert2tree(tree, binary_prefix):
    if len(binary_prefix) == 0:
        return
    
    subtree = tree
    for i in range(len(binary_prefix)):
        if binary_prefix[i] not in subtree:
            subtree[binary_prefix[i]] = {}

        subtree = subtree[binary_prefix[i]]

        if i == len(binary_prefix)-1:
            subtree['l']=True

def get_covered(tree, binary_prefix):
    covered=[]
    subtree = tree
    subp=""
    for t in binary_prefix:
        if t not in subtree:
            break
        subtree = subtree[t]
        subp = subp + t
        if 'l' in subtree:
            covered.append(subp)

    return sorted(covered, key=lambda x: len(x))

def get_records(tree, record_set, binary_prefix):
    
    binary_prefixes = get_covered(tree, binary_prefix)
    
    records = []

    if len(binary_prefixes) == 0:
        return records

    for binary_prefix in binary_prefixes:
        records += record_set.get(binary_prefix, [])
    
    return records
