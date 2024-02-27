import argparse
import pandas as pd
from datetime import *

from pyspark.sql import SQLContext, SparkSession, Row
from pyspark import SparkContext, StorageLevel, SparkConf, broadcast
from multiprocessing import Process
import pydoop.hdfs as hdfs

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from utils.utils import write_result, ip2binary, get_date, date_diff
from utils.binaryPrefixTree import make_binary_prefix_tree, get_records

def get_records(tree, record_set, binary_prefix):
    
    binary_prefixes = get_covered(tree, binary_prefix)
    
    records = []

    if len(binary_prefixes) == 0:
        return records

    for binary_prefix in binary_prefixes:
        records += record_set.get(binary_prefix, [])
    
    return records

def validate_irr(date, prefix_addr, prefix_len, origin, vrp_dict, ip_version='ipv4'):
	if binary_prefix == None: 
		return []

	binary_prefix = ip2binary(prefix_addr, prefix_len)

	tree, record_set = vrp_dict.get(date, ({}, {}))
	records = get_records(tree, record_set, binary_prefix)

	validation = 'unknown' if len(records) == 0 else 'invalid'

	for vrp_prefix_addr, vrp_prefix_len, max_len, vrp_origin in records:
		if vrp_origin != origin: continue
		if vrp_prefix_len <= prefix_len <= max_len:
			validation = 'valid'
		break

	return validation

def parse_IRR(line, vrp_dict, ip_version='ipv4'):
	date, rir, prefix, origin, isp, cc, source, changed = line.split('\t')

	if ip_version == 'ipv4' and ':' in prefix: return []
	elif ip_version == 'ipv6' and '.' in prefix: return []
	
	if date == source: source = "RADB"
	prefix_addr, prefix_len = prefix.split('/')
	try:
		date, origin, prefix_len = list(map(int, [date, origin, prefix_len]))
	except:
		return []

	validation = validate_irr('date', prefix_addr, prefix_len, origin, vrp_dict.value, ip_version=ip_version)
	
	return ['\t'.join(list(map(str, [date, rir, prefix, origin, isp, cc, source, changed, validation])))]


def parse_ROA(line, ip_version='ipv4'):
	tokens = line.split('\t')
	date, prefix_addr, prefix_len, max_len, origin, num_ip, cc, tal = tokens[:8]
	
	if ip_version == 'ipv4' and ':' in prefix_addr: return []
	elif ip_version == 'ipv6' and '.' in prefix_addr: return []

	if max_len == "None": max_len = prefix_len
	date, prefix_len, max_len, origin = list(map(int, [date, prefix_len, max_len, origin]))

	return [ (date, (prefix_addr, prefix_len, max_len, origin)) ]

def spark_validate_IRR(date, irr_files, roa_file, out_file, hdfs_path, local_path):
	
	make_dirs(hdfs_path, local_path)
	
	hdfs_path = hdfs_path + 'raw/'
	make_dirs(hdfs_path, local_path)


	conf = SparkConf().setAppName(
					"Analyze the age of IRR"
				).set(
					"spark.kryoserializer.buffer.max", "256m"
				).set(
					"spark.kryoserializer.buffer", "512k"
				)

	sc = SparkContext(conf=conf)

	spark = SparkSession(sc)

	sc.setLogLevel("WARN")
	
	
	roa_dict = sc.textFile(','.join([roa_file]))\
						.flatMap(parse_ROA)\
						.groupByKey()\
						.map(lambda x: ('date', make_binary_prefix_tree(x[1])))\
						.collectAsMap()

	roa_dict = sc.broadcast(roa_dict)

	results = sc.textFile(','.join(irr_files))\
						.flatMap(lambda line: parse_IRR(line, roa_dict))

	outfi = "validated-irr-" + date

	write_result(results, hdfs_path + out_file, local_path + out_file, extension='.tsv')

	sc.stop()

def toCDF(irr):
	irr = irr[['age']].groupby(['age'])['age'].agg('count').pipe(pd.DataFrame).rename(columns={'age':'frequency'})
	irr['pdf'] = irr['frequency'] / sum(irr['frequency'])
	irr['cdf'] = irr['pdf'].cumsum()
	irr = irr.reset_index()
	return irr[['age','cdf']]

def load_age(irrFile, latest_date):
	irr = pd.read_csv(irrFile, sep='\t', names=['date', 'rir', 'prefix','origin','isp','cc','source','changed','validation'])
	irr = irr[['changed','validation','source']]
	irr['age'] = irr.apply(lambda row: date_diff(row['changed'], latest_date), axis=1)
	irr = irr[irr['age'] != -1]

	return irr

def save_as_cdf(records, out_file):
	records = toCDF(records)
	records.to_csv(path_or_buf=out_file, index=False)

def save_age(irr, out_dir, source):
	if source != 'ALL-IRR':
		irr = irr[irr['source'] == source]

	valid = irr[irr['validation']=='valid']
	invalid = irr[irr['validation']=='invalid']
	
	save_as_cdf(irr, out_dir + source + '-cdf-all.tsv')
	save_as_cdf(valid, out_dir + source + '-cdf-valid.tsv')
	save_as_cdf(invalid, out_dir + source + '-cdf-invalid.tsv')


def analyze_irr_age(date, irr_dirs, rpki_dir, hdfs_dir, local_dir):
	out_file = "validated-irr-" + date

	irr_files = list(map(lambda x: x + date + '.tsv'), irr_dirs)
	roa_file = rpki_dir + date + '.tsv'
	spark_validate_IRR(date, irr_files, roa_file, out_file, hdfs_path, local_path)

	irr = load_age(out_file, date)

	save_age(irr, local_dir, "RADB")
	save_age(irr, local_dir, "ALL-IRR")


def main():
	parser = argparse.ArgumentParser(description='analyze the age of IRR objects\n')
	parser.add_argument('-n', '--names-list', nargs='+', default=[])
	parser.add_argument('--date', default='20230301')
	parser.add_argument('--irr_dirs', nargs='+', default=['/user/mhkang/irrs/daily-tsv-w-changed/', '/user/mhkang/radb/daily-tsv-w-changed/'])
	parser.add_argument('--rpki_dir', default='/user/mhkang/vrps/daily-tsv/')

	parser.add_argument('--hdfs_dir', default='/home/mhkang/irredicator_results/analysis/age/')
	parser.add_argument('--local_dir', default='/home/mhkang/irredicator_results/analysis/age/')
	
	parser.parse_args()
	args = parser.parse_args()
	print(args)

	analyze_irr_age(args.outdir, args.date)

	
if __name__ == '__main__':
	main()

	