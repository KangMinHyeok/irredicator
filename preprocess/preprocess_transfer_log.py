import os
import sys
import json

cwd = os.getcwd().split('/')
sys.path.append('/'.join(cwd[:cwd.index('irredicator')+1]))
from utils.utils import ip2binary, binary2ip

def get_common_prefix(a, b):
	prefix_addr, prefix_len = a, 32
	for i in range(len(a)):
		if a[i] != b[i]:
			prefix_addr, prefix_len = a[:i], i
			break

	return binary2ip(prefix_addr, prefix_len), str(prefix_len)

def main():
	indir = '/home/mhkang/transfers/json/'
	outdir = '/home/mhkang/transfers/tsv/'
	files  = os.listdir(indir)
	files = list(filter(lambda x: x.endswith('.json'), files))
	types = set()

	with open(outdir + 'total.tsv', 'w') as foutTotal:
		for file in files:
			ofile = file.replace('json', 'tsv')
			with open(outdir +  ofile, 'w') as fout:
				with open(indir + file, 'r') as fin:
					line = ''.join(fin.readlines())
					dic = json.loads(line)
					transfers = dic['transfers']
					for transfer in transfers:
						try:
							if 'ip4nets' not in transfer: continue
							transfer_type = transfer['type']
							if 'source_organization' in transfer:
								source_organization = transfer['source_organization']['name']
							else:
								source_organization = 'None'

							source_rir = transfer['source_rir']
							
							recipient_rir = transfer['recipient_rir']
							if 'recipient_organization' in transfer:
								recipient_organization = transfer['recipient_organization']['name']
							else:
								recipient_organization = 'None'
							transfer_date = transfer['transfer_date']
							transfer_date = transfer_date.split('T')[0].replace('-', '')

							transfer_items = transfer['ip4nets']
							if type(transfer_items) != list:
								transfer_items = [transfer_items]

							if source_rir == 'RIPE NCC': source_rir = "RIPE"
							if recipient_rir == 'RIPE NCC': recipient_rir = "RIPE"

							for item in transfer_items:
								for ip in item['transfer_set']:
									start_address = ip['start_address']
									end_address = ip['end_address']
									if ":" in start_address: continue

									start_prefix = ip2binary(start_address, 32)
									end_prefix = ip2binary(end_address, 32)
									prefix_addr, prefix_len = get_common_prefix(start_prefix, end_prefix)
									
									record = [transfer_date, prefix_addr, prefix_len, source_rir, source_organization, recipient_rir, recipient_organization, transfer_type]
									record = list(map(str, record))
									fout.write('\t'.join(record) + '\n')
									foutTotal.write('\t'.join(record) + '\n')
						except Exception as e:
							print(e)
							print(transfer)
							print(record)

if __name__ == '__main__':
	main()