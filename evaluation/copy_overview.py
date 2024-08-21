import os

def main():
	output_dir = '/home/mhkang/rpki-irr/outputs/irredicator/overviews/'
	latest_dir = '/net/data/irrs/webdata/latest/'

	output_files = os.listdir(output_dir)
	latest_files = os.listdir(latest_dir)

	output_files = list(filter(lambda x: x.endswith('.tsv'), output_files))
	latest_files = list(filter(lambda x: x.startswith('overview') and x.endswith('.tsv'), latest_files))

	latest_files = list(map(lambda x: x.split('_')[1], latest_files))
	target_files = sorted(list(set(output_files) - set(latest_files)))

	for target_file in target_files:
		with open(output_dir + target_file, 'r') as fin:
			with open(latest_dir + 'overview_' + target_file, 'w') as fout:
				header = ['prefix','origin','source','country','organization','last_modified','latest_rpki_status','latest_irredicator_status']
				fout.write('\t'.join(header) + '\n')
				for line in fin:
					fout.write(line)

if __name__ == '__main__':
	main()