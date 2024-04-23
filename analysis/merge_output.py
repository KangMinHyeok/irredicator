import os

def merge_output(coverage):
	indir = '/home/mhkang/rpki-irr/outputs/analysis/{}/'.format(coverage)
	print(indir)
	outdir = '/net/data/irrs/webdata/latest/'

	infiles = os.listdir(indir)
	infiles = list(filter(lambda x: x.startswith(coverage), infiles))
	print(indir)

	currfiles = os.listdir(outdir)

	currfiles = list(filter(lambda x: x.startswith(coverage), currfiles))
	latestdate = max(list(map(lambda x: x.split('.')[0].split('_')[-1], currfiles)))

	infiles = list(filter(lambda x: x.split('.')[0].split('-')[-1] > latestdate, infiles))

	percentages = {'ALL-IRR':{}, 'VRP':{}}
	newlatestdate = latestdate
	print(infiles)
	for infile in infiles:
		with open(indir + infile, 'r') as fin:
			for line in fin:
				date, source, cnt, total = line.split(',')[:4]
				if date <= latestdate: continue
				if source in percentages:
					try:
						percentage = float(cnt) / float(total) * 100.0 if int(total) != 0 else 0.0
					except:
						print(line)
						percentage = 0.0
					percentages[source][date] = percentage
					if date > newlatestdate: newlatestdate = date

	print(percentages)
	with open(outdir + '{}_{}.csv'.format(coverage, newlatestdate), 'w') as fout:
		dates = sorted(percentages['ALL-IRR'].keys())
		fout.write('date,RPKI,IRR\n')
		for date in dates:
			fout.write('{},{},{}\n'.format(date, percentages['VRP'].get(date, 0.0), percentages['ALL-IRR'].get(date, 0.0)))


def main():
	merge_output('ip-coverage')
	merge_output('as-coverage')

if __name__ == '__main__':
	main()