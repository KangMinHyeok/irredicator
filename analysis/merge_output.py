import os

def get_infiles_and_latestdate(indir, outdir, target):
	infiles = os.listdir(indir)
	infiles = list(filter(lambda x: x.startswith(target), infiles))
	
	currfiles = os.listdir(outdir)
	currfiles = list(filter(lambda x: x.startswith(target), currfiles))

	latestdate = max(list(map(lambda x: x.split('.')[0].split('_')[-1], currfiles)))

	infiles = list(filter(lambda x: x.split('.')[0].split('-')[-1] > latestdate, infiles))
	return infiles, latestdate

def parse_coverage(line):
	date, source, cnt, total = line.split(',')[:4]
	try:
		cnt = int(cnt)
		percentage = float(cnt) / float(total) * 100.0 if int(total) != 0 else 0.0
	except:
		print(line)
		cnt = 0
		percentage = 0.0
	return date, source, cnt, percentage

def parse_inconsistent_prefix(line):
	date, source, both_covered, consistent, inconsistent = line.split(',')[:5]
	try:
		cnt = int(inconsistent)
		percentage = float(inconsistent) / float(both_covered) * 100.0 if int(both_covered) != 0 else 0.0
	except:
		print(line)
		cnt =  0
		percentage = 0.0
	return date, 'ALL-IRR', cnt, percentage

def parse_inconsistent_bgp(line):
	date, total, covered, inconsistent, consistent = line.split(',')[:5]
	try:
		cnt = int(inconsistent)
		percentage = float(inconsistent) / float(covered) * 100.0 if int(covered) != 0 else 0.0
	except:
		print(line)
		cnt =  0
		percentage = 0.0
	return date, 'ALL-IRR', cnt, percentage

def get_coverage_value(values, date):
	_, vrp_coverage = values['VRP'].get(date, (0, 0.0))
	_, irr_coverage = values['ALL-IRR'].get(date, (0, 0.0))
	return vrp_coverage, irr_coverage

def get_inconsistent_value(values, date):
	cnt, percentage = values['ALL-IRR'].get(date, (0, 0.0))
	return percentage, cnt

def load_values(indir, infiles, values, parse_func, latestdate):
	newlatestdate = latestdate
	dates = set()
	for infile in infiles:
		with open(indir + infile, 'r') as fin:
			for line in fin:
				date, source, cnt, percentage = parse_func(line)
				if date <= latestdate: continue
				if source in values:
					dates.add(date)
					values[source][date] = (cnt, percentage)
					if date > newlatestdate: newlatestdate = date
	return newlatestdate, sorted(list(dates))

def write_values(outdir, target, newlatestdate, dates, values, column_names, get_func):
	with open(outdir + '{}_{}.csv'.format(target, newlatestdate), 'w') as fout:
		fout.write(','.join(column_names) + '\n')
		for date in dates:

			fout.write('{},{},{}\n'.format(date, *get_func(values, date)))


def merge_output(indir, outdir, target, values, column_names, parse_func, get_func):
	print(target)

	indir = indir + '{}/'.format(target)
	infiles, latestdate = get_infiles_and_latestdate(indir, outdir, target)

	if len(infiles) <= 0:
		return

	print(infiles)

	newlatestdate, dates = load_values(indir, infiles, values, parse_func, latestdate)

	print(values)
	write_values(outdir, target, newlatestdate, dates, values, column_names, get_func)
	

def main():
	indir = '/home/mhkang/rpki-irr/outputs/analysis/'
	outdir = '/net/data/irrs/webdata/latest/'

	target = 'ip-coverage'
	values = {'ALL-IRR':{}, 'VRP':{}}
	column_names = ['date','RPKI','IRR']
	parse_func = parse_coverage
	get_func = get_coverage_value
	merge_output(indir, outdir, target, values, column_names, parse_func, get_func)
	
	target = 'as-coverage'
	values = {'ALL-IRR':{}, 'VRP':{}}
	column_names = ['date','RPKI','IRR']
	parse_func = parse_coverage
	get_func = get_coverage_value
	merge_output(indir, outdir, target, values, column_names, parse_func, get_func)
	

	target = 'inconsistent-prefix'
	values = {'ALL-IRR':{}}
	column_names = ['date','IRR(%)','IRR(#)']
	parse_func = parse_inconsistent_prefix
	get_func = get_inconsistent_value
	merge_output(indir, outdir, target, values, column_names, parse_func, get_func)

	target = 'inconsistent-bgp'
	values = {'ALL-IRR':{}}
	column_names = ['date','IRR(%)','IRR(#)']
	parse_func = parse_inconsistent_bgp
	get_func = get_inconsistent_value
	merge_output(indir, outdir, target, values, column_names, parse_func, get_func)

if __name__ == '__main__':
	main()