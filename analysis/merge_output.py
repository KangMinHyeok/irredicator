import os

def get_latestdate(outdir, targets, intargets):
	latestdate = None
	outfiles = os.listdir(outdir)
	for target, intarget in zip(targets, intargets):
		currfiles = list(filter(lambda x: x.startswith(target), outfiles))
		if len(currfiles) > 0:
			curr_latestdate = max(list(map(lambda x: x.split('.')[0].split('_')[-1], currfiles)))
		else:
			curr_latestdate = '20110101'	
		if latestdate is None:
			latestdate = curr_latestdate
		elif curr_latestdate < latestdate:
			latestdate = curr_latestdate

	return latestdate

def get_newlatestdate(indir, targets, intargets):
	latestdate = None
	infiles = os.listdir(indir)
	for target, intarget in zip(targets, intargets):
		
		if intarget != None:
			curr_infiles = list(filter(lambda x: x.startswith(intarget), infiles))
		else:
			curr_infiles = list(filter(lambda x: x.startswith(target), infiles))
		curr_dates = list(map(lambda x: x.split('.')[0].split('-')[-1] , curr_infiles))
		curr_dates = list(filter(lambda x: x > latestdate, curr_dates))
		curr_latestdate = max(curr_dates)
		
		if latestdate is None:
			latestdate = curr_latestdate
		elif curr_latestdate < latestdate:
			latestdate = curr_latestdate

	return latestdate


def get_infiles(indir, latestdate, target, intarget=None):
	
	infiles = os.listdir(indir)
	if intarget != None:
		infiles = list(filter(lambda x: x.startswith(intarget), infiles))
	else:
		infiles = list(filter(lambda x: x.startswith(target), infiles))
	
	infiles = list(filter(lambda x: x.split('.')[0].split('-')[-1] > latestdate, infiles))
	return infiles

def parse_coverage(line):
	date, source, cnt, total = line.split(',')[:4]
	try:
		cnt = int(cnt)
		percentage = float(cnt) / float(total) * 100.0 if int(total) != 0 else 0.0
	except Exception as e:
		print(e)
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
	except Exception as e:
		print(e)
		print(line)
		cnt = 0
		percentage = 0.0
	return date, 'ALL-IRR', cnt, percentage


def parse_bgp_coverage(line):
	date, rir, source, total, covered, valid = line.split(',')[:6]
	try:
		cnt = int(covered)
		percentage = float(covered) / float(total) * 100.0 if int(total) != 0 else 0.0
	except Exception as e:
		print(e)
		print(line)
		cnt = 0
		percentage = 0.0
	return date, source, cnt, percentage

def parse_bgp_valid(line):
	date, rir, source, total, covered, valid = line.split(',')[:6]
	try:
		cnt = int(valid)
		percentage = float(valid) / float(covered) * 100.0 if int(covered) != 0 else 0.0
	except:
		print(line)
		cnt = 0
		percentage = 0.0
	return date, source, cnt, percentage

def get_coverage_value(values, date):
	_, vrp_coverage = values['VRP'].get(date, (0, 0.0))
	_, irr_coverage = values['ALL-IRR'].get(date, (0, 0.0))
	return vrp_coverage, irr_coverage

def get_inconsistent_value(values, date):
	cnt, percentage = values['ALL-IRR'].get(date, (0, 0.0))
	return percentage, cnt

def load_values(indir, infiles, values, parse_func, latestdate, newlatestdate):
	dates = set()
	for infile in infiles:
		with open(indir + infile, 'r') as fin:
			for line in fin:
				date, source, cnt, percentage = parse_func(line)
				if date <= latestdate: continue
				if date >= newlatestdate: continue
				if source in values:
					dates.add(date)
					values[source][date] = (cnt, percentage)
	return sorted(list(dates))

def write_values(outdir, target, newlatestdate, dates, values, column_names, get_func):
	with open(outdir + '{}_{}.csv'.format(target, newlatestdate), 'w') as fout:
		fout.write(','.join(column_names) + '\n')
		for date in dates:

			fout.write('{},{},{}\n'.format(date, *get_func(values, date)))


def merge_output(indir, outdir, latestdate, newlatestdate, target, values, column_names, parse_func, get_func, intarget=None):
	print(target)

	if intarget != None:
		indir = indir + '{}/'.format(intarget)
	else:
		indir = indir + '{}/'.format(target)
	
	infiles = get_infiles(indir, latestdate, target, intarget=intarget)
	if len(infiles) <= 0:
		return

	# print(infiles)

	dates = load_values(indir, infiles, values, parse_func, latestdate, newlatestdate)

	# print(values)
	write_values(outdir, target, newlatestdate, dates, values, column_names, get_func)
	

def main():
	indir = '/home/mhkang/rpki-irr/outputs/analysis/'
	outdir = '/net/data/irrs/webdata/latest/'

	targets = [
		'ip-coverage', 'as-coverage',
		'bgp-coverage', 'bgp-valid',
		'inconsistent-prefix', 'inconsistent-bgp'
		]
	intargets = [
		None, None,
		None, 'bgp-coverage',
		None, None
	]	

	latestdate = get_latestdate(outdir, targets, intargets)
	newlatestdate = get_newlatestdate(indir, targets, intargets)

	def get_args(target):
		values, column_names, parse_func, get_func = None, None, None, None
		if target == 'ip-coverage':
			values = {'ALL-IRR':{}, 'VRP':{}}
			column_names = ['date','RPKI','IRR']
			parse_func = parse_coverage
			get_func = get_coverage_value
		elif target == 'as-coverage':
			values = {'ALL-IRR':{}, 'VRP':{}}
			column_names = ['date','RPKI','IRR']
			parse_func = parse_coverage
			get_func = get_coverage_value
		elif target == 'bgp-coverage':
			values = {'ALL-IRR':{}, 'VRP':{}}
			column_names = ['date','RPKI','IRR']
			parse_func = parse_bgp_coverage
			get_func = get_coverage_value
		elif target == 'bgp-valid':
			values = {'ALL-IRR':{}, 'VRP':{}}
			column_names = ['date','RPKI','IRR']
			parse_func = parse_bgp_valid
			get_func = get_coverage_value
		elif target == 'inconsistent-prefix':
			values = {'ALL-IRR':{}}
			column_names = ['date','IRR(%)','IRR(#)']
			parse_func = parse_inconsistent_prefix
			get_func = get_inconsistent_value
		elif target == 'inconsistent-bgp':
			values = {'ALL-IRR':{}}
			column_names = ['date','IRR(%)','IRR(#)']
			parse_func = parse_inconsistent_bgp
			get_func = get_inconsistent_value

		return values, column_names, parse_func, get_func

	for target, intarget in zip(targets, intargets):
		values, column_names, parse_func, get_func = get_args(target)
		merge_output(indir, outdir, latestdate, newlatestdate, target, values, column_names, parse_func, get_func, intarget=intarget)

	# target = 'ip-coverage'
	# values = {'ALL-IRR':{}, 'VRP':{}}
	# column_names = ['date','RPKI','IRR']
	# parse_func = parse_coverage
	# get_func = get_coverage_value
	# merge_output(indir, outdir, target, values, column_names, parse_func, get_func)
	
	# target = 'as-coverage'
	# values = {'ALL-IRR':{}, 'VRP':{}}
	# column_names = ['date','RPKI','IRR']
	# parse_func = parse_coverage
	# get_func = get_coverage_value
	# merge_output(indir, outdir, target, values, column_names, parse_func, get_func)
	
	# target = 'bgp-coverage'
	# values = {'ALL-IRR':{}, 'VRP':{}}
	# column_names = ['date','RPKI','IRR']
	# parse_func = parse_bgp_coverage
	# get_func = get_coverage_value
	# merge_output(indir, outdir, target, values, column_names, parse_func, get_func)

	# target = 'bgp-valid'
	# values = {'ALL-IRR':{}, 'VRP':{}}
	# column_names = ['date','RPKI','IRR']
	# parse_func = parse_bgp_valid
	# get_func = get_coverage_value
	# merge_output(indir, outdir, target, values, column_names, parse_func, get_func, intarget='bgp-coverage')

	# target = 'inconsistent-prefix'
	# values = {'ALL-IRR':{}}
	# column_names = ['date','IRR(%)','IRR(#)']
	# parse_func = parse_inconsistent_prefix
	# get_func = get_inconsistent_value
	# merge_output(indir, outdir, target, values, column_names, parse_func, get_func)

	# target = 'inconsistent-bgp'
	# values = {'ALL-IRR':{}}
	# column_names = ['date','IRR(%)','IRR(#)']
	# parse_func = parse_inconsistent_bgp
	# get_func = get_inconsistent_value
	# merge_output(indir, outdir, target, values, column_names, parse_func, get_func)

if __name__ == '__main__':
	main()