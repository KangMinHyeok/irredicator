import os

def get_start_date(outdir, targets, intargets):
	start_date = None
	outfiles = os.listdir(outdir)
	for target, intarget in zip(targets, intargets):
		currfiles = list(filter(lambda x: x.startswith(target), outfiles))
		if len(currfiles) > 0:
			curr_date = max(list(map(lambda x: x.split('.')[0].split('_')[-1], currfiles)))
		else:
			curr_date = '20110101'	
		if start_date is None:
			start_date = curr_date
		elif curr_date < start_date:
			start_date = curr_date

	return start_date

def get_end_date(indirs, targets, intargets, start_date):
	end_date = None
	for indir, target, intarget in zip(indirs, targets, intargets):
		infiles = os.listdir(indir)
		if intarget != None:
			curr_infiles = list(filter(lambda x: x.startswith(intarget), infiles))
		else:
			curr_infiles = list(filter(lambda x: x.startswith(target), infiles))
		curr_dates = list(map(lambda x: x.split('.')[0].split('-')[-1] , curr_infiles))
		curr_dates = list(filter(lambda x: x > start_date, curr_dates))
		curr_date = max(curr_dates)
		
		if end_date is None:
			end_date = curr_date
		elif curr_date < end_date:
			end_date = curr_date

	return end_date

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


def get_infiles(indir, start_date, target, intarget=None):
	
	infiles = os.listdir(indir)
	if intarget != None:
		infiles = list(filter(lambda x: x.startswith(intarget), infiles))
	else:
		infiles = list(filter(lambda x: x.startswith(target), infiles))
	
	infiles = list(filter(lambda x: x.split('.')[0].split('-')[-1] > start_date, infiles))
	return infiles

def load_values(indir, infiles, values, parse_func, start_date, end_date):
	dates = set()
	for infile in infiles:
		with open(indir + infile, 'r') as fin:
			for line in fin:
				date, source, cnt, percentage = parse_func(line)
				if date <= start_date: continue
				if date > end_date: continue
				if source in values:
					dates.add(date)
					values[source][date] = (cnt, percentage)
	return sorted(list(dates))

def write_values(outdir, target, end_date, dates, values, column_names, get_func):
	with open(outdir + '{}_{}.csv'.format(target, end_date), 'w') as fout:
		fout.write(','.join(column_names) + '\n')
		for date in dates:

			fout.write('{},{},{}\n'.format(date, *get_func(values, date)))


def merge_output(indir, outdir, start_date, end_date, target, values, column_names, parse_func, get_func, intarget=None):
	print(target)

	if intarget != None:
		indir = indir + '{}/'.format(intarget)
	else:
		indir = indir + '{}/'.format(target)
	
	infiles = get_infiles(indir, start_date, target, intarget=intarget)
	if len(infiles) <= 0:
		return

	dates = load_values(indir, infiles, values, parse_func, start_date, end_date)

	# print(values)
	write_values('./out/', target, end_date, dates, values, column_names, get_func)

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

	indirs = list(map(lambda x: indir + '{}/'.format(x[0] if x[1] is None else x[1]), zip(targets, intargets)))
	start_date = get_start_date(outdir, targets, intargets)
	end_date = get_end_date(indirs, targets, intargets, start_date)

	print(start_date, end_date)

	for target, intarget in zip(targets, intargets):
		values, column_names, parse_func, get_func = get_args(target)
		merge_output(indir, outdir, start_date, end_date, target, values, column_names, parse_func, get_func, intarget=intarget)

if __name__ == '__main__':
	main()