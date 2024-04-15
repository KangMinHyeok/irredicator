import sys
import pandas as pd

from sklearn.model_selection import train_test_split

sys.path.append('/home/mhkang/irredicator')
from utils.utils import *

class Dataset:
    def __init__(self, infiles=None, indir=None, target_date='20230301', source='ALL-IRRs', 
                num_monitoring_windows=20):
        if infiles is None:
            if indir is not None:
                self.infiles = self.get_infiles(indir, target_date)
        else:
            self.infiles = infiles

        self.num_monitoring_windows = num_monitoring_windows
        self.max_monitoring_windows = 20
        
        self.data = None
        self.records = None
        self.rpki_covered_data = None
        
        self.feature_names = None
        pass

    def __del__(self):
        pass

    def get_infiles(self, indir, target_date):
        if type(indir) == list:
            infiles = []
            for d in indir:

                irrfiles = list(os.listdir(d))
                irrfiles = list(map(lambda x: d + x, irrfiles))

                infiles += irrfiles
        elif type(indir) == str:
            infiles = list(os.listdir(indir))
            infiles = list(map(lambda x: indir + x, infiles))
        else:
            print('Invalid indir type: must be str of list of str but the type is {}'.format(type(indir)))

        infiles = list(filter(lambda x: x.endswith('.tsv') and date_diff(get_date(x), target_date) == 0, infiles ))

        return infiles
    

    def get_feature_names(self):
        if self.feature_names is not None:
            return self.feature_names

        days = [1,3,5]
        weeks = [1,2,3]
        months = [1,3,6,9]
        years = [1,2,3,4,5,6,7,8,9,10]
        
        windows = []
        
        for day in days: windows.append(str(day) + 'days')
        for week in weeks: windows.append(str(week) + 'weeks')
        for month in months: windows.append(str(month) + 'months')
        for year in years: windows.append(str(year) + 'years')

        windows = windows[:self.num_monitoring_windows]

        metrics = [
            'uptime', 'lifespan', 'relativeUptime',
            'up', 'down',
            'activeDays.min', 'activeDays.max', 'activeDays.avg', 'activeDays.std',
            'inactiveDays.min', 'inactiveDays.max', 'inactiveDays.avg', 'inactiveDays.std',
            ]

        feature_names = []

        for metric in metrics:
            for window in windows:
                feature_names.append(metric + window)

        
        stats = ['.min', '.max', '.avg', '.std']

        for metric in metrics:
            for stat in stats:
                feature_names.append(metric + stat)
            
        
        self.feature_names = feature_names
        return feature_names

    def trim_features(self, features):
        newFeatures = []
        for i in range(13):
            newFeatures += features[i*self.max_monitoring_windows : i*self.max_monitoring_windows + self.num_monitoring_windows]

        newFeatures += features[13*self.max_monitoring_windows : ]
        return newFeatures

    def split(self, X, Y, k):
        data = pd.concat([X, Y], axis=1)
        
        dvalid = data[data['label'] == 1]
        dinvalid = data[data['label'] == 0]

        vchunk_size = dvalid.shape[0] // k
        ivchunk_size = dinvalid.shape[0] // k

        vchunks = []
        ivchunks = []
        vstart = 0
        vend = vchunk_size
        ivstart = 0
        ivend = ivchunk_size

        for i in range(k):
            if i == k-1:
                vchunks.append(dvalid.iloc[vstart:, :])
                ivchunks.append(dinvalid.iloc[ivstart:, :])
            else:
                vchunks.append(dvalid.iloc[vstart:vend, :])
                ivchunks.append(dinvalid.iloc[ivstart:ivend, :])
            
            vstart += vchunk_size
            vend += vchunk_size

            ivstart += ivchunk_size
            ivend += ivchunk_size

        chunks = list(map(lambda x: pd.concat(x), list(zip(vchunks, ivchunks))))
        chunks = list(map(lambda x: (x.iloc[:, :-1], x.iloc[:,-1]), chunks))
        X, Y = list(zip(*chunks))
        return X, Y

    def split_train_test(self, X_data, Y_data):
        data = pd.concat([X_data, Y_data], axis=1)
        
        valid = data[data.label == 1]
        invalid = data[data.label == 0]
        
        vtrain, vtest = train_test_split(valid, test_size=0.2)
        itrain, itest = train_test_split(invalid, test_size=0.2)
        
        train = pd.concat([vtrain, itrain])
        test = pd.concat([vtest, itest])

        X_train, Y_train = train.iloc[:,:-1], train.iloc[:,-1] 
        X_test, Y_test = test.iloc[:,:-1], test.iloc[:,-1]
        
        return (X_train, Y_train), (X_test, Y_test)

    def get_rpki_covered(self, split=False, num_chunks=5):
        X_covered, Y_covered = None, None

        if self.rpki_covered_data is None:
            data, records = self.load_dataset()

            covered  = data[data['record_type']=='covered']
            X_covered  = covered.iloc[:,:-2]
            Y_covered  = covered.iloc[:,-2]
            self.rpki_covered_data = (X_covered, Y_covered)
        else:
            X_covered, Y_covered = self.rpki_covered_data
        
        if split:
            X_covered, Y_covered = self.split(X_covered, Y_covered, num_chunks)

        return X_covered, Y_covered

    def get_rpki_covered_train_test(self):
        X_covered, Y_covered = self.get_rpki_covered()
        train, test = self.split_train_test(X_covered, Y_covered)
        return train, test

    def get_data(self):
        data, records = self.load_dataset()

        X_data  = data.iloc[:,:-2]
        Y_data  = data.iloc[:,-2]

        return X_data, Y_data

    def get_records(self):
        data, records = self.load_dataset()

        return records

    def load_dataset(self, label_flipping=False, bitvector_file=None):

        if self.data is not None:
            return self.data, self.records

        data = []
        records = []
        
        idx = 0

        for infile in self.infiles:
            if len(data) == 5000: break
            with open(infile, 'r') as fin:
                for line in fin:
                    idx += 1
                    if len(data) == 5000: break
                    tokens = line.replace('\n', '').split('\t')
                    date, prefix_addr, prefix_len, origin, isp, rir, validation, sumRel, source = tokens[:9]
                    # prefix = "{}/{}".format(prefix_addr, prefix_len)
                    prefix_len = int(prefix_len)
                    orgin = int(origin)
                    inactive = len(tokens) == 9
                    excluded = validation == 'invalid' and sumRel != 'none'
                    covered = sumRel != 'not-covered'

                    record_type = 'not-coverd'
                    if inactive: record_type = 'inactive'
                    elif excluded: record_type = 'excluded'
                    elif covered: record_type = 'covered'
                    
                    key = (prefix_addr, prefix_len, origin)
                    record = [idx, date, rir, prefix_addr, prefix_len, origin, isp, sumRel, validation, source]

                    if label_flipping:
                        record.append(key)
                    records.append(record + [record_type])

                    if inactive or excluded:
                        continue

                    label = 1 if validation == 'valid' else 0
                    features = list(map(float, tokens[9:]))
                    features = self.trim_features(features)
                    row = features + [label, record_type, idx]
                    if label_flipping:
                        row += [key, validation]

                    data.append(row)
        

        columns = self.get_feature_names() + ['label', 'record_type', 'idx']
        record_columns = [
            'idx', 'date', 'rir', 
            'prefix_addr', 'prefix_len', 'origin', 'isp', 
            'sumRel', 'validation', 'source', 'record_type'
        ]

        data = pd.DataFrame(data, columns=columns)
        data = data.set_index(['idx'])
        data = data.sample(frac=1)

        records = pd.DataFrame(records, columns=record_columns)
        records = records.set_index(['idx'])
        self.data = data
        self.records = records

        return data, records
