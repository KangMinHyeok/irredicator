import os
import sys
import pandas as pd

sys.path.append('/home/mhkang/irredicator')
from utils.utils import ip2binary
from utils.score import *
from model.dataset import Dataset
from utils.binaryPrefixTree import BinaryPrefixTree

def to_binary_prefix(prefix):
    prefix_addr, prefix_len = prefix.split('/')
    prefix_len = int(prefix_len)
    binary_prefix = ip2binary(prefix_addr, prefix_len)
    return binary_prefix

def load_prediction(result_file):
    prediction = pd.read_csv(result_file, sep='\t')
    prediction = prediction[prediction['record_type'] != 'inactive']
    prediction = prediction[prediction['validation'] == 'unknown']
    
    prediction['binary_prefix'] = prediction['prefix'].apply(to_binary_prefix)
    
    return prediction

def load_transfer_logs(transfer_log):
    names = ['date', 'prefix_addr', 'prefix_len', 'from_RIR', 'from_isp', 'to_RIR', 'to_isp', 'transfer_type']
    transfer = pd.read_csv(transfer_log, sep='\t', names=names)
    transfer['binary_prefix'] = transfer.apply(lambda x: ip2binary(x.prefix_addr, int(x.prefix_len)), axis=1)
    transfer = transfer[transfer['to_isp'] != 'None']
    transfer = transfer[transfer['from_isp'] != 'None']
    
    transfer_logs = BinaryPrefixTree()
    for log in transfer.values.tolist():
        binary_prefix = log[-1]
        record = log[:-1]
        transfer_logs.insert(binary_prefix, record)

    return transfer_logs


def load_active_origins(indirs):
    dataset = Dataset(indir=indirs)

    X, Y = dataset.get_data()
    X = X[X['uptime2weeks']>0.8]
    records = dataset.get_records()
    records = records.iloc[X.index]
    active_origins = {}
    origin2isp = {}
    for record in records.values.tolist():
        date, rir, prefix, origin, isp, sumRel, validation, source, record_type = record

        if prefix not in active_origins:
            active_origins[prefix] = set()
        
        active_origins[prefix].add(origin)
        

        if origin not in origin2isp:
            origin2isp[origin] = isp

    return active_origins, origin2isp

def exist_other_active_origin(active_origins, origin2isp, prefix, origin, isp):
    if prefix not in active_origins: 
        return False

    for active_origin in active_origins[prefix]:
        if active_origin == origin:
            continue
        if origin2isp.get(active_origin, None) == isp:
            continue

        return True

    return False

def evaluate(prediction, transfer_logs, active_origins, origin2isp):
    y_true = []
    y_pred = []
    results = []
    for record in prediction.values.tolist():
        binary_prefix = record[-1]
        date, rir, prefix, origin, isp, sumRel, validation, source, record_type, score0, score1, binary_prefix = record

        logs = transfer_logs.get_records(binary_prefix)

        logs = sorted(logs, key=lambda x:x[0], reverse=True)
        for log in logs:
            transferred_date, prefix_addr, prefix_len, from_RIR, from_isp, to_RIR, to_isp, transfer_type = log
            if from_isp == to_isp: continue

            if isp == to_isp:
                if exist_other_active_origin(active_origins, origin2isp, prefix, origin, isp):
                    continue
                else:
                    y_pred.append((score0, score1))
                    y_true.append(1)
                    results.append((date, source, rir, prefix, origin, isp, transferred_date, from_isp, to_isp, score0, score1, 1))
                    break
            
            elif isp == from_isp:
                y_pred.append((score0, score1))
                y_true.append(0)
                results.append((date, source, rir, prefix, origin, isp, transferred_date, from_isp, to_isp, score0, score1, 0))
                break

            break

    results = pd.DataFrame(results, columns=['date', 'source', 'rir', 'prefix_len', 'origin', 'isp', 'transferred_date', 'from_isp', 'to_isp', 'score0', 'score1', 'label'])
    
    return y_true, y_pred, results

def main():
    prediction_file = '/home/mhkang/evaluation/rpki_performance/records_0.tsv'
    transfer_file = '/home/mhkang/transfers/tsv/total.tsv'
    indirs = ['/home/mhkang/irrs/bgp-features-final/', '/home/mhkang/radb/bgp-features-final/']

    outdir = '/home/mhkang/evaluation/transfer_log_performance/'
    score_file = outdir + 'score.csv'
    result_file = outdir + 'record.csv'

    os.makedirs(outdir, exist_ok=True)

    prediction = load_prediction(prediction_file)
    transfer_logs = load_transfer_logs(transfer_file)

    active_origins, origin2isp = load_active_origins(indirs)

    y_true, y_pred, results = evaluate(prediction, transfer_logs, active_origins, origin2isp)
    
    save_score(score_file, y_true, y_pred)

    results.to_csv(result_file, sep='\t', header=True, index=False)
    
if __name__ == '__main__':
    main()