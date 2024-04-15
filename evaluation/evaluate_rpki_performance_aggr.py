import os
import sys 
import json
import random
import argparse

import numpy as np
import pandas as pd
from datetime import *
from sklearn import metrics
import itertools
import shap
import skimage
import matplotlib.pyplot as plt
import time as tm
from sklearn.model_selection import train_test_split
from bayes_opt import BayesianOptimization

sys.path.append('/home/mhkang/rpki-irr/irredicator')

from utils.utils import *
from utils.score import *
from model.dataset import Dataset
from model.model import Model

seed_value = 940124
    
def evaluate(train_dirs, outdir, params=None
    , init_points=2, n_iter=3, suffix=''
    , save_model=False, model_file=None):
    
    score_file = outdir + 'score{}.csv'.format(suffix)
    result_file = outdir + 'records{}.tsv'.format(suffix)

    dataset = Dataset(indir=train_dirs)

    model = Model(outdir=outdir, params=params)

    pbounds = {
        'rejection_cost': (0.1, 0.49), 
        'learning_rate': (0.001, 0.1),
        'num_leaves': (128, 256),
        # 'num_iterations': (1000, 4000),
        'num_iterations': (10, 40),
    }
    
    tune_args = {
        'pbounds': pbounds,
        'random_state': seed_value,
        'init_points': init_points,
        'n_iter':n_iter
    }
    
    dataset.load_dataset(label_flipping=False)
    X, Y = dataset.get_rpki_covered(split=True, num_chunks=5)

    if params is None:
        params = model.tune_params(dataset.get_rpki_covered(split=True, num_chunks=5), tune_args)
    
    train, test = dataset.get_rpki_covered_train_test()
    model.train(train)
    if save_model:
        model.save_model(model_file=model_file)
    X, Y = dataset.get_rpki_covered()
    X_train, Y_train = train
    X_test, Y_test = test
    Y_pred = model.predict(X_test)

    save_score(score_file, Y_test, Y_pred)

    X_all, Y_all = dataset.get_data()
    index = X_all.index.values.tolist()
    Y_pred_all = model.predict(X_all)
    dpreds = pd.DataFrame(Y_pred_all, columns=['score0', 'score1'], index=index)

    data, record = dataset.load_dataset(label_flipping=False)
    aggr_pred = dpreds.join(record, how='left')
    print(aggr_pred)

    for record in aggr_pred.values.tolist():
         socre0, score1, date, rir, prefix_addr, prefix_len, origin, isp, sumRel, validation, source, record_type = record


    records = dataset.get_records()
    records = records.join(dpreds, how='left')
    
    records.to_csv(result_file, sep='\t', header=True, index=False)

    return params

def evaluate_rpki_performance_aggr(outdir, train_dirs, save_model):
    os.makedirs(outdir, exist_ok=True)

    params = None
    params = evaluate(train_dirs, outdir
        , params=params, save_model=save_model, model_file=outdir + 'model.pkl')

def main():
    parser = argparse.ArgumentParser(description='get vrp\n')
    parser.add_argument('--outdir', type=str, default='/home/mhkang/rpki-irr/outputs/evaluation/rpki_performance_aggr/')
    parser.add_argument('--train_dirs', nargs='+', type=str, default=['/home/mhkang/irrs/bgp-features-final/', '/home/mhkang/radb/bgp-features-final/'])
    parser.add_argument('--save_model', default=True)

    args = parser.parse_args()

    evaluate_rpki_performance_aggr(args.outdir, args.train_dirs, args.save_model)

if __name__ == '__main__':
    random.seed(seed_value)
    np.random.seed(seed_value)
    main()

