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

from utils.utils import get_date
from utils.score import *
from model.dataset import Dataset
from model.model import Model

seed_value = 940124
    
def apply(train_dir, outdir, params=None
    , init_points=2, n_iter=3
    , save_model=False, target_date='20230301'
    , load_model=False, model_file=''):
    
    if model_file == '':
        model_file=outdir + 'models/{}.pkl'.format(target_date)
    score_file = outdir + 'scores/{}.csv'.format(target_date)
    record_file = outdir + 'records/{}.tsv'.format(target_date)

    print('init dataset')
    dataset = Dataset(indir=[train_dir], target_date=target_date)

    print('make model')
    model = Model(outdir=outdir, params=params)

    pbounds = {
        'rejection_cost': (0.1, 0.49), 
        'learning_rate': (0.001, 0.1),
        'num_leaves': (128, 256),
        'num_iterations': (800, 1600),
    }
    
    tune_args = {
        'pbounds': pbounds,
        'random_state': seed_value,
        'init_points': init_points,
        'n_iter':n_iter
    }
    
    print('load dataset')
    dataset.load_dataset(label_flipping=False)
    print('get_rpki_covered')
    covered = dataset.get_rpki_covered()
    X, Y = covered
    if len(X) == 0:
        print('empty data')
        return None
    print('tune params')
    if params is None:
        params = model.tune_params(dataset.get_rpki_covered(split=True, num_chunks=5), tune_args, date=target_date)
    
    covered = dataset.get_rpki_covered()

    if load_model:
        print('load model')
        model.load_model(model_file=model_file)
    else:
        print('train model')
        model.train(covered)
    
        if save_model:
            model.save_model(model_file=model_file)

    print('predict')
    X, Y = covered
    Y_pred = model.predict(X)
    save_score(score_file, Y, Y_pred)

    print('predict all')
    X_all, Y_all = dataset.get_data()
    index = X_all.index.values.tolist()
    Y_pred_all = model.predict(X_all)
    dpreds = pd.DataFrame(Y_pred_all, columns=['score0', 'score1'], index=index)

    records = dataset.get_records()

    print('join records')
    records = records.join(dpreds, how='left')
    
    print('save records')
    records.to_csv(record_file, sep='\t', header=True, index=False)

    return model_file

def apply_irredicator(outdir, train_dir, save_model, start=None, end=None):
    os.makedirs(outdir, exist_ok=True)
    os.makedirs(outdir + 'scores/', exist_ok=True)
    os.makedirs(outdir + 'models/', exist_ok=True)
    os.makedirs(outdir + 'records/', exist_ok=True)
    os.makedirs(outdir + 'params/', exist_ok=True)

    infiles = list(filter(lambda x: x.endswith('tsv'), os.listdir(train_dir)))
    target_dates = list(map(get_date, infiles))
    # print(target_dates)
    outfiles = list(filter(lambda x: x.endswith('tsv'), os.listdir(outdir + '/records')))
    latest_date = '20110101'

    curr_dates = sorted(list(map(get_date, outfiles)), key = lambda x: int(x))
    if len(curr_dates) > 0:
        target_dates = set(target_dates) - set(curr_dates)
        target_dates = list(target_dates)
    target_dates = list(filter(lambda x: int(x) > int(latest_date), target_dates))
    if start is not None:
        target_dates = list(filter(lambda x: int(x) >= int(start), target_dates))
    if end is not None:
        target_dates = list(filter(lambda x: int(x) < int(end), target_dates))
    target_dates = sorted(target_dates, key=lambda x: int(x))
    # print(target_dates)
    # print(target_dates[0], target_dates[-1])
    # exit()
    # print(target_dates)
    target_months = sorted(list(set(map(lambda x: x[:6], target_dates))), key=lambda x: int(x))
    for target_month in target_months:
        curr_target_dates = sorted(list(filter(lambda x: x.startswith(target_month), target_dates)), key=lambda x: int(x))
        model_file = None
        for target_date in curr_target_dates:
            print("date: {}".format(target_date))
            if model_file is None:
                model_file = apply(train_dir, outdir, params=None, save_model=True, target_date=target_date)
            else:
                apply(train_dir, outdir, params=None, save_model=False, target_date=target_date, load_model=True, model_file=model_file)

def main():
    parser = argparse.ArgumentParser(description='apply_irredicator\n')
    parser.add_argument('--outdir', type=str, default='/home/mhkang/rpki-irr/outputs/irredicator/')
    parser.add_argument('--train_dir', type=str, default='/net/data/irrs/bgp-feature/')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--save_model', default=True)

    args = parser.parse_args()

    apply_irredicator(args.outdir, args.train_dir, args.save_model, start=args.start, end=args.end)

if __name__ == '__main__':
    random.seed(seed_value)
    np.random.seed(seed_value)
    main()


# python3.8 apply_irredicator.py --start 20230307 --end 20230701
# python3.8 apply_irredicator.py --start 20230701 --end 20231101
# python3.8 apply_irredicator.py --start 20231101 --end 20240301
# python3.8 apply_irredicator.py --start 20240301