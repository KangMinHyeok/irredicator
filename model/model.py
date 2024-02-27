import os
import sys 
import json
import random
import pickle
import argparse

import numpy as np
import pandas as pd
import xgboost as xgb
import lightgbm as lgb
from datetime import *
from sklearn import metrics
import itertools
import shap
import skimage
import matplotlib.pyplot as plt
import time as tm
from sklearn.model_selection import train_test_split
from bayes_opt import BayesianOptimization

sys.path.append('/home/mhkang/irredicator')
from model.loss import sigmoid, softplus
from utils.score import *

class Model:

    def __init__(self, params=None, outdir='./'):
        self.params = params
        self.outdir = outdir
        
    def cross_validate(self, params):

        assert self.tune_data is not None, "tune_data is unavailable"
            
        X, Y = self.tune_data

        assert len(X) == len(Y), "the lengths of X and Y sould be the same: len(X)={}, len(Y)={}".format(len(X), len(Y))

        aucs = []
        k = len(X)
        for i in range(k):
            x_validation, y_validation = X[i], Y[i]
            x_train, y_train = [], []
            for j in range(k):
                x_train.append(X[j])
                y_train.append(Y[j])
            x_train = pd.concat(x_train)
            y_train = pd.concat(y_train)

            train_data = (x_train, y_train)
            self.model = self.train(train_data, params)
            y_pred = self.predict(x_validation)

            auc_all, auc_ex = get_AUC(y_validation, y_pred)
            aucs.append(auc_ex)

        return np.mean(aucs)

    def tune_target(self, rejection_cost, num_leaves, learning_rate, num_iterations):
        num_leaves = int(num_leaves)
        num_iterations = int(num_iterations)
        params = {
            'objective': 'multiclass',
            'num_leaves': num_leaves, 
            'learning_rate': learning_rate,
            'num_iterations': num_iterations,
            'num_class': 2,
            'rejection_cost': rejection_cost,
            'verbose': -1,
        }
        
        auc = self.cross_validate(params)
        
        if self.ftune is None:
            self.ftune = open(self.tune_file, 'w')
            keys = sorted(params.keys())
            self.ftune.write('{},{}\n'.format('auc', ','.join(keys)))
        
        values = list(map(lambda x: str(x[1]), sorted(params.items(), key=lambda item: item[0])))
        self.ftune.write('{},{}\n'.format(auc, ','.join(values)))
        
        return auc


    def tune_params(self, tune_data, tune_args):
        self.tune_data = tune_data

        pbounds = tune_args.get('pbounds')
        random_state = tune_args.get('random_state')
        init_points = tune_args.get('init_points')
        n_iter = tune_args.get('n_iter')
        self.tune_file = self.outdir + 'candidate_parameters.csv'
        self.ftune = None
        bayes_optimizer = BayesianOptimization(f=self.tune_target, pbounds=pbounds, random_state=random_state)
        bayes_optimizer.maximize(init_points=init_points, n_iter=init_points)
        
        if self.ftune is not None:
            self.ftune.close()

        params = self.load_params()
        self.params = params
        return params

    def train(self, train_data, params=None, tune_file=None):
        if params is None:
            if tune_file is not None:
                params = self.load_params(tune_file=tune_file)
            elif self.params is not None:
                params = self.params
            else:
                print('param is unavailable')
                return None

        X_train, Y_train = train_data
        dtrain = lgb.Dataset(X_train, Y_train)

        self.rejection_cost = params.get('rejection_cost')
        self.model = lgb.train(params,
                dtrain,
                valid_sets=dtrain,
                fobj=self.obj,
                feval=self.eval,
                early_stopping_rounds=100,
                )

        return self.model


    def obj(self, z, data):
        t = data.get_label()
        len_t = len(t)
        z0, z1 = z[:len_t], z[len_t:]
        neg_y0 = sigmoid(-z0)
        y1 = sigmoid(z1)
        grad0 = self.rejection_cost * (t - neg_y0) 
        grad1 = self.rejection_cost * (y1 - t) 
        hess0 = self.rejection_cost * (neg_y0 * (1 - neg_y0)) 
        hess1 = self.rejection_cost * (y1 * (1 - y1)) 
        grad = np.concatenate((grad0, grad1))
        hess = np.concatenate((hess0, hess1))
        return grad, hess

    def _loss0(self, z, t):
        return (1-t) * softplus(-z) + t * softplus(z)

    def _loss1(self, z, t):
        return t * softplus(-z) + (1 - t) * softplus(z)

    def eval(self, z, data):
        t = data.get_label()
        len_t = len(t)
        z0, z1 = z[:len_t], z[len_t:]
        loss0 = self.rejection_cost * self._loss0(z0, t) + (1 - self.rejection_cost) * self._loss0(-z1, t)
        loss1 = self.rejection_cost * self._loss1(z1, t) + (1 - self.rejection_cost) * self._loss1(-z0, t)
        loss = np.concatenate((loss0, loss1))
        return 'cs', loss.mean(), False

    def save_model(self, model_file=None):
        if model_file is not None:
            self.model_file = model_file
        else:
            param_list = list(map(lambda x: str(x[1]), sorted(self.params.items(), key=lambda item: item[0])))
            param_list = '_'.join(param_list)

            self.model_file = self.outdir + 'model_{}.pkl'.format(param_list)
        with open(self.model_file, 'wb') as fout:
            pickle.dump(self.model, fout)

    def load_model(self, model_file=None):
        if model_file is None:
            
            if self.model_file is None:
                if self.params is None: 
                    print('model file is not specified')
                    return None
                param_list = list(map(lambda x: str(x[1]), sorted(self.params.items(), key=lambda item: item[0])))
                param_list = '_'.join(param_list)

                self.model_file = self.outdir + 'model_{}.pkl'.format(param_list)
            model_file = self.model_file

        with open(model_file, 'rb') as fin:
            self.model = pickle.load(fin)

        return self.model

    def load_params(self, tune_file=None, metric='auc'):
        if tune_file is None:
            tune_file = self.tune_file

        if tune_file is None:
            print('Unable to read parameters: file is not specified')
            return None
        
        cadidates= pd.read_csv(tune_file)
        params = cadidates.iloc[cadidates[metric].idxmax()].to_dict()
        params.pop(metric, None)
        return params

    def predict(self, X_test):
        
        prediction = self.model.predict(X_test)

        return prediction

