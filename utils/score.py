import sys
import numpy as np
from sklearn import metrics

sys.path.append('/home/mhkang/irredicator')
from model.loss import softmax
from utils.utils import filter_rejected

def write_score(score_file, predictions):
    with open(score_file, 'w') as fout:
        y_true = np.array(list(map(lambda x: x[0], predictions)))
        y_pred = np.array(list(map(lambda x: x[1], predictions)))
        precision, recall, thresholds = metrics.precision_recall_curve(y_true, y_pred)

        for p, r, t in zip(p, r, c):
            fout.write(','.join(list(map(str, [t, p, r, 2*p*r/(p+r)]))) + '\n')

def save_score(score_file, Y, Y_pred):
    predictions = list(zip(Y, Y_pred))

    predictions_w_reject = filter_rejected(predictions)
    predictions_w_reject = list(map(lambda x: (x[0], softmax(x[1])[1]), predictions_w_reject))
    
    with open(score_file, 'w') as fout:
        y_true = np.array(list(map(lambda x: x[0], predictions_w_reject)))
        y_pred = np.array(list(map(lambda x: x[1], predictions_w_reject)))
        precision, recall, thresholds = metrics.precision_recall_curve(y_true, y_pred)

        for p, r, t in zip(precision, recall, thresholds):
            f1_score = 2*p*r/(p+r)
            fout.write(','.join(list(map(str, [t, p, r, f1_score]))) + '\n')


def calc_AUC(predictions):
    predictions = list(map(lambda x: (x[0], softmax(x[1])[1]), predictions))

    y_true = np.array(list(map(lambda x: x[0], predictions)))
    y_pred = np.array(list(map(lambda x: x[1], predictions)))
    
    precision, recall, thresholds = metrics.precision_recall_curve(y_true, y_pred)
    
    return metrics.auc(recall, precision)

def get_AUC(Y, Y_pred, reject=True):
    predictions = list(zip(Y, Y_pred))
    auc_pr_all = calc_AUC(predictions)

    if reject:
        predictions_w_reject = filter_rejected(predictions)
        auc_pr_ex = calc_AUC(predictions_w_reject)
        return auc_pr_all, auc_pr_ex

    return auc_pr_all
