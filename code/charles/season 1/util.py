#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: util.py
# Date: 2016-10-24
# Author: Chaos <xinchaoxt@gmail.com>

import pandas as pd
from sklearn import metrics
import numpy as np
from config import *


def sigmoid(x, a=60, b=30):
    return 1.0 / (1 + np.exp(-a * x + b))


def split_map(x, a=0.3, b=0.7):
    return 0. if x < a else 1. if x > b else x


def extract_features(infile, degree=0.95):
    df = pd.read_csv(infile)
    obj = df.to_dict()
    fscores = obj['fscore']
    tot = sum(fscores.values())
    res = list()
    acc = 0
    for k in fscores.keys()[::-1]:
        acc += fscores[k]
        if float(acc) / float(tot) > degree:
            break
        res.append(obj['feature'][k])
    print len(res)
    print res


def calc_auc(df):
    y_true = df['Label'].values
    y_pred = df[probability_consumed_label].values
    auc = metrics.roc_auc_score(np.array(y_true), np.array(y_pred))
    return pd.DataFrame({coupon_label: [df[coupon_label][0]], 'auc': [auc]})


if __name__ == '__main__':
    extract_features('{0}_{1}{2}'.format(model_path, '2016110105PM56', model_feature_importance_csv))

