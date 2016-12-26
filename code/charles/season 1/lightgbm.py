#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: data_view.py
# Date: 2016-10-18
# Author: Chaos <xinchaoxt@gmail.com>

from pylightgbm.models import GBMRegressor
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import time
import os
from feature_extract import *
import json
import operator


def create_feature_map(features, fmap):
    outfile = open(fmap, 'w')
    for i, feat in enumerate(features):
        outfile.write('{0}\t{1}\tq\n'.format(i, feat))
    outfile.close()


if __name__ == '__main__':
    exec_time = time.strftime("%Y%m%d%I%p%M", time.localtime())

    os.mkdir('{0}_lgbm_{1}'.format(model_path, exec_time))
    os.mkdir('{0}_lgbm_{1}'.format(submission_path, exec_time))

    print 'get training data'
    train_features = pd.read_csv(train_path + 'train_features.csv').astype(float)
    train_labels = pd.read_csv(train_path + 'labels.csv').astype(float)

    validate_features = pd.read_csv(validate_path + 'train_features.csv').astype(float)
    validate_labels = pd.read_csv(validate_path + 'labels.csv').astype(float)

    predict_features = pd.read_csv(predict_path + 'train_features.csv').astype(float)

    train_features.fillna(0, inplace=True)
    validate_features.fillna(0, inplace=True)
    predict_features.fillna(0, inplace=True)

    create_feature_map(train_features.columns.tolist(), '{0}_lgbm_{1}{2}'.format(model_path, exec_time, model_fmap_file))

    print 'LightGBM Training'
    seed = 13
    gbmr = GBMRegressor(
        exec_path='/usr/local/lib/python2.7/site-packages/pylightgbm/LightGBM/lightgbm',
        config='',
        application='regression',
        num_iterations=500,
        learning_rate=0.1,
        tree_learner='serial',
        min_data_in_leaf=10,
        metric='auc',
        feature_fraction=0.7,
        feature_fraction_seed=seed,
        bagging_fraction=1,
        bagging_freq=10,
        bagging_seed=seed,
        metric_freq=1,
        early_stopping_round=50
    )
    json.dump(gbmr.param, open('{0}_lgbm_{1}{2}'.format(model_path, exec_time, model_params), 'wb+'))
    gbmr.fit(validate_features.values, validate_labels.values[:, 0], test_data=[(train_features.values, train_labels.values[:, 0])])

    importance = dict(gbmr.feature_importance(train_features.columns.tolist()))
    importance = sorted(importance.items(), key=operator.itemgetter(1))
    df = pd.DataFrame(gbmr.feature_importance(train_features.columns.tolist()), columns=['feature', 'importance'])
    df['importance'] = df['importance'] / df['importance'].sum()
    df.to_csv('{0}_lgbm_{1}{2}'.format(model_path, exec_time, model_feature_importance_csv), index=False)

    val_label = gbmr.predict(validate_features)
    val_frame = pd.Series(val_label, index=validate_features.index)
    val_frame.name = probability_consumed_label
    val_coupons = pd.read_csv(validate_path + 'dataset.csv')
    val_coupons = val_coupons.join(val_frame).join(val_frame.map(lambda x: 0. if x < 0.5 else 1.).rename('map')).join(pd.read_csv(validate_path + 'labels.csv')['Label'])
    val_coupons.to_csv('{0}_lgbm_{1}{2}'.format(model_path, exec_time, val_diff_file), index=False)
    print confusion_matrix(val_coupons['Label'], val_coupons['map'])

    print gbmr.best_round
    print 'generate submission'
    labels = gbmr.predict(predict_features)
    frame = pd.Series(labels, index=predict_features.index)
    frame.name = probability_consumed_label

    plt.figure()
    frame.hist(figsize=(10, 8))
    plt.title('results histogram')
    plt.xlabel('predict probability')
    plt.gcf().savefig('{0}_lgbm_{1}{2}'.format(submission_path, exec_time, submission_hist_file))

    submission = pd.read_csv(predict_path + 'dataset.csv')
    submission = submission[[user_label, coupon_label, date_received_label]].join(frame)
    submission.to_csv('{0}_lgbm_{1}{2}'.format(submission_path, exec_time, submission_file), index=False)

