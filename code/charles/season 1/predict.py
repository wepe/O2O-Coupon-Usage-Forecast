#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: data_view.py
# Date: 2016-10-21
# Author: Chaos <xinchaoxt@gmail.com>
import xgb as xgb
import matplotlib.pyplot as plt
import time
from add_feature import *


def get_predict_features(df, index, active=False, dis=0.):
    user_feature_data = cPickle.load(open(pickle_data_path + ('user_features' if not active else 'active_user_features'), 'rb'))
    merchant_feature_data = cPickle.load(open(pickle_data_path + ('merchant_features' if not active else 'active_merchant_features'), 'rb'))
    user_merchant_feature_data = cPickle.load(open(pickle_data_path + ('user_merchant_features' if not active else 'active_user_merchant_features'), 'rb'))

    df = df.merge(user_feature_data, on=user_label, how='left')
    df = df.merge(merchant_feature_data, on=merchant_label, how='left')
    df = df.merge(user_merchant_feature_data, on=[user_label, merchant_label], how='left')
    df = add_coupon_features(df)
    df = add_distance_rate(df, dis)

    df.fillna(-1, inplace=True)
    df.index = index

    return df

if __name__ == '__main__':
    tag = 'new_split'
    exec_time = time.strftime("%Y%m%d%I%p%M", time.localtime())

    print 'get predict data'
    raw_data = pd.read_csv(offline_test_file_path)
    print 'raw data shape:', raw_data.shape

    offline_users = cPickle.load(open(offline_users_path, 'rb'))
    active_users = cPickle.load(open(active_users_path, 'rb'))
    offline_data = raw_data[raw_data[user_label].isin(offline_users)]
    active_data = raw_data[raw_data[user_label].isin(active_users)]
    other_data = raw_data[~raw_data[user_label].isin(active_users | offline_users)]

    print 'other users:', set(other_data[user_label].tolist())

    offline_data = get_predict_features(offline_data, dis=2.41930680506, index=offline_data.index)

    print 'predict offline data shape: ', offline_data.iloc[:, 6:].shape
    predict_data = xgb.DMatrix(offline_data.iloc[:, 6:].values, missing=-1)

    print 'init and load model'
    param = {
        'max_depth': 5,
        'eta': 0.1,
        'silent': 1,
        'seed': 13,
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'scale_pos_weight': 2,
        'subsample': 1,
        'colsample_bytree': 0.7,
        'min_child_weight': 100,
        'max_delta_step': 20
    }
    model = xgb.Booster(params=param)
    model.load_model(offline_model_file_path)

    print 'predicting'
    offline_labels = model.predict(predict_data)
    offline_frame = pd.Series(offline_labels, index=offline_data.index)
    offline_frame.name = probability_consumed_label
    offline_data = offline_data.iloc[:, :6].join(offline_frame)

    active_data = get_predict_features(active_data, active=True, dis=2.31082408425, index=active_data.index)
    print 'predict active data shape: ', active_data.iloc[:, 6:].shape
    predict_data = xgb.DMatrix(active_data.iloc[:, 6:].values, missing=-1)

    print 'init and load model'
    param = {
        'max_depth': 5,
        'eta': 0.1,
        'silent': 1,
        'seed': 13,
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'scale_pos_weight': 2,
        'subsample': 1,
        'colsample_bytree': 0.7,
        'min_child_weight': 100,
        'max_delta_step': 20
    }
    num_round = 400
    model = xgb.Booster(params=param)
    model.load_model(active_model_file_path)

    print 'predicting'
    active_labels = model.predict(predict_data)
    active_frame = pd.Series(active_labels, index=active_data.index)
    active_frame.name = probability_consumed_label
    active_data = active_data.iloc[:, :6].join(active_frame)

    print 'generate submission'
    offline_data = offline_data[[user_label, coupon_label, date_received_label, probability_consumed_label]]
    active_data = active_data[[user_label, coupon_label, date_received_label, probability_consumed_label]]
    raw_columns = [user_label, coupon_label, date_received_label]
    data = raw_data[raw_columns]
    print 'data shape:', data.shape
    combine_data = pd.concat([offline_data, active_data])
    result = data.join(combine_data[probability_consumed_label])
    print 'result shape:', result.shape
    result.fillna(result.mean(), inplace=True)

    frame = result[probability_consumed_label]
    plt.figure()
    frame.hist(figsize=(10, 8))
    plt.title('results histogram')
    plt.xlabel('predict probability')
    plt.gcf().savefig('{0}_{1}_{2}.png'.format(result_hist_path, exec_time, tag))

    result.to_csv('{0}_{1}_{2}.csv'.format(predict_data_path, exec_time, tag), index=False)

    # get predict data
    # raw data shape: (113640, 6)
    # other users: set([2495873, 1286474])
    # distance_rate_mean:  2.32226413348
    # predict offline data shape:  (49068, 51)
    # init and load model
    # predicting
    # distance_rate_mean:  2.33231096109
    # predict active data shape:  (64568, 86)
    # init and load model
    # predicting
    # generate submission
    # data shape: (113640, 3)
    # result shape: (113640, 4)
