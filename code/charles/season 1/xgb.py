#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: xgb.py
# Date: 2016-10-21
# Author: Chaos <xinchaoxt@gmail.com>
from sklearn.metrics import confusion_matrix, roc_auc_score
from config import *
import numpy as np
import xgboost
import pandas as pd
import matplotlib.pyplot as plt
import time
import os
import sys
import json
import operator
# import itertools

save_stdout = sys.stdout


def calc_auc(df):
    coupon = df[coupon_label].iloc[0]
    y_true = df['Label'].values
    if len(np.unique(y_true)) != 2:
        auc = np.nan
    else:
        y_pred = df[probability_consumed_label].values
        auc = roc_auc_score(np.array(y_true), np.array(y_pred))
    return pd.DataFrame({coupon_label: [coupon], 'auc': [auc]})


def check_average_auc(df):
    grouped = df.groupby(coupon_label, as_index=False).apply(lambda x: calc_auc(x))
    return grouped['auc'].mean(skipna=True)


def create_feature_map(features, fmap):
    outfile = open(fmap, 'w')
    for i, feat in enumerate(features):
        outfile.write('{0}\t{1}\tq\n'.format(i, feat))
    outfile.close()


def train(param, num_round=1000, early_stopping_rounds=20):
    exec_time = time.strftime("%Y%m%d%I%p%M", time.localtime())

    os.mkdir('{0}_{1}'.format(model_path, exec_time))
    os.mkdir('{0}_{1}'.format(submission_path, exec_time))

    train_params = param.copy()
    train_params['num_boost_round'] = num_round
    train_params['early_stopping_rounds'] = early_stopping_rounds
    json.dump(train_params, open('{0}_{1}{2}'.format(model_path, exec_time, model_params), 'wb+'))

    print 'get training data'

    train_features = pd.read_csv(train_path + 'train_features.csv').astype(float)
    train_labels = pd.read_csv(train_path + 'labels.csv').astype(float)

    validate_features = pd.read_csv(validate_path + 'train_features.csv').astype(float)
    validate_labels = pd.read_csv(validate_path + 'labels.csv').astype(float)

    predict_features = pd.read_csv(predict_path + 'train_features.csv').astype(float)

    create_feature_map(train_features.columns.tolist(), '{0}_{1}{2}'.format(model_path, exec_time, model_fmap_file))

    train_matrix = xgboost.DMatrix(train_features.values, label=train_labels.values, feature_names=train_features.columns)
    val_matrix = xgboost.DMatrix(validate_features.values, label=validate_labels.values, feature_names=validate_features.columns)
    predict_matrix = xgboost.DMatrix(predict_features.values, feature_names=predict_features.columns)

    watchlist = [(train_matrix, 'train'), (val_matrix, 'eval')]

    print 'model training'
    with open('{0}_{1}{2}'.format(model_path, exec_time, model_train_log), 'wb+') as outf:
        sys.stdout = outf
        model = xgboost.train(param, train_matrix, num_boost_round=num_round, evals=watchlist, early_stopping_rounds=early_stopping_rounds)

    sys.stdout = save_stdout
    print 'model.best_score: {0}, model.best_iteration: {1}, model.best_ntree_limit: {2}'.format(model.best_score, model.best_iteration, model.best_ntree_limit)

    print 'output offline model data'
    model.save_model('{0}_{1}{2}'.format(model_path, exec_time, model_file))
    model.dump_model('{0}_{1}{2}'.format(model_path, exec_time, model_dump_file))

    importance = model.get_fscore(fmap='{0}_{1}{2}'.format(model_path, exec_time, model_fmap_file))
    importance = sorted(importance.items(), key=operator.itemgetter(1))
    df = pd.DataFrame(importance, columns=['feature', 'fscore'])
    df['fscore'] = df['fscore'] / df['fscore'].sum()
    df.to_csv('{0}_{1}{2}'.format(model_path, exec_time, model_feature_importance_csv), index=False)

    xgboost.plot_importance(model)
    plt.gcf().set_size_inches(20, 16)
    plt.gcf().set_tight_layout(True)
    plt.gcf().savefig('{0}_{1}{2}'.format(model_path, exec_time, model_feature_importance_file))
    plt.close()

    train_pred_labels = model.predict(train_matrix, ntree_limit=model.best_ntree_limit)
    val_pred_labels = model.predict(val_matrix, ntree_limit=model.best_ntree_limit)

    train_pred_frame = pd.Series(train_pred_labels, index=train_features.index)
    train_pred_frame.name = probability_consumed_label
    val_pred_frame = pd.Series(val_pred_labels, index=validate_features.index)
    val_pred_frame.name = probability_consumed_label

    train_true_frame = pd.read_csv(train_path + 'labels.csv')['Label']
    val_true_frame = pd.read_csv(validate_path + 'labels.csv')['Label']
    train_coupons = pd.read_csv(train_path + 'dataset.csv')
    val_coupons = pd.read_csv(validate_path + 'dataset.csv')
    train_check_matrix = train_coupons[[coupon_label]].join(train_true_frame).join(train_pred_frame)
    val_check_matrix = val_coupons[[coupon_label]].join(val_true_frame).join(val_pred_frame)
    print 'Average auc of train matrix: ', check_average_auc(train_check_matrix)
    print 'Average auc of validate matrix', check_average_auc(val_check_matrix)

    val_coupons = val_coupons.join(val_pred_frame).join(val_pred_frame.map(lambda x: 0. if x < 0.5 else 1.).rename('map')).join(val_true_frame)
    val_coupons.to_csv('{0}_{1}{2}'.format(model_path, exec_time, val_diff_file), index=False)
    print confusion_matrix(val_coupons['Label'], val_coupons['map'])

    labels = model.predict(predict_matrix, ntree_limit=model.best_ntree_limit)
    frame = pd.Series(labels, index=predict_features.index)
    frame.name = probability_consumed_label

    plt.figure()
    frame.hist(figsize=(10, 8))
    plt.title('results histogram')
    plt.xlabel('predict probability')
    plt.gcf().savefig('{0}_{1}{2}'.format(submission_path, exec_time, submission_hist_file))
    plt.close()

    submission = pd.read_csv(predict_path + 'dataset.csv')
    submission = submission[[user_label, coupon_label, date_received_label]].join(frame)
    submission.to_csv('{0}_{1}{2}'.format(submission_path, exec_time, submission_file), index=False)


if __name__ == '__main__':
    # 调参
    # param = dict()
    # param['objective'] = 'binary:logistic'
    # param['eval_metric'] = 'auc'
    # param['silent'] = 1
    # param['scale_pos_weight'] = 10
    # all_etas = [0.01, 0.05, 0.1, 0.15, 0.2]
    # all_subsamples = [0.6, 0.8, 1.0]
    # all_colsample_bytree = [0.6, 0.8, 1.0]
    # all_depth = [6, 7, 8, 9]
    # all_child_weights = [1, 10, 20, 50]
    # all_gamma = [0, 5, 20, 50]
    # for e, s, cb, d, cw, g in list(itertools.product(all_etas, all_subsamples, all_colsample_bytree, all_depth, all_child_weights, all_gamma)):
    #     param['eta'] = e
    #     param['subsample'] = s
    #     param['colsample_bytree'] = cb
    #     param['max_depth'] = d
    #     param['min_child_weight'] = cw
    #     param['gamma'] = g
    #     train(param)

    init_param = {
        'max_depth': 8,
        'eta': 0.1,
        'silent': 1,
        'seed': 13,
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'scale_pos_weight': 2,
        'subsample': 0.8,
        'colsample_bytree': 0.7,
        'min_child_weight': 100,
        'max_delta_step': 20
    }
    train(init_param, num_round=1000, early_stopping_rounds=50)

    # scale10  0.74242129
    # add_weight 0.72549162
    # select features 0.73841308
    # rmse 0.73966498
    # 2016110210PM11  1-discount  100 20    0.73404316
    # 2016110210PM23  1-discount  500 20    0.73569110
    # 0.74108233 2016110308pm00 sub new  500 20  model.best_score: 0.852115, model.best_iteration: 120, model.best_ntree_limit: 121  [[123366   4728][  5927   3146]]
    # model.best_score: 0.85172, model.best_iteration: 119, model.best_ntree_limit: 120  [[123461   4633][  5928   3145]]

    # submission_2016110502PM12 0.74261168
    # model.best_score: 0.85663, model.best_iteration: 137, model.best_ntree_limit: 138
    # Average auc of train matrix:  0.727421275725
    # Average auc of validate matrix 0.681185722817
    # [[122738   5356]
    # [  5543   3530]]

    # submission_2016110509PM19  0.79000986
    # get training data
    # model training
    # model.best_score: 0.858095, model.best_iteration: 69, model.best_ntree_limit: 70
    # output offline model data
    # Average auc of train matrix:  0.753320768317
    # Average auc of validate matrix 0.709031241249
    # [[123959   4135]
    #  [  5694   3379]]

    # submission_2016110509PM29 0.78708616
    # get training data
    # model training
    # model.best_score: 0.858987, model.best_iteration: 202, model.best_ntree_limit: 203
    # output offline model data
    # Average auc of train matrix:  0.761710580217
    # Average auc of validate matrix 0.710597949875
    # [[123322   4772]
    #  [  5335   3738]]

    # submission_2016110509PM36 0.78068065
    # get training data
    # model training
    # model.best_score: 0.85776, model.best_iteration: 129, model.best_ntree_limit: 130
    # output offline model data
    # Average auc of train matrix:  0.753199698721
    # Average auc of validate matrix 0.708276683647
    # [[123540   4554]
    #  [  5485   3588]]

    # submission_2016110509PM14 depth6 0.79348295
    # get training data
    # model training
    # model.best_score: 0.861524, model.best_iteration: 135, model.best_ntree_limit: 136
    # output offline model data
    # Average auc of train matrix:  0.761878748112
    # Average auc of validate matrix 0.709323127022
    # [[123566   4528]
    #  [  5394   3679]]

    # submission_2016110607PM17 depth 7  0.78921573
    # get training data
    # model training
    # model.best_score: 0.861385, model.best_iteration: 73, model.best_ntree_limit: 74
    # output offline model data
    # Average auc of train matrix:  0.761441571028
    # Average auc of validate matrix 0.71185537501
    # [[124045   4049]
    #  [  5608   3465]]

    # submission_2016110607PM25 depth 8  0.79633029
    # get training data
    # model training
    # model.best_score: 0.862489, model.best_iteration: 89, model.best_ntree_limit: 90
    # output offline model data
    # Average auc of train matrix:  0.765137453445
    # Average auc of validate matrix 0.711120528716
    # [[123793   4301]
    #  [  5501   3572]]

    # submission_2016110607PM29 500 50  0.79076895
    # get training data
    # model training
    # model.best_score: 0.862736, model.best_iteration: 283, model.best_ntree_limit: 284
    # output offline model data
    # Average auc of train matrix:  0.772870264569
    # Average auc of validate matrix 0.710568736196
    # [[123168   4926]
    #  [  5298   3775]]

    # submission_2016110701PM00  depth 9  0.78827241
    # get training data
    # model training
    # model.best_score: 0.861664, model.best_iteration: 111, model.best_ntree_limit: 112
    # output offline model data
    # Average auc of train matrix:  0.775929268395
    # Average auc of validate matrix 0.711823289069
    # [[123486   4608]
    #  [  5391   3682]]

    # submission_2016110701PM07 400 20 depth8  0.79399286
    # get training data
    # model training
    # model.best_score: 0.862964, model.best_iteration: 119, model.best_ntree_limit: 120
    # output offline model data
    # Average auc of train matrix:  0.767193259367
    # Average auc of validate matrix 0.713414099978
    # [[123673   4421]
    #  [  5438   3635]]

    # submission_2016110701PM11 depth10 0.78944517
    # get training data
    # model training
    # model.best_score: 0.861831, model.best_iteration: 82, model.best_ntree_limit: 83
    # output offline model data
    # Average auc of train matrix:  0.770331320947
    # Average auc of validate matrix 0.713417297885
    # [[123666   4428]
    #  [  5472   3601]]

    # submission_2016110701PM19 depth8 100 5  0.79276135
    # get training data
    # model training
    # model.best_score: 0.861766, model.best_iteration: 66, model.best_ntree_limit: 67
    # output offline model data
    # Average auc of train matrix:  0.762139340748
    # Average auc of validate matrix 0.710293797489
    # [[123996   4098]
    #  [  5579   3494]]

    # add all features
    # submission_2016110812PM26 depth9 200 10 0.78665721
    # get training data
    # model training
    # model.best_score: 0.858732, model.best_iteration: 84, model.best_ntree_limit: 85
    # output offline model data
    # Average auc of train matrix:  0.77127265585
    # Average auc of validate matrix 0.709394969472
    # [[123311   4783]
    #  [  5486   3587]]

    # submission_2016110812PM30 depth8 200 10  0.79309992
    # get training data
    # model training
    # model.best_score: 0.860688, model.best_iteration: 127, model.best_ntree_limit: 128
    # output offline model data
    # Average auc of train matrix:  0.774212759815
    # Average auc of validate matrix 0.710990924752
    # [[123384   4710]
    #  [  5495   3578]]

    # submission_2016110812PM33 depth7 200 10  0.79044134
    # get training data
    # model training
    # model.best_score: 0.862977, model.best_iteration: 168, model.best_ntree_limit: 169
    # output offline model data
    # Average auc of train matrix:  0.770838829031
    # Average auc of validate matrix 0.712736888436
    # [[123596   4498]
    #  [  5447   3626]]

    # submission_2016110812PM36 depth6 200 10
    # get training data
    # model training
    # model.best_score: 0.862854, model.best_iteration: 132, model.best_ntree_limit: 133
    # output offline model data
    # Average auc of train matrix:  0.761340624447
    # Average auc of validate matrix 0.712946435585
    # [[123813   4281]
    #  [  5494   3579]]

    # submission_2016110812PM39  depth10 200 10
    # get training data
    # model training
    # model.best_score: 0.859635, model.best_iteration: 82, model.best_ntree_limit: 83
    # output offline model data
    # Average auc of train matrix:  0.771884946019
    # Average auc of validate matrix 0.707826900582
    # [[123235   4859]
    #  [  5419   3654]]

    # submission_2016110812PM43 depth11 200 10  0.79202918
    # get training data
    # model training
    # model.best_score: 0.860852, model.best_iteration: 126, model.best_ntree_limit: 127
    # output offline model data
    # Average auc of train matrix:  0.78487172982
    # Average auc of validate matrix 0.708540378396
    # [[123325   4769]
    #  [  5365   3708]]

    # submission_2016110812PM47 depth12 200 10
    # get training data
    # model training
    # model.best_score: 0.860819, model.best_iteration: 95, model.best_ntree_limit: 96
    # output offline model data
    # Average auc of train matrix:  0.781210223665
    # Average auc of validate matrix 0.705365076863
    # [[123225   4869]
    #  [  5372   3701]]
