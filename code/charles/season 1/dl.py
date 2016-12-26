#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: dl.py
# Date: 2016-10-18
# Author: Chaos <xinchaoxt@gmail.com>

# from keras.regularizers import l1, l2, l1l2, activity_l1, activity_l2, activity_l1l2
# from keras.utils import np_utils

import matplotlib.pyplot as plt
import time
from sklearn import metrics
from feature_extract_old import *
from keras.models import Sequential
from keras.layers.core import Dense, Dropout
import numpy as np
import os


def auc(y_true, y_pred):
    return metrics.roc_auc_score(y_true, y_pred)


def create_feature_map(features, fmap):
    outfile = open(fmap, 'w')
    for i, feat in enumerate(features):
        outfile.write('{0}\t{1}\tq\n'.format(i, feat))
    outfile.close()


if __name__ == '__main__':
    exec_time = time.strftime("%Y%m%d%I%p%M", time.localtime())

    os.mkdir('{0}_dl_{1}'.format(model_path, exec_time))
    os.mkdir('{0}_dl_{1}'.format(submission_path, exec_time))

    print 'get training data'
    train_features = pd.read_csv(train_path + 'train_features.csv').astype(float)
    train_labels = pd.read_csv(train_path + 'labels.csv').astype(float)

    validate_features = pd.read_csv(validate_path + 'train_features.csv').astype(float)
    validate_labels = pd.read_csv(validate_path + 'labels.csv').astype(float)

    predict_features = pd.read_csv(predict_path + 'train_features.csv').astype(float)

    dfs = [train_features, validate_features, predict_features]
    for df in dfs:
        df = df.applymap(lambda x: np.nan if x == -1. else x)

    create_feature_map(train_features.columns.tolist(), '{0}_dl_{1}{2}'.format(model_path, exec_time, model_fmap_file))

    print 'Keras Training'
    model = Sequential()
    model.add(Dense(60, input_dim=105, init='uniform', activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(60, activation='relu'))
    model.add(Dropout(0.2))
    model.add(Dense(1, activation='sigmoid'))

    model.compile(loss='binary_crossentropy', optimizer='rmsprop', metrics=['binary_crossentropy'])

    # Training
    model.fit(validate_features.values, validate_labels.values[:, 0], nb_epoch=10, batch_size=32, verbose=1, validation_data=(train_features.values, train_labels.values[:, 0]))

    # Save the model weights to a local file
    model.save_weights('{0}_dl_{1}{2}'.format(model_path, exec_time, model_file), overwrite=True)
    predict_labels = model.predict(train_features.values, batch_size=16)
    print 'AUC Score:', auc(np.array(train_labels.values[:, 0]), predict_labels.T[0])

    labels = model.predict(predict_features.values, batch_size=32)
    labels = labels.T[0]

    print 'generate submission'
    frame = pd.Series(labels, index=predict_features.index)
    frame.name = probability_consumed_label

    plt.figure()
    frame.hist(figsize=(10, 8))
    plt.title('results histogram')
    plt.xlabel('predict probability')
    plt.gcf().savefig('{0}_dl_{1}{2}'.format(submission_path, exec_time, submission_hist_file))

    submission = pd.read_csv(predict_path + 'dataset.csv')
    submission = submission[[user_label, coupon_label, date_received_label]].join(frame)
    submission.to_csv('{0}_dl_{1}{2}'.format(submission_path, exec_time, submission_file), index=False)
