#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: feature_extract.py
# Date: 2016-10-30
# Author: Chaos <xinchaoxt@gmail.com>

from feature_extract import *
import pandas as pd


def gen_feature_data(features_dir, dataset, output):
    user_features = pd.read_csv(features_dir + 'user_features.csv').astype(str)
    merchant_features = pd.read_csv(features_dir + 'merchant_features.csv').astype(str)
    user_merchant_features = pd.read_csv(features_dir + 'user_merchant_features.csv').astype(str)

    columns = dataset.columns.tolist()
    df = dataset.merge(user_features, on=user_label, how='left')
    df = df.merge(merchant_features, on=merchant_label, how='left')
    df = df.merge(user_merchant_features, on=[user_label, merchant_label], how='left')

    df = add_dataset_features(df)
    df.drop(columns, axis=1, inplace=True)
    df.fillna(np.nan, inplace=True)
    print df.shape
    print 'start dump feature data'
    df.to_csv(output, index=False)


def gen_label_data(dataset, output):
    df = add_label(dataset)
    print 'start dump label data'
    df[['Label']].astype(float).to_csv(output, index=False)


def gen_data(path, label=True):
    features_dir = path + 'features/'
    dataset_file = path + 'dataset.csv'
    dataset = pd.read_csv(dataset_file).astype(str)
    gen_feature_data(features_dir, dataset, path + 'train_features.csv')
    if label:
        gen_label_data(dataset, path + 'labels.csv')


if __name__ == '__main__':
    print 'generate train data...'
    gen_data(train_path)
    print 'generate validate data...'
    gen_data(validate_path)
    print 'generate predict features...'
    gen_data(predict_path, label=False)

    # generate train data...
    # (258446, 44)
    # start dump feature data
    # pos_counts: 23485, neg_counts: 234961
    # start dump label data
    # generate validate data...
    # (137167, 44)
    # start dump feature data
    # pos_counts: 9073, neg_counts: 128094
    # start dump label data
    # generate predict features...
    # (113640, 44)
    # start dump feature data

    # generate train data...
    # sort begin...
    # (258446, 92)
    # start dump feature data
    # pos_counts: 23485, neg_counts: 234961
    # start dump label data
    # generate validate data...
    # sort begin...
    # (137167, 92)
    # start dump feature data
    # pos_counts: 9073, neg_counts: 128094
    # start dump label data
    # generate predict features...
    # sort begin...
    # (113640, 92)
    # start dump feature data
