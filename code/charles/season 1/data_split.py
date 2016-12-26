#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: data_split.py
# Date: 2016-10-14
# Author: Chaos <xinchaoxt@gmail.com>
from data_view import DataView
from config import *


if __name__ == '__main__':
    train_offline_data = DataView(offline_train_file_path)
    test_offline_data = DataView(offline_test_file_path)
    train_online_data = DataView(online_train_file_path)

    # split by user

    train_offline_user_list, train_offline_user_set = train_offline_data.user_list, train_offline_data.user_set
    test_offline_user_list, test_offline_user_set = test_offline_data.user_list, test_offline_data.user_set
    train_online_user_list, train_online_user_set = train_online_data.user_list, train_online_data.user_set
    active_users = train_offline_user_set & train_online_user_set

    active_user_offline_record = train_offline_data.data[train_offline_data.data[user_label].isin(active_users)]
    active_user_online_record = train_online_data.data[train_online_data.data[user_label].isin(active_users)]
    offline_user_record = train_offline_data.data[~train_offline_data.data[user_label].isin(active_users)]
    online_user_record = train_online_data.data[~train_online_data.data[user_label].isin(active_users)]

    active_user_offline_record.to_csv(active_user_offline_data_path, index=False)
    active_user_online_record.to_csv(active_user_online_data_path, index=False)
    offline_user_record.to_csv(offline_user_data_path, index=False)
    online_user_record.to_csv(online_user_data_path, index=False)

    # split by received time

    # 之后将从raw_data中提取feature, 用dataset中的label结合feature做训练、验证和预测

    train_raw_data = train_offline_data.filter_by_received_time(train_feature_start_time, train_feature_end_time)
    print train_offline_data.data.shape, train_raw_data.shape
    train_raw_data.to_csv(train_raw_data_path, index=False)

    validate_raw_data = train_offline_data.filter_by_received_time(validate_feature_start_time, validate_feature_end_time)
    print train_offline_data.data.shape, validate_raw_data.shape
    validate_raw_data.to_csv(validate_raw_data_path, index=False)

    predict_raw_data = train_offline_data.filter_by_received_time(predict_feature_start_time, predict_feature_end_time)
    print train_offline_data.data.shape, predict_raw_data.shape
    predict_raw_data.to_csv(predict_raw_data_path, index=False)

    train_dataset = train_offline_data.filter_by_received_time(train_dataset_start_time, train_dataset_end_time)
    print train_offline_data.data.shape, train_dataset.shape
    train_dataset.to_csv(train_dataset_path, index=False)

    validate_dataset = train_offline_data.filter_by_received_time(validate_dataset_start_time, validate_dataset_end_time)
    print train_offline_data.data.shape, validate_dataset.shape
    validate_dataset.to_csv(validate_dataset_path, index=False)

    predict_dataset = test_offline_data.filter_by_received_time(predict_dataset_start_time, predict_dataset_end_time)
    print test_offline_data.data.shape, predict_dataset.shape
    predict_dataset.to_csv(predict_dataset_path, index=False)

    # 之后将从raw_online_data中提取online feature

    active_user_online_data = DataView(active_user_online_data_path)
    train_online_raw_data = active_user_online_data.filter_by_received_time(train_feature_start_time, train_feature_end_time)
    train_online_raw_data.to_csv(train_raw_online_data_path, index=False)
    validate_online_raw_data = active_user_online_data.filter_by_received_time(validate_feature_start_time, validate_feature_end_time)
    validate_online_raw_data.to_csv(validate_raw_online_data_path, index=False)
    predict_online_raw_data = active_user_online_data.filter_by_received_time(predict_feature_start_time, predict_feature_end_time)
    predict_online_raw_data.to_csv(predict_raw_online_data_path, index=False)

    # (1754884, 7) (423297, 7)
    # (1754884, 7) (657669, 7)
    # (1754884, 7) (537172, 7)
    # (1754884, 7) (258446, 7)
    # (1754884, 7) (137167, 7)
    # (113640, 6) (113640, 6)
