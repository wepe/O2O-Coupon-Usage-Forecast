# data file path
online_train_file_path = './data/ccf_data_revised/ccf_online_stage1_train.csv'
offline_train_file_path = './data/ccf_data_revised/ccf_offline_stage1_train.csv'
offline_test_file_path = './data/ccf_data_revised/ccf_offline_stage1_test_revised.csv'

# split data path
active_user_offline_data_path = './data/data_split/active_user_offline_record.csv'
active_user_online_data_path = './data/data_split/active_user_online_record.csv'
offline_user_data_path = './data/data_split/offline_user_record.csv'
online_user_data_path = './data/data_split/online_user_record.csv'

train_path = './data/data_split/train_data/'
train_feature_data_path = train_path + 'features/'
train_raw_data_path = train_path + 'raw_data.csv'
train_dataset_path = train_path + 'dataset.csv'
train_raw_online_data_path = train_path + 'raw_online_data.csv'

validate_path = './data/data_split/validate_data/'
validate_feature_data_path = validate_path + 'features/'
validate_raw_data_path = validate_path + 'raw_data.csv'
validate_dataset_path = validate_path + 'dataset.csv'
validate_raw_online_data_path = validate_path + 'raw_online_data.csv'

predict_path = './data/data_split/predict_data/'
predict_feature_data_path = predict_path + 'features/'
predict_raw_data_path = predict_path + 'raw_data.csv'
predict_dataset_path = predict_path + 'dataset.csv'
predict_raw_online_data_path = predict_path + 'raw_online_data.csv'

# model path
model_path = './data/model/model'
model_file = '/model'
model_dump_file = '/model_dump.txt'
model_fmap_file = '/model.fmap'
model_feature_importance_file = '/feature_importance.png'
model_feature_importance_csv = '/feature_importance.csv'
model_train_log = '/train.log'
model_params = '/param.json'

val_diff_file = '/val_diff.csv'

# submission path
submission_path = './data/submission/submission'
submission_hist_file = '/hist.png'
submission_file = '/submission.csv'

# raw field name
user_label = 'User_id'
merchant_label = 'Merchant_id'
coupon_label = 'Coupon_id'
action_label = 'Action'
discount_label = 'Discount_rate'
distance_label = 'Distance'
date_received_label = 'Date_received'
date_consumed_label = 'Date'
probability_consumed_label = 'Probability'

# global values
consume_time_limit = 15

train_feature_start_time = '20160201'
train_feature_end_time = '20160514'
train_dataset_start_time = '20160515'
train_dataset_end_time = '20160615'

validate_feature_start_time = '20160101'
validate_feature_end_time = '20160413'
validate_dataset_start_time = '20160414'
validate_dataset_end_time = '20160514'

predict_feature_start_time = '20160315'
predict_feature_end_time = '20160630'
predict_dataset_start_time = '20160701'
predict_dataset_end_time = '20160731'


