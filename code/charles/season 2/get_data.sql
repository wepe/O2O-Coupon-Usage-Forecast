create table if not exists charles_train_offline_stage2 as select * from odps_tc_257100_f673506e024.train_offline_stage2;
create table if not exists charles_train_online_stage2 as select * from odps_tc_257100_f673506e024.train_online_stage2;
create table if not exists charles_prediction_stage2 as select * from odps_tc_257100_f673506e024.prediction_stage2;