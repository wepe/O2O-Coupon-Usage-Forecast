-- 数据集划分:
--               	(date_received)                              
--    	dateset3: 20160701~20160731 ,features3 from 20160315~20160630  (测试集，dataset3对应label窗，feature3对应特征窗)
--    	dateset2: 20160515~20160615 ,features2 from 20160201~20160514  
--    	dateset1: 20160414~20160514 ,features1 from 20160101~20160413 

-- 划分数据集
create table if not exists charles_dataset3 as select * from prediction_stage2;
create table if not exists charles_feature3 as select * from train_offline_stage2 where "20160315"<=date_received and date_received<="20160630";
create table if not exists charles_online_feature3 as select * from train_online_stage2 where "20160315"<=date_received and date_received<="20160630";

create table if not exists charles_dataset2 as select * from train_offline_stage2 where "20160515"<=date_received and date_received<="20160615";
create table if not exists charles_feature2 as select * from train_offline_stage2 where "20160201"<=date_received and date_received<="20160514";
create table if not exists charles_online_feature2 as select * from train_online_stage2 where "20160201"<=date_received and date_received<="20160514";

create table if not exists charles_dataset1 as select * from train_offline_stage2 where "20160414"<=date_received and date_received<="20160514";
create table if not exists charles_feature1 as select * from train_offline_stage2 where "20160101"<=date_received and date_received<="20160413";
create table if not exists charles_online_feature1 as select * from train_online_stage2 where "20160101"<=date_received and date_received<="20160413";

-- 取出一个小的数据集做测试
create table if not exists charles_small_dataset3 as select * from charles_dataset3 limit 1000;
create table if not exists charles_small_feature3 as select * from charles_feature3 limit 1000;
create table if not exists charles_small_online_feature3 as select * from charles_online_feature3 limit 1000;

create table if not exists charles_small_dataset2 as select * from charles_dataset2 limit 1000;
create table if not exists charles_small_feature2 as select * from charles_feature2 limit 1000;
create table if not exists charles_small_online_feature2 as select * from charles_online_feature2 limit 1000;

create table if not exists charles_small_dataset1 as select * from charles_dataset1 limit 1000;
create table if not exists charles_small_feature1 as select * from charles_feature1 limit 1000;
create table if not exists charles_small_online_feature1 as select * from charles_online_feature1 limit 1000;
