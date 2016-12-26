-- 验证集 0.6579397052272582
-- 测试集 0.75824453
drop table if exists charles_xgb_train;
create table charles_xgb_train as select * from charles_df2_fillna;

drop table if exists charles_xgb_val;
create table charles_xgb_val as select * from charles_df1_fillna;

drop table if exists charles_xgb_test;
create table charles_xgb_test as select * from charles_df3_fillna;

select count(*) from charles_xgb_train; -- 2339793
select count(*) from charles_xgb_val; -- 1232761
select count(*) from charles_xgb_test; -- 1024520

drop table if exists charles_xgb_val_pred;
drop table if exists charles_xgb_test_pred;
DROP OFFLINEMODEL IF EXISTS charles_xgboost_7;

-- train
PAI
-name xgboost
-project algo_public
-Deta="0.1"
-Dobjective="binary:logistic" --"rank:pairwise"
-DitemDelimiter=","
-Dseed="13"
-Dnum_round="400"
-DlabelColName="label"
-DinputTableName="charles_xgb_train"  --输入表
-DenableSparse="false"
-Dmax_depth="8"
-Dsubsample="0.8"
-Dcolsample_bytree="0.7"
-DmodelName="charles_xgboost_7"  --输出模型
-Dgamma="0"
-Dlambda="10" 
-DfeatureColNames="coupon_type,coupon_discount_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_500_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_500_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users"
		--特征字段
-Dbase_score="0.5"
-Dmin_child_weight="100"
-DkvDelimiter=":";

-- predict for val data
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="user_id,coupon_id,date_received,label"
-DmodelName="charles_xgboost_7"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="charles_xgb_val_pred" --输出表
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="coupon_type,coupon_discount_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_500_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_500_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users"
		--特征字段
-DinputTableName="charles_xgb_val" --输入表
-DenableSparse="false";

drop table if exists charles_xgb_eval;
create table charles_xgb_eval as select user_id,coupon_id,date_received,label,
case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability from charles_xgb_val_pred;

drop table if exists charles_xgb_final_eval;
create table charles_xgb_final_eval as 
	select user_id,coupon_id,date_received,max(label) as label,max(probability) as prediction_score from
	(select * from charles_xgb_eval)t
	group by user_id,coupon_id,date_received;
	
drop table if exists charles_eval_tmp;
create table charles_eval_tmp(
	coupon_id string,
	auc double
);

drop table if exists charles_xgb_eval_view;
create table charles_xgb_eval_view as
	select sum(cnt) as counts,sum(pred_1_cnt) as pred_1_counts,sum(pred_0_cnt) as pred_0_counts,
		   sum(tp_cnt) as tp_counts,sum(tn_cnt) as tn_counts,sum(fp_cnt) as fp_counts,sum(fn_cnt) as fn_counts from
	(
		select 1 as cnt,
		case when prediction_score>=0.5 then 1 else 0 end as pred_1_cnt,
		case when prediction_score>=0.5 then 0 else 1 end as pred_0_cnt,
		case when prediction_score>=0.5 and label=1 then 1 else 0 end as tp_cnt,
		case when prediction_score<0.5 and label=0 then 1 else 0 end as tn_cnt,
		case when prediction_score>=0.5 and label=0 then 1 else 0 end as fp_cnt,
		case when prediction_score<0.5 and label=1 then 1 else 0 end as fn_cnt
		from charles_xgb_final_eval
	)t;
select * from charles_xgb_eval_view;
-- counts,pred_1_counts,pred_0_counts,tp_counts,tn_counts,fp_counts,fn_counts
-- 1232761,42487,1190274,22767,1134489,19720,55785

-- predict for test data
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="user_id,coupon_id,date_received"
-DmodelName="charles_xgboost_7"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="charles_xgb_test_pred" --输出表
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="coupon_type,coupon_discount_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_500_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_500_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users"
		--特征字段
-DinputTableName="charles_xgb_test" --输入表
-DenableSparse="false";

drop table if exists charles_xgb_submission_7;
create table charles_xgb_submission_7 as 
	select user_id,coupon_id,date_received,max(probability) as probability from
	(
		select user_id,coupon_id,date_received,
			case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
		from charles_xgb_test_pred
	)t
	group by user_id,coupon_id,date_received;
	
drop table if exists charles_xgb_submission_view;
create table charles_xgb_submission_view as
	select sum(cnt) as counts,sum(pred_1_cnt) as pred_1_counts,sum(pred_0_cnt) as pred_0_counts from
	(
		select 1 as cnt,
		case when probability>=0.5 then 1 else 0 end as pred_1_cnt,
		case when probability>=0.5 then 0 else 1 end as pred_0_cnt
		from charles_xgb_submission_7
	)t;
select * from charles_xgb_submission_view;
-- counts,pred_1_counts,pred_0_counts
-- 1024520,34962,989558