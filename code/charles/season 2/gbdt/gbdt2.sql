-- 验证集 0.6590665304569867
-- 测试集 0.75711007
drop table if exists charles_gbdt_val_eval;
create table charles_gbdt_val_eval as select user_id,coupon_id,date_received,label,
case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability from charles_gbdt_eval_2;

drop table if exists charles_gbdt_final_eval;
create table charles_gbdt_final_eval as 
	select user_id,coupon_id,date_received,max(label) as label,max(probability) as prediction_score from
	(select * from charles_gbdt_val_eval)t
	group by user_id,coupon_id,date_received;
	
drop table if exists charles_eval_tmp;
create table charles_eval_tmp(
	coupon_id string,
	auc double
);

drop table if exists charles_gbdt_eval_view;
create table charles_gbdt_eval_view as
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
		from charles_gbdt_final_eval
	)t;
select * from charles_gbdt_eval_view;
-- counts,pred_1_counts,pred_0_counts,tp_counts,tn_counts,fp_counts,fn_counts
-- 1232761,35645,1197116,20267,1138831,15378,58285

drop table if exists charles_gbdt_submission_2;
create table charles_gbdt_submission_2 as 
	select user_id,coupon_id,date_received,max(probability) as probability from
	(
		select user_id,coupon_id,date_received,
			case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
		from charles_gbdt_pred_2
	)t
	group by user_id,coupon_id,date_received;
	
drop table if exists charles_gbdt_submission_view;
create table charles_gbdt_submission_view as
	select sum(cnt) as counts,sum(pred_1_cnt) as pred_1_counts,sum(pred_0_cnt) as pred_0_counts from
	(
		select 1 as cnt,
		case when probability>=0.5 then 1 else 0 end as pred_1_cnt,
		case when probability>=0.5 then 0 else 1 end as pred_0_cnt
		from charles_gbdt_submission_2
	)t;
select * from charles_gbdt_submission_view;
-- counts,pred_1_counts,pred_0_counts
-- 1024520,33124,991396
