-- 数据集划分:
--                    预测区间（label窗）          特征区间（feature窗）                                                
--           dateset3: 20160701~20160731 ,features3 from 20160315~20160630
--           dateset2: 20160515~20160615 ,features2 from 20160201~20160514  
--           dateset1: 20160414~20160514 ,features1 from 20160101~20160413        

-- off-line表提取的特征 
-- 1.merchant related: 
--      sales_use_coupon. total_coupon
--       transfer_rate = sales_use_coupon/total_coupon.
--       merchant_avg_distance,merchant_min_distance,merchant_max_distance of those use coupon 
--       total_sales.  coupon_rate = sales_use_coupon/total_sales.  

--       新添加：
--       消费过该商家的不同用户数量   merchant_user_buy_count
       
-- 2.coupon related: 
--       discount_rate. discount_man. discount_jian. is_man_jian
--       day_of_week,day_of_month. (date_received)

--       新添加：（label窗里的coupon在特征窗有出现，应提取相关特征）
--             label窗里的coupon，在特征窗中被消费过的数目  label_coupon_feature_buy_count
--             label窗里的coupon，在特征窗中被领取过的数目  label_coupon_feature_receive_count
--             label窗里的coupon，在特征窗中的核销率  label_coupon_feature_rate = label_coupon_feature_buy_count/label_coupon_feature_receive_count
      
-- 3.user related: 
--       distance. 
--       user_avg_distance, user_min_distance,user_max_distance. 
--       buy_use_coupon. buy_total. coupon_received.
--       buy_use_coupon/coupon_received. 
--      avg_diff_date_datereceived. min_diff_date_datereceived. max_diff_date_datereceived.  
--       count_merchant.  

-- 4.user_merchant: 
--       user_merchant_buy_total.  user_merchant_received    user_merchant_buy_use_coupon  user_merchant_any  user_merchant_buy_common
--       user_merchant_coupon_transform_rate = user_merchant_buy_use_coupon/user_merchant_received
--       user_merchant_coupon_buy_rate = user_merchant_buy_use_coupon/user_merchant_buy_total
--       user_merchant_common_buy_rate = user_merchant_buy_common/user_merchant_buy_total
--       user_merchant_rate = user_merchant_buy_total/user_merchant_any
     

-- 5. other feature:（label 窗提取的特征）
--       this_month_user_receive_all_coupon_count
--       this_month_user_receive_same_coupon_count
--       this_month_user_receive_same_coupon_lastone
--       this_month_user_receive_same_coupon_firstone
--       this_day_user_receive_all_coupon_count
--       this_day_user_receive_same_coupon_count

--       新添加：
--       day_gap_before, day_gap_after  (receive the same coupon)
--       商家有交集的用户数目 label_merchant_user_count
--       商家发出的所有优惠券数目  label_merchant_coupon_count
--       商家发出的所有优惠券种类数目  label_merchant_coupon_type_count
--       用户领取该商家的所有优惠券数目  label_user_merchant_coupon_count
--       用户在此次优惠券之后还领取了多少该优惠券   label_same_coupon_count_later
--       用户在此次优惠券之后还领取了多少优惠券     label_coupon_count_later
--       用户有交集的商家数目     label_user_merchant_count


-- 6. user_coupon:
--       对label窗里的user_coupon，特征窗里用户领取过该coupon几次   label_user_coupon_feature_receive_count
--       对label窗里的user_coupon，特征窗里用户用该coupon消费过几次   label_user_coupon_feature_buy_count
--       对label窗里的user_coupon，特征窗里用户对该coupon的核销率   label_user_coupon_feature_rate = label_user_coupon_feature_buy_count/label_user_coupon_feature_receive_count



-- 7. online表的特征（都是用户相关）
--       用户线上购买总次数  online_buy_total
--       用户线上用coupon购买的总次数 online_buy_use_coupon
--       用户线上用fixed购买的总次数  online_buy_use_fixed
--       用户线上收到的coupon总次数   online_coupon_received
--       用户线上有发生购买的merchant个数  online_buy_merchant_count
--       用户线上有action的merchant个数      online_action_merchant_count
--       online_buy_use_coupon_fixed = online_buy_use_coupon+online_buy_use_fixed
--       online_buy_use_coupon_rate = online_buy_use_coupon/online_buy_total
--       online_buy_use_fixed_rate = online_buy_use_fixed/online_buy_total 
--       online_buy_use_coupon_fixed_rate = online_buy_use_coupon_fixed/online_buy_total 
--       online_coupon_transform_rate = online_buy_use_coupon/online_coupon_received

-- #################################################################################

-- 划分数据集
create table if not exists wepon_dataset3 as select * from prediction_stage2;
create table if not exists wepon_feature3 as select * from train_offline_stage2 
where ("20160315"<=date_pay and date_pay<="20160630") or (date_pay="null" and "20160315"<=date_received and date_received<="20160630");
create table if not exists wepon_online_feature3 as select * from train_online_stage2 
where ("20160315"<=date_pay and date_pay<="20160630") or (date_pay="null" and "20160315"<=date_received and date_received<="20160630");

create table if not exists wepon_dataset2 as select * from train_offline_stage2 where "20160515"<=date_received and date_received<="20160615";
create table if not exists wepon_feature2 as select * from train_offline_stage2
where ("20160201"<=date_pay and date_pay<="20160514") or (date_pay="null" and "20160201"<=date_received and date_received<="20160514");
create table if not exists wepon_online_feature2 as select * from train_online_stage2
where ("20160201"<=date_pay and date_pay<="20160514") or (date_pay="null" and "20160201"<=date_received and date_received<="20160514");

create table if not exists wepon_dataset1 as select * from train_offline_stage2 where "20160414"<=date_received and date_received<="20160514";
create table if not exists wepon_feature1 as select * from train_offline_stage2
where ("20160101"<=date_pay and date_pay<="20160413") or (date_pay="null" and "20160101"<=date_received and date_received<="20160413");
create table if not exists wepon_online_feature1 as select * from train_online_stage2
where ("20160101"<=date_pay and date_pay<="20160413") or (date_pay="null" and "20160101"<=date_received and date_received<="20160413");



-- 7. online表的特征（都是用户相关）
--       用户线上购买总次数  online_buy_total
--       用户线上用coupon购买的总次数 online_buy_use_coupon
--       用户线上用fixed购买的总次数  online_buy_use_fixed
--       用户线上收到的coupon总次数   online_coupon_received
--       用户线上有发生购买的merchant个数  online_buy_merchant_count
--       用户线上有action的merchant个数      online_action_merchant_count
--       online_buy_use_coupon_fixed = online_buy_use_coupon+online_buy_use_fixed
--       online_buy_use_coupon_rate = online_buy_use_coupon/online_buy_total
--       online_buy_use_fixed_rate = online_buy_use_fixed/online_buy_total 
--       online_buy_use_coupon_fixed_rate = online_buy_use_coupon_fixed/online_buy_total 
--       online_coupon_transform_rate = online_buy_use_coupon/online_coupon_received


-- ##############  for dataset3  ################### 
create table wepon_d3_f7_t1 as
select user_id,count(*) as online_buy_total from
(
	select user_id from wepon_online_feature3 where action=1
)t 
group by user_id;


create table wepon_d3_f7_t2 as
select user_id,count(*) as online_buy_use_coupon from
(
	select user_id from wepon_online_feature3 where action=1 and coupon_id!="null" and coupon_id!="fixed"
)t 
group by user_id;

create table wepon_d3_f7_t3 as
select user_id,count(*) as online_buy_use_fixed from
(
	select user_id from wepon_online_feature3 where action=1 and coupon_id="fixed"
)t 
group by user_id;


create table wepon_d3_f7_t4 as
select user_id,count(*) as online_coupon_received from
(
	select user_id from wepon_online_feature3 where coupon_id!="null" and coupon_id!="fixed"
)t 
group by user_id;


create table wepon_d3_f7_t5 as
select user_id,count(*) as online_buy_merchant_count from
(
	select distinct user_id,merchant_id from wepon_online_feature3 where action=1
)t 
group by user_id;

create table wepon_d3_f7_t6 as
select user_id,count(*) as online_action_merchant_count from
(
	select distinct user_id,merchant_id from wepon_online_feature3
)t 
group by user_id;

create table wepon_d3_f7 as
select t.*,t.online_buy_use_coupon+t.online_buy_use_fixed as online_buy_use_coupon_fixed,
           case when t.online_buy_total=0 then -1 else t.online_buy_use_coupon/t.online_buy_total end as online_buy_use_coupon_rate,
		   case when t.online_buy_total=0 then -1 else t.online_buy_use_fixed/t.online_buy_total  end as online_buy_use_fixed_rate,
		   case when t.online_buy_total=0 then -1 else (t.online_buy_use_coupon+t.online_buy_use_fixed)/t.online_buy_total end as  online_buy_use_coupon_fixed_rate,
		   case when t.online_coupon_received=0 then -1 else t.online_buy_use_coupon/t.online_coupon_received end as online_coupon_transform_rate
from		   
(
	select a.user_id,case when b.online_buy_total is null then 0 else b.online_buy_total end as online_buy_total,
					case when c.online_buy_use_coupon is null then 0 else c.online_buy_use_coupon end as online_buy_use_coupon,
					case when d.online_buy_use_fixed is null then 0 else d.online_buy_use_fixed end as online_buy_use_fixed,
					case when e.online_coupon_received is null then 0 else e.online_coupon_received end as online_coupon_received,
					case when f.online_buy_merchant_count is null then 0 else f.online_buy_merchant_count end as online_buy_merchant_count,
					case when g.online_action_merchant_count is null then 0 else g.online_action_merchant_count end as online_action_merchant_count
	from
		(select distinct user_id from wepon_online_feature3) a
	left outer join
		wepon_d3_f7_t1 b  
		on a.user_id=b.user_id
	left outer join
		wepon_d3_f7_t2 c  
		on a.user_id=c.user_id
	left outer join
		wepon_d3_f7_t3 d 
		on a.user_id=d.user_id
	left outer join
		wepon_d3_f7_t4 e
		on a.user_id=e.user_id
	left outer join
		wepon_d3_f7_t5 f
		on a.user_id=f.user_id
	left outer join
		wepon_d3_f7_t6 g
		on a.user_id=g.user_id
)t;


-- ##############  for dataset2  ################### 
create table wepon_d2_f7_t1 as
select user_id,count(*) as online_buy_total from
(
	select user_id from wepon_online_feature2 where action=1
)t 
group by user_id;


create table wepon_d2_f7_t2 as
select user_id,count(*) as online_buy_use_coupon from
(
	select user_id from wepon_online_feature2 where action=1 and coupon_id!="null" and coupon_id!="fixed"
)t 
group by user_id;

create table wepon_d2_f7_t3 as
select user_id,count(*) as online_buy_use_fixed from
(
	select user_id from wepon_online_feature2 where action=1 and coupon_id="fixed"
)t 
group by user_id;


create table wepon_d2_f7_t4 as
select user_id,count(*) as online_coupon_received from
(
	select user_id from wepon_online_feature2 where coupon_id!="null" and coupon_id!="fixed"
)t 
group by user_id;


create table wepon_d2_f7_t5 as
select user_id,count(*) as online_buy_merchant_count from
(
	select distinct user_id,merchant_id from wepon_online_feature2 where action=1
)t 
group by user_id;

create table wepon_d2_f7_t6 as
select user_id,count(*) as online_action_merchant_count from
(
	select distinct user_id,merchant_id from wepon_online_feature2
)t 
group by user_id;

create table wepon_d2_f7 as
select t.*,t.online_buy_use_coupon+t.online_buy_use_fixed as online_buy_use_coupon_fixed,
           case when t.online_buy_total=0 then -1 else t.online_buy_use_coupon/t.online_buy_total end as online_buy_use_coupon_rate,
		   case when t.online_buy_total=0 then -1 else t.online_buy_use_fixed/t.online_buy_total  end as online_buy_use_fixed_rate,
		   case when t.online_buy_total=0 then -1 else (t.online_buy_use_coupon+t.online_buy_use_fixed)/t.online_buy_total end as  online_buy_use_coupon_fixed_rate,
		   case when t.online_coupon_received=0 then -1 else t.online_buy_use_coupon/t.online_coupon_received end as online_coupon_transform_rate
from		   
(
	select a.user_id,case when b.online_buy_total is null then 0 else b.online_buy_total end as online_buy_total,
					case when c.online_buy_use_coupon is null then 0 else c.online_buy_use_coupon end as online_buy_use_coupon,
					case when d.online_buy_use_fixed is null then 0 else d.online_buy_use_fixed end as online_buy_use_fixed,
					case when e.online_coupon_received is null then 0 else e.online_coupon_received end as online_coupon_received,
					case when f.online_buy_merchant_count is null then 0 else f.online_buy_merchant_count end as online_buy_merchant_count,
					case when g.online_action_merchant_count is null then 0 else g.online_action_merchant_count end as online_action_merchant_count
	from
		(select distinct user_id from wepon_online_feature2) a
	left outer join
		wepon_d2_f7_t1 b  
		on a.user_id=b.user_id
	left outer join
		wepon_d2_f7_t2 c  
		on a.user_id=c.user_id
	left outer join
		wepon_d2_f7_t3 d 
		on a.user_id=d.user_id
	left outer join
		wepon_d2_f7_t4 e
		on a.user_id=e.user_id
	left outer join
		wepon_d2_f7_t5 f
		on a.user_id=f.user_id
	left outer join
		wepon_d2_f7_t6 g
		on a.user_id=g.user_id
)t;


-- ##############  for dataset1  ################### 
create table wepon_d1_f7_t1 as
select user_id,count(*) as online_buy_total from
(
	select user_id from wepon_online_feature1 where action=1
)t 
group by user_id;


create table wepon_d1_f7_t2 as
select user_id,count(*) as online_buy_use_coupon from
(
	select user_id from wepon_online_feature1 where action=1 and coupon_id!="null" and coupon_id!="fixed"
)t 
group by user_id;

create table wepon_d1_f7_t3 as
select user_id,count(*) as online_buy_use_fixed from
(
	select user_id from wepon_online_feature1 where action=1 and coupon_id="fixed"
)t 
group by user_id;


create table wepon_d1_f7_t4 as
select user_id,count(*) as online_coupon_received from
(
	select user_id from wepon_online_feature1 where coupon_id!="null" and coupon_id!="fixed"
)t 
group by user_id;


create table wepon_d1_f7_t5 as
select user_id,count(*) as online_buy_merchant_count from
(
	select distinct user_id,merchant_id from wepon_online_feature1 where action=1
)t 
group by user_id;

create table wepon_d1_f7_t6 as
select user_id,count(*) as online_action_merchant_count from
(
	select distinct user_id,merchant_id from wepon_online_feature1
)t 
group by user_id;

create table wepon_d1_f7 as
select t.*,t.online_buy_use_coupon+t.online_buy_use_fixed as online_buy_use_coupon_fixed,
           case when t.online_buy_total=0 then -1 else t.online_buy_use_coupon/t.online_buy_total end as online_buy_use_coupon_rate,
		   case when t.online_buy_total=0 then -1 else t.online_buy_use_fixed/t.online_buy_total  end as online_buy_use_fixed_rate,
		   case when t.online_buy_total=0 then -1 else (t.online_buy_use_coupon+t.online_buy_use_fixed)/t.online_buy_total end as  online_buy_use_coupon_fixed_rate,
		   case when t.online_coupon_received=0 then -1 else t.online_buy_use_coupon/t.online_coupon_received end as online_coupon_transform_rate
from		   
(
	select a.user_id,case when b.online_buy_total is null then 0 else b.online_buy_total end as online_buy_total,
					case when c.online_buy_use_coupon is null then 0 else c.online_buy_use_coupon end as online_buy_use_coupon,
					case when d.online_buy_use_fixed is null then 0 else d.online_buy_use_fixed end as online_buy_use_fixed,
					case when e.online_coupon_received is null then 0 else e.online_coupon_received end as online_coupon_received,
					case when f.online_buy_merchant_count is null then 0 else f.online_buy_merchant_count end as online_buy_merchant_count,
					case when g.online_action_merchant_count is null then 0 else g.online_action_merchant_count end as online_action_merchant_count
	from
		(select distinct user_id from wepon_online_feature1) a
	left outer join
		wepon_d1_f7_t1 b  
		on a.user_id=b.user_id
	left outer join
		wepon_d1_f7_t2 c  
		on a.user_id=c.user_id
	left outer join
		wepon_d1_f7_t3 d 
		on a.user_id=d.user_id
	left outer join
		wepon_d1_f7_t4 e
		on a.user_id=e.user_id
	left outer join
		wepon_d1_f7_t5 f
		on a.user_id=f.user_id
	left outer join
		wepon_d1_f7_t6 g
		on a.user_id=g.user_id
)t;

	

-- 5. other feature:（label 窗口提取的特征）
--       this_month_user_receive_all_coupon_count
--       this_month_user_receive_same_coupon_count
--       this_day_user_receive_all_coupon_count
--       this_day_user_receive_same_coupon_count
--       this_month_user_receive_same_coupon_lastone
--       this_month_user_receive_same_coupon_firstone
--       商家有交集的用户数目 label_merchant_user_count
--       用户有交集的商家数目     label_user_merchant_count
--       商家发出的所有优惠券数目  label_merchant_coupon_count
--       商家发出的所有优惠券种类数目  label_merchant_coupon_type_count
--       用户领取该商家的所有优惠券数目  label_user_merchant_coupon_count
--       用户在此次优惠券之后还领取了多少该优惠券   label_same_coupon_count_later
--       用户在此次优惠券之后还领取了多少优惠券     label_coupon_count_later


-- ##############  for dataset3  ################### 
create table wepon_d3_f5_t1 as 
select user_id,sum(cnt) as this_month_user_receive_all_coupon_count from
(
	select user_id,1 as cnt from wepon_dataset3
)t 
group by user_id;

create table wepon_d3_f5_t2 as 
select user_id,coupon_id,sum(cnt) as this_month_user_receive_same_coupon_count from
(
	select user_id,coupon_id,1 as cnt from wepon_dataset3
)t 
group by user_id,coupon_id;

create table wepon_d3_f5_t3 as 
select user_id,date_received,sum(cnt) as this_day_user_receive_all_coupon_count from
(
	select user_id,date_received,1 as cnt from wepon_dataset3
)t 
group by user_id,date_received;

create table wepon_d3_f5_t4 as 
select user_id,coupon_id,date_received,sum(cnt) as this_day_user_receive_same_coupon_count from
(
	select user_id,coupon_id,date_received,1 as cnt from wepon_dataset3
)t 
group by user_id,coupon_id,date_received;

create table wepon_d3_f5_temp as
select user_id,coupon_id,max(date_received) as max_date_received, min(date_received) as min_date_received from
(
  select a.user_id,a.coupon_id,a.date_received from
	(select user_id,coupon_id,date_received from wepon_dataset3)a
	join
	(select user_id,coupon_id from wepon_d3_f5_t2 where this_month_user_receive_same_coupon_count>1)b --领取过同张优惠卷多次的
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t 
group by user_id,coupon_id;


create table wepon_d3_f5_t5 as
select user_id,coupon_id,merchant_id,date_received,
       case when date_received=max_date_received then 1
	        when max_date_received is null then -1  -- 只领取过一次的
			else 0 end as this_month_user_receive_same_coupon_lastone,
		case when date_received=min_date_received then 1
	        when min_date_received is null then -1  -- 只领取过一次的
			else 0 end as this_month_user_receive_same_coupon_firstone
from
(
  select a.user_id,a.coupon_id,a.merchant_id,a.date_received,b.max_date_received,b.min_date_received
  from wepon_dataset3 a left outer join wepon_d3_f5_temp b
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t;

create table wepon_d3_f5_t6 as
select merchant_id,count(*) as label_merchant_user_count from
(
	select distinct merchant_id,user_id from wepon_dataset3
)t 
group by merchant_id;

create table wepon_d3_f5_t7 as
select user_id,count(*) as label_user_merchant_count from
(
	select distinct merchant_id,user_id from wepon_dataset3
)t 
group by user_id;


create table wepon_d3_f5_t8 as select merchant_id,count(*) as label_merchant_coupon_count from wepon_dataset3 group by merchant_id;

create table wepon_d3_f5_t9 as select merchant_id,count(*) as label_merchant_coupon_type_count from 
(select distinct merchant_id,coupon_id from wepon_dataset3)t
group by merchant_id;

create table wepon_d3_f5_t10 as
select user_id,count(*) as label_user_merchant_coupon_count from
(
	select merchant_id,user_id from wepon_dataset3
)t 
group by user_id;


create table wepon_d3_f5_t11 as    -- 用户在此次优惠券之后还领取了多少该优惠券   label_same_coupon_count_later  （实现时，先对每天“同用户同优惠卷”去重）
select user_id,coupon_id,date_received,label_same_coupon_count_later-1 as label_same_coupon_count_later from
(
  select user_id,coupon_id,date_received,row_number() over (partition by user_id,coupon_id order by date_received desc) as label_same_coupon_count_later from 
  (
	  select distinct user_id,coupon_id,date_received from wepon_dataset3 
  )t
)tt;


create table wepon_d3_f5_t12 as    --用户在此次优惠券之后还领取了多少优惠券     label_coupon_count_later  （实现时，先对每天“同用户同天”去重）
select user_id,date_received,label_coupon_count_later-1 as label_coupon_count_later from
(
  select user_id,date_received,row_number() over (partition by user_id order by date_received desc) as label_coupon_count_later from 
  (
	  select distinct user_id,date_received from wepon_dataset3 
  )t
)tt;
 


-- 合并各个特征
create table wepon_d3_f5 as
select u.*,v.label_coupon_count_later from
(
	select s.*,t.label_same_coupon_count_later from
	(
		select q.*,r.label_user_merchant_coupon_count from
		(
			select o.*,p.label_merchant_coupon_type_count from
			(
				select m.*,n.label_merchant_coupon_count from
				(
					select k.*,l.label_user_merchant_count from
					(
					  select i.*,j.label_merchant_user_count from
					  (
						select g.*,h.this_day_user_receive_same_coupon_count from
						(
							select e.*,f.this_day_user_receive_all_coupon_count from
							(
							  select c.*,d.this_month_user_receive_same_coupon_count from
							  (
								select a.*,b.this_month_user_receive_all_coupon_count from
								wepon_d3_f5_t5 a join wepon_d3_f5_t1 b 
								on a.user_id=b.user_id
							  )c join wepon_d3_f5_t2 d 
							  on c.user_id=d.user_id and c.coupon_id=d.coupon_id
							)e join wepon_d3_f5_t3 f 
							on e.user_id=f.user_id and e.date_received=f.date_received
						)g join wepon_d3_f5_t4 h 
						on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
					  )i left outer join wepon_d3_f5_t6 j 
					  on i.merchant_id=j.merchant_id
					)k left outer join wepon_d3_f5_t7 l 
					on k.user_id=l.user_id
				)m left outer join wepon_d3_f5_t8 n 
				on m.merchant_id=n.merchant_id
			)o left outer join wepon_d3_f5_t9 p
			on o.merchant_id=p.merchant_id
		)q left outer join wepon_d3_f5_t10 r
		on q.user_id=r.user_id
	)s left outer join wepon_d3_f5_t11 t
	on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
)u  left outer join wepon_d3_f5_t12 v
on u.user_id=v.user_id and u.date_received=v.date_received;


-- ##############  for dataset2  ################### 
create table wepon_d2_f5_t1 as 
select user_id,sum(cnt) as this_month_user_receive_all_coupon_count from
(
	select user_id,1 as cnt from wepon_dataset2
)t 
group by user_id;

create table wepon_d2_f5_t2 as 
select user_id,coupon_id,sum(cnt) as this_month_user_receive_same_coupon_count from
(
	select user_id,coupon_id,1 as cnt from wepon_dataset2
)t 
group by user_id,coupon_id;

create table wepon_d2_f5_t3 as 
select user_id,date_received,sum(cnt) as this_day_user_receive_all_coupon_count from
(
	select user_id,date_received,1 as cnt from wepon_dataset2
)t 
group by user_id,date_received;

create table wepon_d2_f5_t4 as 
select user_id,coupon_id,date_received,sum(cnt) as this_day_user_receive_same_coupon_count from
(
	select user_id,coupon_id,date_received,1 as cnt from wepon_dataset2
)t 
group by user_id,coupon_id,date_received;

create table wepon_d2_f5_temp as
select user_id,coupon_id,max(date_received) as max_date_received, min(date_received) as min_date_received from
(
  select a.user_id,a.coupon_id,a.date_received from
	(select user_id,coupon_id,date_received from wepon_dataset2)a
	join
	(select user_id,coupon_id from wepon_d2_f5_t2 where this_month_user_receive_same_coupon_count>1)b --领取过同张优惠卷多次的
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t 
group by user_id,coupon_id;


create table wepon_d2_f5_t5 as
select user_id,coupon_id,merchant_id,date_received,
       case when date_received=max_date_received then 1
	        when max_date_received is null then -1  -- 只领取过一次的
			else 0 end as this_month_user_receive_same_coupon_lastone,
		case when date_received=min_date_received then 1
	        when min_date_received is null then -1  -- 只领取过一次的
			else 0 end as this_month_user_receive_same_coupon_firstone
from
(
  select a.user_id,a.coupon_id,a.merchant_id,a.date_received,b.max_date_received,b.min_date_received
  from wepon_dataset2 a left outer join wepon_d2_f5_temp b
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t;

create table wepon_d2_f5_t6 as
select merchant_id,count(*) as label_merchant_user_count from
(
	select distinct merchant_id,user_id from wepon_dataset2
)t 
group by merchant_id;

create table wepon_d2_f5_t7 as
select user_id,count(*) as label_user_merchant_count from
(
	select distinct merchant_id,user_id from wepon_dataset2
)t 
group by user_id;


create table wepon_d2_f5_t8 as select merchant_id,count(*) as label_merchant_coupon_count from wepon_dataset2 group by merchant_id;

create table wepon_d2_f5_t9 as select merchant_id,count(*) as label_merchant_coupon_type_count from 
(select distinct merchant_id,coupon_id from wepon_dataset2)t
group by merchant_id;

create table wepon_d2_f5_t10 as
select user_id,count(*) as label_user_merchant_coupon_count from
(
	select merchant_id,user_id from wepon_dataset2
)t 
group by user_id;


create table wepon_d2_f5_t11 as    -- 用户在此次优惠券之后还领取了多少该优惠券   label_same_coupon_count_later  （实现时，先对每天“同用户同优惠卷”去重）
select user_id,coupon_id,date_received,label_same_coupon_count_later-1 as label_same_coupon_count_later from
(
  select user_id,coupon_id,date_received,row_number() over (partition by user_id,coupon_id order by date_received desc) as label_same_coupon_count_later from 
  (
	  select distinct user_id,coupon_id,date_received from wepon_dataset2 
  )t
)tt;


create table wepon_d2_f5_t12 as    --用户在此次优惠券之后还领取了多少优惠券     label_coupon_count_later  （实现时，先对每天“同用户同天”去重）
select user_id,date_received,label_coupon_count_later-1 as label_coupon_count_later from
(
  select user_id,date_received,row_number() over (partition by user_id order by date_received desc) as label_coupon_count_later from 
  (
	  select distinct user_id,date_received from wepon_dataset2 
  )t
)tt;
 


-- 合并各个特征
create table wepon_d2_f5 as
select u.*,v.label_coupon_count_later from
(
	select s.*,t.label_same_coupon_count_later from
	(
		select q.*,r.label_user_merchant_coupon_count from
		(
			select o.*,p.label_merchant_coupon_type_count from
			(
				select m.*,n.label_merchant_coupon_count from
				(
					select k.*,l.label_user_merchant_count from
					(
					  select i.*,j.label_merchant_user_count from
					  (
						select g.*,h.this_day_user_receive_same_coupon_count from
						(
							select e.*,f.this_day_user_receive_all_coupon_count from
							(
							  select c.*,d.this_month_user_receive_same_coupon_count from
							  (
								select a.*,b.this_month_user_receive_all_coupon_count from
								wepon_d2_f5_t5 a join wepon_d2_f5_t1 b 
								on a.user_id=b.user_id
							  )c join wepon_d2_f5_t2 d 
							  on c.user_id=d.user_id and c.coupon_id=d.coupon_id
							)e join wepon_d2_f5_t3 f 
							on e.user_id=f.user_id and e.date_received=f.date_received
						)g join wepon_d2_f5_t4 h 
						on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
					  )i left outer join wepon_d2_f5_t6 j 
					  on i.merchant_id=j.merchant_id
					)k left outer join wepon_d2_f5_t7 l 
					on k.user_id=l.user_id
				)m left outer join wepon_d2_f5_t8 n 
				on m.merchant_id=n.merchant_id
			)o left outer join wepon_d2_f5_t9 p
			on o.merchant_id=p.merchant_id
		)q left outer join wepon_d2_f5_t10 r
		on q.user_id=r.user_id
	)s left outer join wepon_d2_f5_t11 t
	on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
)u  left outer join wepon_d2_f5_t12 v
on u.user_id=v.user_id and u.date_received=v.date_received;



-- ##############  for dataset1  ################### 
create table wepon_d1_f5_t1 as 
select user_id,sum(cnt) as this_month_user_receive_all_coupon_count from
(
	select user_id,1 as cnt from wepon_dataset1
)t 
group by user_id;

create table wepon_d1_f5_t2 as 
select user_id,coupon_id,sum(cnt) as this_month_user_receive_same_coupon_count from
(
	select user_id,coupon_id,1 as cnt from wepon_dataset1
)t 
group by user_id,coupon_id;

create table wepon_d1_f5_t3 as 
select user_id,date_received,sum(cnt) as this_day_user_receive_all_coupon_count from
(
	select user_id,date_received,1 as cnt from wepon_dataset1
)t 
group by user_id,date_received;

create table wepon_d1_f5_t4 as 
select user_id,coupon_id,date_received,sum(cnt) as this_day_user_receive_same_coupon_count from
(
	select user_id,coupon_id,date_received,1 as cnt from wepon_dataset1
)t 
group by user_id,coupon_id,date_received;

create table wepon_d1_f5_temp as
select user_id,coupon_id,max(date_received) as max_date_received, min(date_received) as min_date_received from
(
  select a.user_id,a.coupon_id,a.date_received from
	(select user_id,coupon_id,date_received from wepon_dataset1)a
	join
	(select user_id,coupon_id from wepon_d1_f5_t2 where this_month_user_receive_same_coupon_count>1)b --领取过同张优惠卷多次的
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t 
group by user_id,coupon_id;



create table wepon_d1_f5_t5 as
select user_id,coupon_id,merchant_id,date_received,
       case when date_received=max_date_received then 1
	        when max_date_received is null then -1  -- 只领取过一次的
			else 0 end as this_month_user_receive_same_coupon_lastone,
		case when date_received=min_date_received then 1
	        when min_date_received is null then -1  -- 只领取过一次的
			else 0 end as this_month_user_receive_same_coupon_firstone
from
(
  select a.user_id,a.coupon_id,a.merchant_id,a.date_received,b.max_date_received,b.min_date_received
  from wepon_dataset1 a left outer join wepon_d1_f5_temp b
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t;

create table wepon_d1_f5_t6 as
select merchant_id,count(*) as label_merchant_user_count from
(
	select distinct merchant_id,user_id from wepon_dataset1
)t 
group by merchant_id;

create table wepon_d1_f5_t7 as
select user_id,count(*) as label_user_merchant_count from
(
	select distinct merchant_id,user_id from wepon_dataset1
)t 
group by user_id;


create table wepon_d1_f5_t8 as select merchant_id,count(*) as label_merchant_coupon_count from wepon_dataset1 group by merchant_id;

create table wepon_d1_f5_t9 as select merchant_id,count(*) as label_merchant_coupon_type_count from 
(select distinct merchant_id,coupon_id from wepon_dataset1)t
group by merchant_id;

create table wepon_d1_f5_t10 as
select user_id,count(*) as label_user_merchant_coupon_count from
(
	select merchant_id,user_id from wepon_dataset1
)t 
group by user_id;


create table wepon_d1_f5_t11 as    -- 用户在此次优惠券之后还领取了多少该优惠券   label_same_coupon_count_later  （实现时，先对每天“同用户同优惠卷”去重）
select user_id,coupon_id,date_received,label_same_coupon_count_later-1 as label_same_coupon_count_later from
(
  select user_id,coupon_id,date_received,row_number() over (partition by user_id,coupon_id order by date_received desc) as label_same_coupon_count_later from 
  (
	  select distinct user_id,coupon_id,date_received from wepon_dataset1
  )t
)tt;


create table wepon_d1_f5_t12 as    --用户在此次优惠券之后还领取了多少优惠券     label_coupon_count_later  （实现时，先对每天“同用户同天”去重）
select user_id,date_received,label_coupon_count_later-1 as label_coupon_count_later from
(
  select user_id,date_received,row_number() over (partition by user_id order by date_received desc) as label_coupon_count_later from 
  (
	  select distinct user_id,date_received from wepon_dataset1 
  )t
)tt;
 


-- 合并各个特征
create table wepon_d1_f5 as
select u.*,v.label_coupon_count_later from
(
	select s.*,t.label_same_coupon_count_later from
	(
		select q.*,r.label_user_merchant_coupon_count from
		(
			select o.*,p.label_merchant_coupon_type_count from
			(
				select m.*,n.label_merchant_coupon_count from
				(
					select k.*,l.label_user_merchant_count from
					(
					  select i.*,j.label_merchant_user_count from
					  (
						select g.*,h.this_day_user_receive_same_coupon_count from
						(
							select e.*,f.this_day_user_receive_all_coupon_count from
							(
							  select c.*,d.this_month_user_receive_same_coupon_count from
							  (
								select a.*,b.this_month_user_receive_all_coupon_count from
								wepon_d1_f5_t5 a join wepon_d1_f5_t1 b 
								on a.user_id=b.user_id
							  )c join wepon_d1_f5_t2 d 
							  on c.user_id=d.user_id and c.coupon_id=d.coupon_id
							)e join wepon_d1_f5_t3 f 
							on e.user_id=f.user_id and e.date_received=f.date_received
						)g join wepon_d1_f5_t4 h 
						on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
					  )i left outer join wepon_d1_f5_t6 j 
					  on i.merchant_id=j.merchant_id
					)k left outer join wepon_d1_f5_t7 l 
					on k.user_id=l.user_id
				)m left outer join wepon_d1_f5_t8 n 
				on m.merchant_id=n.merchant_id
			)o left outer join wepon_d1_f5_t9 p
			on o.merchant_id=p.merchant_id
		)q left outer join wepon_d1_f5_t10 r
		on q.user_id=r.user_id
	)s left outer join wepon_d1_f5_t11 t
	on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
)u  left outer join wepon_d1_f5_t12 v
on u.user_id=v.user_id and u.date_received=v.date_received;





-- 2.coupon related: 
--       discount_rate. discount_man. discount_jian. is_man_jian
--       day_of_week,day_of_month. (date_received)
--             label窗里的coupon，在特征窗中被消费过的数目  label_coupon_feature_buy_count
--             label窗里的coupon，在特征窗中被领取过的数目  label_coupon_feature_receive_count
--             label窗里的coupon，在特征窗中的核销率  label_coupon_feature_rate = label_coupon_feature_buy_count/label_coupon_feature_receive_count


-- ###################   for dataset3  ################### 
create table wepon_d3_f2_t1 as
select t.user_id,t.coupon_id,t.merchant_id,t.date_received,t.days_distance,t.day_of_week,t.day_of_month,t.is_man_jian,t.discount_man,t.discount_jian,t.distance,
	  case when is_man_jian=1 then 1.0 - discount_jian/discount_man else discount_rate end as discount_rate
from
(
  select user_id,coupon_id,merchant_id,date_received,discount_rate,
          case when distance="null" then -1 else cast(distance as bigint) end as distance,
		  datediff(to_date(date_received,"yyyymmdd"),to_date("20160630","yyyymmdd"),"dd") as days_distance,
		  weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
		  cast(substr(date_received,7,2) as bigint) as day_of_month,
		  case when instr(discount_rate,":")=0 then 0 else 1 end as is_man_jian,
		  case when instr(discount_rate,":")=0 then -1 else split_part(discount_rate,":",1) end as discount_man,
		  case when instr(discount_rate,":")=0 then -1 else split_part(discount_rate,":",2) end as discount_jian
  from wepon_dataset3
)t;

create table wepon_d3_f2_t2 as
select coupon_id,sum(cnt) as coupon_count
from (select coupon_id,1 as cnt from wepon_dataset3)t 
group by coupon_id;


create table wepon_d3_f2_t3 as
select coupon_id,sum(cnt) as label_coupon_feature_receive_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset3)a  left outer join (select coupon_id,1 as cnt from wepon_feature3 )b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;

create table wepon_d3_f2_t4 as
select coupon_id,sum(cnt) as label_coupon_feature_buy_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset3)a  left outer join (select coupon_id,1 as cnt from wepon_feature3 where coupon_id!="null" and date_pay!="null")b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;


create table wepon_d3_f2 as 
select e.*,f.label_coupon_feature_buy_count,
		  case when e.label_coupon_feature_receive_count=0 then -1 else f.label_coupon_feature_buy_count/e.label_coupon_feature_receive_count end as label_coupon_feature_rate
from
(
  select c.*,d.label_coupon_feature_receive_count from
  (
	select a.user_id,a.coupon_id,a.merchant_id,a.date_received,a.days_distance,a.day_of_week,a.day_of_month,a.is_man_jian,a.distance,
		  cast(a.discount_man as double) as discount_man,cast(a.discount_jian as double ) as discount_jian,cast(a.discount_rate as double) as discount_rate,b.coupon_count 
	from wepon_d3_f2_t1 a join wepon_d3_f2_t2 b 
	on a.coupon_id=b.coupon_id
  )c left outer join wepon_d3_f2_t3 d 
  on c.coupon_id=d.coupon_id
)e left outer join wepon_d3_f2_t4 f 
on e.coupon_id=f.coupon_id;

-- ###################   for dataset2  ################### 
create table wepon_d2_f2_t1 as
select t.user_id,t.coupon_id,t.merchant_id,t.date_received,t.date_pay,t.days_distance,t.day_of_week,t.day_of_month,t.is_man_jian,t.discount_man,t.discount_jian,t.distance,
	  case when is_man_jian=1 then 1.0 - discount_jian/discount_man else discount_rate end as discount_rate
from
(
  select user_id,coupon_id,merchant_id,date_received,date_pay,discount_rate,
  	      case when distance="null" then -1 else cast(distance as bigint) end as distance,
		  datediff(to_date(date_received,"yyyymmdd"),to_date("20160514","yyyymmdd"),"dd") as days_distance,
		  weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
		  cast(substr(date_received,7,2) as bigint) as day_of_month,
		  case when instr(discount_rate,":")=0 then 0 else 1 end as is_man_jian,
		  case when instr(discount_rate,":")=0 then -1 else split_part(discount_rate,":",1) end as discount_man,
		  case when instr(discount_rate,":")=0 then -1 else split_part(discount_rate,":",2) end as discount_jian
  from wepon_dataset2
)t;

create table wepon_d2_f2_t2 as
select coupon_id,sum(cnt) as coupon_count
from (select coupon_id,1 as cnt from wepon_dataset2)t 
group by coupon_id;


create table wepon_d2_f2_t3 as
select coupon_id,sum(cnt) as label_coupon_feature_receive_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset2)a  left outer join (select coupon_id,1 as cnt from wepon_feature2 )b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;

create table wepon_d2_f2_t4 as
select coupon_id,sum(cnt) as label_coupon_feature_buy_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset2)a  left outer join (select coupon_id,1 as cnt from wepon_feature2 where coupon_id!="null" and date_pay!="null")b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;


create table wepon_d2_f2 as 
select e.*,f.label_coupon_feature_buy_count,
		  case when e.label_coupon_feature_receive_count=0 then -1 else f.label_coupon_feature_buy_count/e.label_coupon_feature_receive_count end as label_coupon_feature_rate
from
(
  select c.*,d.label_coupon_feature_receive_count from
  (
	select a.user_id,a.coupon_id,a.merchant_id,a.date_received,a.date_pay,a.days_distance,a.day_of_week,a.day_of_month,a.is_man_jian,a.distance,
		  cast(a.discount_man as double) as discount_man,cast(a.discount_jian as double ) as discount_jian,cast(a.discount_rate as double) as discount_rate,b.coupon_count 
	from wepon_d2_f2_t1 a join wepon_d2_f2_t2 b 
	on a.coupon_id=b.coupon_id
  )c left outer join wepon_d2_f2_t3 d 
  on c.coupon_id=d.coupon_id
)e left outer join wepon_d2_f2_t4 f 
on e.coupon_id=f.coupon_id;


-- ###################   for dataset1  ################### 
create table wepon_d1_f2_t1 as
select t.user_id,t.coupon_id,t.merchant_id,t.date_received,t.date_pay,t.days_distance,t.day_of_week,t.day_of_month,t.is_man_jian,t.discount_man,t.discount_jian,t.distance,
	  case when is_man_jian=1 then 1.0 - discount_jian/discount_man else discount_rate end as discount_rate
from
(
  select user_id,coupon_id,merchant_id,date_received,date_pay,discount_rate,
          case when distance="null" then -1 else cast(distance as bigint) end as distance,
		  datediff(to_date(date_received,"yyyymmdd"),to_date("20160413","yyyymmdd"),"dd") as days_distance,
		  weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
		  cast(substr(date_received,7,2) as bigint) as day_of_month,
		  case when instr(discount_rate,":")=0 then 0 else 1 end as is_man_jian,
		  case when instr(discount_rate,":")=0 then -1 else split_part(discount_rate,":",1) end as discount_man,
		  case when instr(discount_rate,":")=0 then -1 else split_part(discount_rate,":",2) end as discount_jian
  from wepon_dataset1
)t;

create table wepon_d1_f2_t2 as
select coupon_id,sum(cnt) as coupon_count
from (select coupon_id,1 as cnt from wepon_dataset1)t 
group by coupon_id;


create table wepon_d1_f2_t3 as
select coupon_id,sum(cnt) as label_coupon_feature_receive_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset1)a  left outer join (select coupon_id,1 as cnt from wepon_feature1 )b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;

create table wepon_d1_f2_t4 as
select coupon_id,sum(cnt) as label_coupon_feature_buy_count from
(
  select a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct coupon_id from wepon_dataset1)a  left outer join (select coupon_id,1 as cnt from wepon_feature1 where coupon_id!="null" and date_pay!="null")b 
  on a.coupon_id=b.coupon_id
)t
group by coupon_id;


create table wepon_d1_f2 as 
select e.*,f.label_coupon_feature_buy_count,
		  case when e.label_coupon_feature_receive_count=0 then -1 else f.label_coupon_feature_buy_count/e.label_coupon_feature_receive_count end as label_coupon_feature_rate
from
(
  select c.*,d.label_coupon_feature_receive_count from
  (
	select a.user_id,a.coupon_id,a.merchant_id,a.date_received,a.date_pay,a.days_distance,a.day_of_week,a.day_of_month,a.is_man_jian,a.distance,
		  cast(a.discount_man as double) as discount_man,cast(a.discount_jian as double ) as discount_jian,cast(a.discount_rate as double) as discount_rate,b.coupon_count 
	from wepon_d1_f2_t1 a join wepon_d1_f2_t2 b 
	on a.coupon_id=b.coupon_id
  )c left outer join wepon_d1_f2_t3 d 
  on c.coupon_id=d.coupon_id
)e left outer join wepon_d1_f2_t4 f 
on e.coupon_id=f.coupon_id;





-- 1.merchant related: 
--       sales_use_coupon. total_coupon  distinct_coupon_count
--       transform_rate = sales_use_coupon/total_coupon.
--       merchant_avg_distance,merchant_min_distance,merchant_max_distance of those use coupon 
--       total_sales.  coupon_rate = sales_use_coupon/total_sales.  
--       消费过该商家的不同用户数量   merchant_user_buy_count

-- ##############  for dataset3  ################### 
create table wepon_merchant3 as select merchant_id,user_id,coupon_id,distance,date_received,date_pay from wepon_feature3;

create table wepon_d3_f1_t1 as 
select merchant_id,sum(cnt) as total_sales from
(
	select merchant_id,1 as cnt from wepon_merchant3 where date_pay!="null"
)t 
group by merchant_id;

create table wepon_d3_f1_t2 as 
select merchant_id,sum(cnt) as sales_use_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant3 where date_pay!="null" and coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d3_f1_t3 as 
select merchant_id,sum(cnt) as total_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant3 where coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d3_f1_t4 as 
select merchant_id,count(*) as distinct_coupon_count from
(
	select distinct merchant_id,coupon_id from wepon_merchant3 where coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d3_f1_t5 as 
select merchant_id,avg(distance) as merchant_avg_distance,median(distance) as merchant_median_distance,
       max(distance) as merchant_max_distance,min(distance) as merchant_min_distance from
(
	select merchant_id,distance from wepon_merchant3 where date_pay!="null" and coupon_id!="null" and distance!="null"
)t
group by merchant_id;


create table wepon_d3_f1_t6 as 
select merchant_id,count(*) as merchant_user_buy_count from
(
	  select distinct merchant_id,user_id from wepon_merchant3 where date_pay!="null"
)t 
group by merchant_id;



create table wepon_d3_f1 as
select merchant_id,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,cast(merchant_max_distance as bigint) as merchant_max_distance,cast(merchant_min_distance as bigint ) as merchant_min_distance,
	  merchant_user_buy_count,sales_use_coupon,transform_rate,coupon_rate,case when total_coupon is null then 0.0 else total_coupon end as total_coupon,case when total_sales is null then 0.0 else total_sales end as total_sales
from
(
	select tt.*,tt.sales_use_coupon/tt.total_coupon as transform_rate,tt.sales_use_coupon/tt.total_sales as coupon_rate from
	(
	  select merchant_id,total_sales,total_coupon,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_min_distance,merchant_user_buy_count,
			 case when sales_use_coupon is null then 0.0 else sales_use_coupon end as sales_use_coupon
	  from
	  (
	      select k.*,l.merchant_user_buy_count from
		  (
			select i.*,j.merchant_avg_distance,j.merchant_median_distance,j.merchant_max_distance,j.merchant_min_distance from
			(
			  select g.*,h.distinct_coupon_count from
			  (
				select e.*,f.total_coupon from
				(
				  select c.*,d.sales_use_coupon from
				  (
					select a.*,b.total_sales from
					(select distinct merchant_id from wepon_merchant3) a left outer join wepon_d3_f1_t1 b 
					on a.merchant_id=b.merchant_id
				  )c left outer join wepon_d3_f1_t2 d 
				  on c.merchant_id=d.merchant_id
				)e left outer join wepon_d3_f1_t3 f 
				on e.merchant_id=f.merchant_id
			  )g left outer join wepon_d3_f1_t4 h 
			  on g.merchant_id=h.merchant_id
			)i left outer join wepon_d3_f1_t5 j 
			on i.merchant_id=j.merchant_id
		  )k left outer join wepon_d3_f1_t6 l 
		  on k.merchant_id=l.merchant_id
	  )t
	)tt
)ttt;





-- ##############  for dataset2  ################### 
create table wepon_merchant2 as select merchant_id,user_id,coupon_id,distance,date_received,date_pay from wepon_feature2;

create table wepon_d2_f1_t1 as 
select merchant_id,sum(cnt) as total_sales from
(
	select merchant_id,1 as cnt from wepon_merchant2 where date_pay!="null"
)t 
group by merchant_id;

create table wepon_d2_f1_t2 as 
select merchant_id,sum(cnt) as sales_use_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant2 where date_pay!="null" and coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d2_f1_t3 as 
select merchant_id,sum(cnt) as total_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant2 where coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d2_f1_t4 as 
select merchant_id,count(*) as distinct_coupon_count from
(
	select distinct merchant_id,coupon_id from wepon_merchant2 where coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d2_f1_t5 as 
select merchant_id,avg(distance) as merchant_avg_distance,median(distance) as merchant_median_distance,
       max(distance) as merchant_max_distance,min(distance) as merchant_min_distance from
(
	select merchant_id,distance from wepon_merchant2 where date_pay!="null" and coupon_id!="null" and distance!="null"
)t
group by merchant_id;


create table wepon_d2_f1_t6 as 
select merchant_id,count(*) as merchant_user_buy_count from
(
	  select distinct merchant_id,user_id from wepon_merchant2 where date_pay!="null"
)t 
group by merchant_id;


create table wepon_d2_f1 as
select merchant_id,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,cast(merchant_max_distance as bigint) as merchant_max_distance,cast(merchant_min_distance as bigint ) as merchant_min_distance,
	  merchant_user_buy_count,sales_use_coupon,transform_rate,coupon_rate,case when total_coupon is null then 0.0 else total_coupon end as total_coupon,case when total_sales is null then 0.0 else total_sales end as total_sales
from
(
	select tt.*,tt.sales_use_coupon/tt.total_coupon as transform_rate,tt.sales_use_coupon/tt.total_sales as coupon_rate from
	(
	  select merchant_id,total_sales,total_coupon,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_min_distance,merchant_user_buy_count,
			 case when sales_use_coupon is null then 0.0 else sales_use_coupon end as sales_use_coupon
	  from
	  (
	      select k.*,l.merchant_user_buy_count from
		  (
			select i.*,j.merchant_avg_distance,j.merchant_median_distance,j.merchant_max_distance,j.merchant_min_distance from
			(
			  select g.*,h.distinct_coupon_count from
			  (
				select e.*,f.total_coupon from
				(
				  select c.*,d.sales_use_coupon from
				  (
					select a.*,b.total_sales from
					(select distinct merchant_id from wepon_merchant2) a left outer join wepon_d2_f1_t1 b 
					on a.merchant_id=b.merchant_id
				  )c left outer join wepon_d2_f1_t2 d 
				  on c.merchant_id=d.merchant_id
				)e left outer join wepon_d2_f1_t3 f 
				on e.merchant_id=f.merchant_id
			  )g left outer join wepon_d2_f1_t4 h 
			  on g.merchant_id=h.merchant_id
			)i left outer join wepon_d2_f1_t5 j 
			on i.merchant_id=j.merchant_id
		  )k left outer join wepon_d2_f1_t6 l 
		  on k.merchant_id=l.merchant_id
	  )t
	)tt
)ttt;


-- ##############  for dataset1  ################### 
create table wepon_merchant1 as select merchant_id,user_id,coupon_id,distance,date_received,date_pay from wepon_feature1;

create table wepon_d1_f1_t1 as 
select merchant_id,sum(cnt) as total_sales from
(
	select merchant_id,1 as cnt from wepon_merchant1 where date_pay!="null"
)t 
group by merchant_id;

create table wepon_d1_f1_t2 as 
select merchant_id,sum(cnt) as sales_use_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant1 where date_pay!="null" and coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d1_f1_t3 as 
select merchant_id,sum(cnt) as total_coupon from
(
	select merchant_id,1 as cnt from wepon_merchant1 where coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d1_f1_t4 as 
select merchant_id,count(*) as distinct_coupon_count from
(
	select distinct merchant_id,coupon_id from wepon_merchant1 where coupon_id!="null"
)t 
group by merchant_id;

create table wepon_d1_f1_t5 as 
select merchant_id,avg(distance) as merchant_avg_distance,median(distance) as merchant_median_distance,
       max(distance) as merchant_max_distance,min(distance) as merchant_min_distance from
(
	select merchant_id,distance from wepon_merchant1 where date_pay!="null" and coupon_id!="null" and distance!="null"
)t
group by merchant_id;


create table wepon_d1_f1_t6 as 
select merchant_id,count(*) as merchant_user_buy_count from
(
	  select distinct merchant_id,user_id from wepon_merchant1 where date_pay!="null"
)t 
group by merchant_id;


create table wepon_d1_f1 as
select merchant_id,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,cast(merchant_max_distance as bigint) as merchant_max_distance,cast(merchant_min_distance as bigint ) as merchant_min_distance,
	  merchant_user_buy_count,sales_use_coupon,transform_rate,coupon_rate,case when total_coupon is null then 0.0 else total_coupon end as total_coupon,case when total_sales is null then 0.0 else total_sales end as total_sales
from
(
	select tt.*,tt.sales_use_coupon/tt.total_coupon as transform_rate,tt.sales_use_coupon/tt.total_sales as coupon_rate from
	(
	  select merchant_id,total_sales,total_coupon,distinct_coupon_count,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_min_distance,merchant_user_buy_count,
			 case when sales_use_coupon is null then 0.0 else sales_use_coupon end as sales_use_coupon
	  from
	  (
	      select k.*,l.merchant_user_buy_count from
		  (
			select i.*,j.merchant_avg_distance,j.merchant_median_distance,j.merchant_max_distance,j.merchant_min_distance from
			(
			  select g.*,h.distinct_coupon_count from
			  (
				select e.*,f.total_coupon from
				(
				  select c.*,d.sales_use_coupon from
				  (
					select a.*,b.total_sales from
					(select distinct merchant_id from wepon_merchant1) a left outer join wepon_d1_f1_t1 b 
					on a.merchant_id=b.merchant_id
				  )c left outer join wepon_d1_f1_t2 d 
				  on c.merchant_id=d.merchant_id
				)e left outer join wepon_d1_f1_t3 f 
				on e.merchant_id=f.merchant_id
			  )g left outer join wepon_d1_f1_t4 h 
			  on g.merchant_id=h.merchant_id
			)i left outer join wepon_d1_f1_t5 j 
			on i.merchant_id=j.merchant_id
		  )k left outer join wepon_d1_f1_t6 l 
		  on k.merchant_id=l.merchant_id
	  )t
	)tt
)ttt;




-- 3.user related: 
--       count_merchant, distance. 
--       user_avg_distance, user_min_distance,user_max_distance. 
--       buy_use_coupon. buy_total. coupon_received.
--      avg_diff_date_datereceived. min_diff_date_datereceived. max_diff_date_datereceived.  
--      buy_use_coupon_rate = buy_use_coupon/buy_total
--       user_coupon_transform_rate = buy_use_coupon/coupon_received. 


-- ###################   for dataset3  ################### 
create table wepon_user3 as select user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date_pay from wepon_feature3;

create table wepon_d3_f3_t1 as 
select user_id,count(*) as count_merchant from
(
	select distinct user_id,merchant_id from wepon_user3 where date_pay!="null"
)t 
group by user_id;

create table wepon_d3_f3_t2 as
select user_id,avg(distance) as user_avg_distance,min(distance) as user_min_distance,max(distance) as user_max_distance,median(distance) as user_median_distance from
(
	select user_id,distance from wepon_user3 where date_pay!="null" and coupon_id!="null" and distance!="null"
)t 
group by user_id;

create table wepon_d3_f3_t3 as
select user_id,count(*) as buy_use_coupon
(
	select user_id from wepon_user3 where date_pay!="null" and coupon_id!="null"
)t 
group by user_id;

create table wepon_d3_f3_t4 as
select user_id,count(*) as buy_total
(
	select user_id from wepon_user3 where date_pay!="null"
)t 
group by user_id;

create table wepon_d3_f3_t5 as
select user_id,count(*) as coupon_received
(
	select user_id from wepon_user3 where coupon_id!="null"
)t 
group by user_id;

create table wepon_d3_f3_t6 as 
select user_id,avg(day_gap) as avg_diff_date_datereceived,min(day_gap) as min_diff_date_datereceived,max(day_gap) as max_diff_date_datereceived from
(
  select user_id,datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") as day_gap 
  from wepon_user3 
  where date_pay!="null" and date_received!="null"
)t 
group by user_id;


create table wepon_d3_f3 as
select user_id,user_avg_distance,cast(user_min_distance as double) as user_min_distance,cast(user_max_distance as double) as user_max_distance,user_median_distance,
       abs(avg_diff_date_datereceived) as avg_diff_date_datereceived,abs(min_diff_date_datereceived) as min_diff_date_datereceived,abs(max_diff_date_datereceived) max_diff_date_datereceived,
       buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,
	   case when count_merchant is null then 0.0 else count_merchant end as count_merchant,
	   case when buy_total is null then 0.0 else buy_total end as buy_total,
	   case when coupon_received is null then 0.0 else coupon_received end as coupon_received
from
(
	select tt.*,tt.buy_use_coupon/tt.buy_total as buy_use_coupon_rate,tt.buy_use_coupon/tt.coupon_received as user_coupon_transform_rate from

	(
	  select user_id,count_merchant,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,buy_total,coupon_received,avg_diff_date_datereceived,min_diff_date_datereceived,
			 max_diff_date_datereceived,case when buy_use_coupon is null then 0.0 else buy_use_coupon end as buy_use_coupon
	  from
	  (
		  select k.*,l.avg_diff_date_datereceived,l.min_diff_date_datereceived,l.max_diff_date_datereceived from
		  (
			select i.*,j.coupon_received from
			(
			  select g.*,h.buy_total from
			  (
				select e.*,f.buy_use_coupon from
				(
				  select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance from
				  (
					select a.*,b.count_merchant from
					(select distinct user_id from wepon_user3) a left outer join wepon_d3_f3_t1 b
					on a.user_id=b.user_id
				  )c left outer join wepon_d3_f3_t2 d
				  on c.user_id=d.user_id
				)e left outer join wepon_d3_f3_t3 f
				on e.user_id=f.user_id
			  )g left outer join wepon_d3_f3_t4 h
			  on g.user_id=h.user_id
			)i left outer join wepon_d3_f3_t5 j 
			on i.user_id=j.user_id
		  )k left outer join wepon_d3_f3_t6 l
		  on k.user_id=l.user_id
	  )t
	)tt
)ttt;





-- ###################   for dataset2  ################### 
create table wepon_user2 as select user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date_pay from wepon_feature2;

create table wepon_d2_f3_t1 as 
select user_id,count(*) as count_merchant from
(
	select distinct user_id,merchant_id from wepon_user2 where date_pay!="null"
)t 
group by user_id;

create table wepon_d2_f3_t2 as
select user_id,avg(distance) as user_avg_distance,min(distance) as user_min_distance,max(distance) as user_max_distance,median(distance) as user_median_distance from
(
	select user_id,distance from wepon_user2 where date_pay!="null" and coupon_id!="null" and distance!="null"
)t 
group by user_id;

create table wepon_d2_f3_t3 as
select user_id,count(*) as buy_use_coupon from
(
	select user_id from wepon_user2 where date_pay!="null" and coupon_id!="null"
)t 
group by user_id;

create table wepon_d2_f3_t4 as
select user_id,count(*) as buy_total from
(
	select user_id from wepon_user2 where date_pay!="null"
)t 
group by user_id;

create table wepon_d2_f3_t5 as
select user_id,count(*) as coupon_received from
(
	select user_id from wepon_user2 where coupon_id!="null"
)t 
group by user_id;

create table wepon_d2_f3_t6 as 
select user_id,avg(day_gap) as avg_diff_date_datereceived,min(day_gap) as min_diff_date_datereceived,max(day_gap) as max_diff_date_datereceived from
(
  select user_id,datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") as day_gap 
  from wepon_user2
  where date_pay!="null" and date_received!="null"
)t 
group by user_id;


create table wepon_d2_f3 as
select user_id,user_avg_distance,cast(user_min_distance as double) as user_min_distance,cast(user_max_distance as double) as user_max_distance,user_median_distance,
       abs(avg_diff_date_datereceived) as avg_diff_date_datereceived,abs(min_diff_date_datereceived) as min_diff_date_datereceived,abs(max_diff_date_datereceived) max_diff_date_datereceived,
       buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,
	   case when count_merchant is null then 0.0 else count_merchant end as count_merchant,
	   case when buy_total is null then 0.0 else buy_total end as buy_total,
	   case when coupon_received is null then 0.0 else coupon_received end as coupon_received
from
(
	select tt.*,tt.buy_use_coupon/tt.buy_total as buy_use_coupon_rate,tt.buy_use_coupon/tt.coupon_received as user_coupon_transform_rate from

	(
	  select user_id,count_merchant,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,buy_total,coupon_received,avg_diff_date_datereceived,min_diff_date_datereceived,
			 max_diff_date_datereceived,case when buy_use_coupon is null then 0.0 else buy_use_coupon end as buy_use_coupon
	  from
	  (
		  select k.*,l.avg_diff_date_datereceived,l.min_diff_date_datereceived,l.max_diff_date_datereceived from
		  (
			select i.*,j.coupon_received from
			(
			  select g.*,h.buy_total from
			  (
				select e.*,f.buy_use_coupon from
				(
				  select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance from
				  (
					select a.*,b.count_merchant from
					(select distinct user_id from wepon_user2) a left outer join wepon_d2_f3_t1 b
					on a.user_id=b.user_id
				  )c left outer join wepon_d2_f3_t2 d
				  on c.user_id=d.user_id
				)e left outer join wepon_d2_f3_t3 f
				on e.user_id=f.user_id
			  )g left outer join wepon_d2_f3_t4 h
			  on g.user_id=h.user_id
			)i left outer join wepon_d2_f3_t5 j 
			on i.user_id=j.user_id
		  )k left outer join wepon_d2_f3_t6 l
		  on k.user_id=l.user_id
	  )t
	)tt
)ttt;


-- ###################   for dataset1  ################### 
create table wepon_user1 as select user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date_pay from wepon_feature1;

create table wepon_d1_f3_t1 as 
select user_id,count(*) as count_merchant from
(
	select distinct user_id,merchant_id from wepon_user1 where date_pay!="null"
)t 
group by user_id;

create table wepon_d1_f3_t2 as
select user_id,avg(distance) as user_avg_distance,min(distance) as user_min_distance,max(distance) as user_max_distance,median(distance) as user_median_distance from
(
	select user_id,distance from wepon_user1 where date_pay!="null" and coupon_id!="null" and distance!="null"
)t 
group by user_id;

create table wepon_d1_f3_t3 as
select user_id,count(*) as buy_use_coupon from
(
	select user_id from wepon_user1 where date_pay!="null" and coupon_id!="null"
)t 
group by user_id;

create table wepon_d1_f3_t4 as
select user_id,count(*) as buy_total from
(
	select user_id from wepon_user1 where date_pay!="null"
)t 
group by user_id;

create table wepon_d1_f3_t5 as
select user_id,count(*) as coupon_received from
(
	select user_id from wepon_user1 where coupon_id!="null"
)t 
group by user_id;

create table wepon_d1_f3_t6 as 
select user_id,avg(day_gap) as avg_diff_date_datereceived,min(day_gap) as min_diff_date_datereceived,max(day_gap) as max_diff_date_datereceived from
(
  select user_id,datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") as day_gap 
  from wepon_user1
  where date_pay!="null" and date_received!="null"
)t 
group by user_id;


create table wepon_d1_f3 as
select user_id,user_avg_distance,cast(user_min_distance as double) as user_min_distance,cast(user_max_distance as double) as user_max_distance,user_median_distance,
       abs(avg_diff_date_datereceived) as avg_diff_date_datereceived,abs(min_diff_date_datereceived) as min_diff_date_datereceived,abs(max_diff_date_datereceived) max_diff_date_datereceived,
       buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,
	   case when count_merchant is null then 0.0 else count_merchant end as count_merchant,
	   case when buy_total is null then 0.0 else buy_total end as buy_total,
	   case when coupon_received is null then 0.0 else coupon_received end as coupon_received
from
(
	select tt.*,tt.buy_use_coupon/tt.buy_total as buy_use_coupon_rate,tt.buy_use_coupon/tt.coupon_received as user_coupon_transform_rate from

	(
	  select user_id,count_merchant,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,buy_total,coupon_received,avg_diff_date_datereceived,min_diff_date_datereceived,
			 max_diff_date_datereceived,case when buy_use_coupon is null then 0.0 else buy_use_coupon end as buy_use_coupon
	  from
	  (
		  select k.*,l.avg_diff_date_datereceived,l.min_diff_date_datereceived,l.max_diff_date_datereceived from
		  (
			select i.*,j.coupon_received from
			(
			  select g.*,h.buy_total from
			  (
				select e.*,f.buy_use_coupon from
				(
				  select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance from
				  (
					select a.*,b.count_merchant from
					(select distinct user_id from wepon_user1) a left outer join wepon_d1_f3_t1 b
					on a.user_id=b.user_id
				  )c left outer join wepon_d1_f3_t2 d
				  on c.user_id=d.user_id
				)e left outer join wepon_d1_f3_t3 f
				on e.user_id=f.user_id
			  )g left outer join wepon_d1_f3_t4 h
			  on g.user_id=h.user_id
			)i left outer join wepon_d1_f3_t5 j 
			on i.user_id=j.user_id
		  )k left outer join wepon_d1_f3_t6 l
		  on k.user_id=l.user_id
	  )t
	)tt
)ttt;





-- 4.user_merchant: 
--       user_merchant_buy_total.  user_merchant_received    user_merchant_buy_use_coupon  user_merchant_any  user_merchant_buy_common
--       user_merchant_coupon_transform_rate = user_merchant_buy_use_coupon/user_merchant_received
--       user_merchant_coupon_buy_rate = user_merchant_buy_use_coupon/user_merchant_buy_total
--       user_merchant_common_buy_rate = user_merchant_buy_common/user_merchant_buy_total
--       user_merchant_rate = user_merchant_buy_total/user_merchant_any


-- ###################   for dataset3  ################### 
create table wepon_d3_f4_t1 as 
select user_id,merchant_id,count(*) as user_merchant_buy_total from
( select user_id,merchant_id from wepon_feature3 where date_pay!="null" )t
group by user_id,merchant_id;

create table wepon_d3_f4_t2 as 
select user_id,merchant_id,count(*) as user_merchant_received from
( select user_id,merchant_id from wepon_feature3 where coupon_id!="null" )t
group by user_id,merchant_id;

create table wepon_d3_f4_t3 as 
select user_id,merchant_id,count(*) as user_merchant_buy_use_coupon from
( select user_id,merchant_id from wepon_feature3 where date_pay!="null" and date_received!="null")t
group by user_id,merchant_id;

create table wepon_d3_f4_t4 as 
select user_id,merchant_id,count(*) as user_merchant_any from
( select user_id,merchant_id from wepon_feature3 )t
group by user_id,merchant_id;


create table wepon_d3_f4_t5 as 
select user_id,merchant_id,count(*) as user_merchant_buy_common from
( select user_id,merchant_id from wepon_feature3 where date_pay!="null" and coupon_id=="null" )t
group by user_id,merchant_id;


create table wepon_d3_f4 as 
select user_id,merchant_id,user_merchant_buy_use_coupon,user_merchant_buy_common,
       user_merchant_coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,
	   case when user_merchant_buy_total is null then 0.0 else user_merchant_buy_total end as user_merchant_buy_total,
	   case when user_merchant_received is null then 0.0 else user_merchant_received end as user_merchant_received,
	   case when user_merchant_any is null then 0.0 else user_merchant_any end as user_merchant_any
from
(
  select tt.*,tt.user_merchant_buy_use_coupon/tt.user_merchant_received as user_merchant_coupon_transform_rate,
			  tt.user_merchant_buy_use_coupon/tt.user_merchant_buy_total as user_merchant_coupon_buy_rate,
			  tt.user_merchant_buy_common/tt.user_merchant_buy_total as user_merchant_common_buy_rate,
			  tt.user_merchant_buy_total/tt.user_merchant_any as user_merchant_rate
  from
  (
	  select user_id,merchant_id,user_merchant_buy_total,user_merchant_received,user_merchant_any,
			 case when user_merchant_buy_use_coupon is null then 0.0 else user_merchant_buy_use_coupon end as user_merchant_buy_use_coupon,
			 case when user_merchant_buy_common is null then 0.0 else user_merchant_buy_common end as user_merchant_buy_common
	  from
	  (
		  select i.*,j.user_merchant_buy_common from
		  (
			select g.*,h.user_merchant_any from
			(
			  select e.*,f.user_merchant_buy_use_coupon from
			  (
				select c.*,d.user_merchant_received from
				(
				  select a.*,b.user_merchant_buy_total from
				  (select distinct user_id,merchant_id from wepon_feature3) a left outer join wepon_d3_f4_t1 b 
				  on a.user_id=b.user_id and a.merchant_id=b.merchant_id
				)c left outer join wepon_d3_f4_t2 d 
				on c.user_id=d.user_id and c.merchant_id=d.merchant_id
			  )e left outer join wepon_d3_f4_t3 f 
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g  left outer join wepon_d3_f4_t4 h 
			on g.user_id=h.user_id and g.merchant_id=h.merchant_id
		  )i left outer join wepon_d3_f4_t5 j
		  on i.user_id=j.user_id and i.merchant_id=j.merchant_id
	  )t
  )tt
)ttt;



-- ###################   for dataset2  ################### 
create table wepon_d2_f4_t1 as 
select user_id,merchant_id,count(*) as user_merchant_buy_total from
( select user_id,merchant_id from wepon_feature2 where date_pay!="null" )t
group by user_id,merchant_id;

create table wepon_d2_f4_t2 as 
select user_id,merchant_id,count(*) as user_merchant_received from
( select user_id,merchant_id from wepon_feature2 where coupon_id!="null" )t
group by user_id,merchant_id;

create table wepon_d2_f4_t3 as 
select user_id,merchant_id,count(*) as user_merchant_buy_use_coupon from
( select user_id,merchant_id from wepon_feature2 where date_pay!="null" and date_received!="null")t
group by user_id,merchant_id;

create table wepon_d2_f4_t4 as 
select user_id,merchant_id,count(*) as user_merchant_any from
( select user_id,merchant_id from wepon_feature2 )t
group by user_id,merchant_id;


create table wepon_d2_f4_t5 as 
select user_id,merchant_id,count(*) as user_merchant_buy_common from
( select user_id,merchant_id from wepon_feature2 where date_pay!="null" and coupon_id=="null" )t
group by user_id,merchant_id;


create table wepon_d2_f4 as 
select user_id,merchant_id,user_merchant_buy_use_coupon,user_merchant_buy_common,
       user_merchant_coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,
	   case when user_merchant_buy_total is null then 0.0 else user_merchant_buy_total end as user_merchant_buy_total,
	   case when user_merchant_received is null then 0.0 else user_merchant_received end as user_merchant_received,
	   case when user_merchant_any is null then 0.0 else user_merchant_any end as user_merchant_any
from
(
  select tt.*,tt.user_merchant_buy_use_coupon/tt.user_merchant_received as user_merchant_coupon_transform_rate,
			  tt.user_merchant_buy_use_coupon/tt.user_merchant_buy_total as user_merchant_coupon_buy_rate,
			  tt.user_merchant_buy_common/tt.user_merchant_buy_total as user_merchant_common_buy_rate,
			  tt.user_merchant_buy_total/tt.user_merchant_any as user_merchant_rate
  from
  (
	  select user_id,merchant_id,user_merchant_buy_total,user_merchant_received,user_merchant_any,
			 case when user_merchant_buy_use_coupon is null then 0.0 else user_merchant_buy_use_coupon end as user_merchant_buy_use_coupon,
			 case when user_merchant_buy_common is null then 0.0 else user_merchant_buy_common end as user_merchant_buy_common
	  from
	  (
		  select i.*,j.user_merchant_buy_common from
		  (
			select g.*,h.user_merchant_any from
			(
			  select e.*,f.user_merchant_buy_use_coupon from
			  (
				select c.*,d.user_merchant_received from
				(
				  select a.*,b.user_merchant_buy_total from
				  (select distinct user_id,merchant_id from wepon_feature2) a left outer join wepon_d2_f4_t1 b 
				  on a.user_id=b.user_id and a.merchant_id=b.merchant_id
				)c left outer join wepon_d2_f4_t2 d 
				on c.user_id=d.user_id and c.merchant_id=d.merchant_id
			  )e left outer join wepon_d2_f4_t3 f 
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g  left outer join wepon_d2_f4_t4 h 
			on g.user_id=h.user_id and g.merchant_id=h.merchant_id
		  )i left outer join wepon_d2_f4_t5 j
		  on i.user_id=j.user_id and i.merchant_id=j.merchant_id
	  )t
  )tt
)ttt;



-- ###################   for dataset1  ################### 
create table wepon_d1_f4_t1 as 
select user_id,merchant_id,count(*) as user_merchant_buy_total from
( select user_id,merchant_id from wepon_feature1 where date_pay!="null" )t
group by user_id,merchant_id;

create table wepon_d1_f4_t2 as 
select user_id,merchant_id,count(*) as user_merchant_received from
( select user_id,merchant_id from wepon_feature1 where coupon_id!="null" )t
group by user_id,merchant_id;

create table wepon_d1_f4_t3 as 
select user_id,merchant_id,count(*) as user_merchant_buy_use_coupon from
( select user_id,merchant_id from wepon_feature1 where date_pay!="null" and date_received!="null")t
group by user_id,merchant_id;

create table wepon_d1_f4_t4 as 
select user_id,merchant_id,count(*) as user_merchant_any from
( select user_id,merchant_id from wepon_feature1 )t
group by user_id,merchant_id;


create table wepon_d1_f4_t5 as 
select user_id,merchant_id,count(*) as user_merchant_buy_common from
( select user_id,merchant_id from wepon_feature1 where date_pay!="null" and coupon_id=="null" )t
group by user_id,merchant_id;


create table wepon_d1_f4 as 
select user_id,merchant_id,user_merchant_buy_use_coupon,user_merchant_buy_common,
       user_merchant_coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,
	   case when user_merchant_buy_total is null then 0.0 else user_merchant_buy_total end as user_merchant_buy_total,
	   case when user_merchant_received is null then 0.0 else user_merchant_received end as user_merchant_received,
	   case when user_merchant_any is null then 0.0 else user_merchant_any end as user_merchant_any
from
(
  select tt.*,tt.user_merchant_buy_use_coupon/tt.user_merchant_received as user_merchant_coupon_transform_rate,
			  tt.user_merchant_buy_use_coupon/tt.user_merchant_buy_total as user_merchant_coupon_buy_rate,
			  tt.user_merchant_buy_common/tt.user_merchant_buy_total as user_merchant_common_buy_rate,
			  tt.user_merchant_buy_total/tt.user_merchant_any as user_merchant_rate
  from
  (
	  select user_id,merchant_id,user_merchant_buy_total,user_merchant_received,user_merchant_any,
			 case when user_merchant_buy_use_coupon is null then 0.0 else user_merchant_buy_use_coupon end as user_merchant_buy_use_coupon,
			 case when user_merchant_buy_common is null then 0.0 else user_merchant_buy_common end as user_merchant_buy_common
	  from
	  (
		  select i.*,j.user_merchant_buy_common from
		  (
			select g.*,h.user_merchant_any from
			(
			  select e.*,f.user_merchant_buy_use_coupon from
			  (
				select c.*,d.user_merchant_received from
				(
				  select a.*,b.user_merchant_buy_total from
				  (select distinct user_id,merchant_id from wepon_feature1) a left outer join wepon_d1_f4_t1 b 
				  on a.user_id=b.user_id and a.merchant_id=b.merchant_id
				)c left outer join wepon_d1_f4_t2 d 
				on c.user_id=d.user_id and c.merchant_id=d.merchant_id
			  )e left outer join wepon_d1_f4_t3 f 
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g  left outer join wepon_d1_f4_t4 h 
			on g.user_id=h.user_id and g.merchant_id=h.merchant_id
		  )i left outer join wepon_d1_f4_t5 j
		  on i.user_id=j.user_id and i.merchant_id=j.merchant_id
	  )t
  )tt
)ttt;




-- 6. user_coupon:
--       对label窗里的user_coupon，特征窗里用户领取过该coupon几次   label_user_coupon_feature_receive_count
--       对label窗里的user_coupon，特征窗里用户用该coupon消费过几次   label_user_coupon_feature_buy_count
--       对label窗里的user_coupon，特征窗里用户对该coupon的核销率   label_user_coupon_feature_rate = label_user_coupon_feature_buy_count/label_user_coupon_feature_receive_count

-- ###################   for dataset3  ################### 
create table wepon_d3_f6_t1 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_receive_count from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset3)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature3 )b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

create table wepon_d3_f6_t2 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_buy_count from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset3)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature3 where date_pay!="null" and coupon_id!="null")b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

create table wepon_d3_f6 as 
select a.*,b.label_user_coupon_feature_buy_count,
       case when a.label_user_coupon_feature_receive_count=0 then -1 else b.label_user_coupon_feature_buy_count/a.label_user_coupon_feature_receive_count end as label_user_coupon_feature_rate
from wepon_d3_f6_t1 a left outer join wepon_d3_f6_t2 b 
on a.user_id=b.user_id and a.coupon_id=b.coupon_id;


-- ###################   for dataset2  ################### 
create table wepon_d2_f6_t1 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_receive_count from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset2)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature2 )b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

create table wepon_d2_f6_t2 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_buy_count from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset2)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature2 where date_pay!="null" and coupon_id!="null")b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

create table wepon_d2_f6 as 
select a.*,b.label_user_coupon_feature_buy_count,
       case when a.label_user_coupon_feature_receive_count=0 then -1 else b.label_user_coupon_feature_buy_count/a.label_user_coupon_feature_receive_count end as label_user_coupon_feature_rate
from wepon_d2_f6_t1 a left outer join wepon_d2_f6_t2 b 
on a.user_id=b.user_id and a.coupon_id=b.coupon_id;


-- ###################   for dataset1  ################### 
create table wepon_d1_f6_t1 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_receive_count from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset1)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature1 )b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

create table wepon_d1_f6_t2 as
select user_id,coupon_id,sum(cnt) as label_user_coupon_feature_buy_count from
(
  select a.user_id,a.coupon_id, case when b.cnt is null then 0 else 1 end as cnt from
  (select distinct user_id,coupon_id from wepon_dataset1)a  left outer join (select user_id,coupon_id,1 as cnt from wepon_feature1 where date_pay!="null" and coupon_id!="null")b 
  on a.user_id=b.user_id and a.coupon_id=b.coupon_id
)t
group by user_id,coupon_id;

create table wepon_d1_f6 as 
select a.*,b.label_user_coupon_feature_buy_count,
       case when a.label_user_coupon_feature_receive_count=0 then -1 else b.label_user_coupon_feature_buy_count/a.label_user_coupon_feature_receive_count end as label_user_coupon_feature_rate
from wepon_d1_f6_t1 a left outer join wepon_d1_f6_t2 b 
on a.user_id=b.user_id and a.coupon_id=b.coupon_id;



-- ##############################################  合并各种特征文件，生成训练集测试集  ######################################
create table wepon_d3 as
select t.*, case day_of_week when 0 then 1 else 0 end as weekday1,case day_of_week when 1 then 1 else 0 end as weekday2,
            case day_of_week when 2 then 1 else 0 end as weekday3,case day_of_week when 3 then 1 else 0 end as weekday4,
			case day_of_week when 4 then 1 else 0 end as weekday5,case day_of_week when 5 then 1 else 0 end as weekday6,
			case day_of_week when 6 then 1 else 0 end as weekday7 
from
(
    select k.*,l.online_buy_total,l.online_buy_use_coupon ,l.online_buy_use_fixed ,l.online_coupon_received ,l.online_buy_merchant_count ,l.online_action_merchant_count ,
				l.online_buy_use_coupon_fixed ,l.online_buy_use_coupon_rate ,l.online_buy_use_fixed_rate ,l.online_buy_use_coupon_fixed_rate ,l.online_coupon_transform_rate 
	from
	(
		select i.*,j.label_user_coupon_feature_receive_count ,j.label_user_coupon_feature_buy_count ,j.label_user_coupon_feature_rate  from
		(
		  select g.*,h.this_month_user_receive_all_coupon_count,h.this_month_user_receive_same_coupon_count,h.this_day_user_receive_all_coupon_count,
					 h.this_day_user_receive_same_coupon_count,h.this_month_user_receive_same_coupon_lastone,h.this_month_user_receive_same_coupon_firstone,
					 h.label_merchant_user_count,h.label_user_merchant_count,h.label_merchant_coupon_count,h.label_merchant_coupon_type_count,
					 h.label_user_merchant_coupon_count,h.label_same_coupon_count_later,h.label_coupon_count_later
		  from
		  (
			  select e.*,f.user_merchant_buy_total,f.user_merchant_received,f.user_merchant_any,f.user_merchant_buy_use_coupon,f.user_merchant_buy_common,
						 f.user_merchant_coupon_transform_rate,f.user_merchant_coupon_buy_rate,f.user_merchant_common_buy_rate,f.user_merchant_rate from
			  (
				select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance,d.avg_diff_date_datereceived,d.min_diff_date_datereceived,
					   d.max_diff_date_datereceived,d.buy_use_coupon,d.buy_use_coupon_rate,d.user_coupon_transform_rate,d.count_merchant,d.buy_total,d.coupon_received from
				(
				  select a.*,b.distinct_coupon_count, b.merchant_avg_distance, b.merchant_median_distance, b.merchant_max_distance,b.merchant_user_buy_count,
						 b.merchant_min_distance, b.sales_use_coupon, b.transform_rate,b.coupon_rate,b.total_coupon,b.total_sales from
				  wepon_d3_f2 a left outer join wepon_d3_f1 b 
				  on a.merchant_id=b.merchant_id
				)c left outer join wepon_d3_f3 d
				on c.user_id=d.user_id
			  )e left outer join wepon_d3_f4 f
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
		  )g  left outer join wepon_d3_f5 h
		  on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
		)i left outer join wepon_d3_f6 j 
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id
	)k left outer join wepon_d3_f7 l 
	on k.user_id=l.user_id
)t;




create table wepon_d2 as
select t.*, case day_of_week when 0 then 1 else 0 end as weekday1,case day_of_week when 1 then 1 else 0 end as weekday2,
            case day_of_week when 2 then 1 else 0 end as weekday3,case day_of_week when 3 then 1 else 0 end as weekday4,
			case day_of_week when 4 then 1 else 0 end as weekday5,case day_of_week when 5 then 1 else 0 end as weekday6,
			case day_of_week when 6 then 1 else 0 end as weekday7 
from
(
	select k.*,l.online_buy_total,l.online_buy_use_coupon ,l.online_buy_use_fixed ,l.online_coupon_received ,l.online_buy_merchant_count ,l.online_action_merchant_count ,
				l.online_buy_use_coupon_fixed ,l.online_buy_use_coupon_rate ,l.online_buy_use_fixed_rate ,l.online_buy_use_coupon_fixed_rate ,l.online_coupon_transform_rate 
	from
	(
		select i.*,j.label_user_coupon_feature_receive_count ,j.label_user_coupon_feature_buy_count ,j.label_user_coupon_feature_rate  from
		(
		  select g.*,h.this_month_user_receive_all_coupon_count,h.this_month_user_receive_same_coupon_count,h.this_day_user_receive_all_coupon_count,
					 h.this_day_user_receive_same_coupon_count,h.this_month_user_receive_same_coupon_lastone,h.this_month_user_receive_same_coupon_firstone,
					 h.label_merchant_user_count,h.label_user_merchant_count,h.label_merchant_coupon_count,h.label_merchant_coupon_type_count,
					 h.label_user_merchant_coupon_count,h.label_same_coupon_count_later,h.label_coupon_count_later,
					 case when g.date_pay="null" then 0 when abs(datediff(to_date(g.date_pay,"yyyymmdd"),to_date(g.date_received,"yyyymmdd"),"dd"))<=15 then 1  else 0 end as label	 
		  from
		  (
			  select e.*,f.user_merchant_buy_total,f.user_merchant_received,f.user_merchant_any,f.user_merchant_buy_use_coupon,f.user_merchant_buy_common,
						 f.user_merchant_coupon_transform_rate,f.user_merchant_coupon_buy_rate,f.user_merchant_common_buy_rate,f.user_merchant_rate from
			  (
				select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance,d.avg_diff_date_datereceived,d.min_diff_date_datereceived,
					   d.max_diff_date_datereceived,d.buy_use_coupon,d.buy_use_coupon_rate,d.user_coupon_transform_rate,d.count_merchant,d.buy_total,d.coupon_received from
				(
				  select a.*,b.distinct_coupon_count, b.merchant_avg_distance, b.merchant_median_distance, b.merchant_max_distance,b.merchant_user_buy_count,
						 b.merchant_min_distance, b.sales_use_coupon, b.transform_rate,b.coupon_rate,b.total_coupon,b.total_sales from
				  wepon_d2_f2 a left outer join wepon_d2_f1 b 
				  on a.merchant_id=b.merchant_id
				)c left outer join wepon_d2_f3 d
				on c.user_id=d.user_id
			  )e left outer join wepon_d2_f4 f
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
		  )g  left outer join wepon_d2_f5 h
		  on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
		)i left outer join wepon_d2_f6 j 
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id
	)k left outer join wepon_d2_f7 l 
	on k.user_id=l.user_id
)t;



create table wepon_d1 as
select t.*, case day_of_week when 0 then 1 else 0 end as weekday1,case day_of_week when 1 then 1 else 0 end as weekday2,
            case day_of_week when 2 then 1 else 0 end as weekday3,case day_of_week when 3 then 1 else 0 end as weekday4,
			case day_of_week when 4 then 1 else 0 end as weekday5,case day_of_week when 5 then 1 else 0 end as weekday6,
			case day_of_week when 6 then 1 else 0 end as weekday7 
from
(
	select k.*,l.online_buy_total,l.online_buy_use_coupon ,l.online_buy_use_fixed ,l.online_coupon_received ,l.online_buy_merchant_count ,l.online_action_merchant_count ,
				l.online_buy_use_coupon_fixed ,l.online_buy_use_coupon_rate ,l.online_buy_use_fixed_rate ,l.online_buy_use_coupon_fixed_rate ,l.online_coupon_transform_rate 
	from	
	(
		select i.*,j.label_user_coupon_feature_receive_count ,j.label_user_coupon_feature_buy_count ,j.label_user_coupon_feature_rate  from
		(
		  select g.*,h.this_month_user_receive_all_coupon_count,h.this_month_user_receive_same_coupon_count,h.this_day_user_receive_all_coupon_count,
					 h.this_day_user_receive_same_coupon_count,h.this_month_user_receive_same_coupon_lastone,h.this_month_user_receive_same_coupon_firstone,
					 h.label_merchant_user_count,h.label_user_merchant_count,h.label_merchant_coupon_count,h.label_merchant_coupon_type_count,
					 h.label_user_merchant_coupon_count,h.label_same_coupon_count_later,h.label_coupon_count_later,
					 case when g.date_pay="null" then 0 when abs(datediff(to_date(g.date_pay,"yyyymmdd"),to_date(g.date_received,"yyyymmdd"),"dd"))<=15 then 1  else 0 end as label	 
		  from
		  (
			  select e.*,f.user_merchant_buy_total,f.user_merchant_received,f.user_merchant_any,f.user_merchant_buy_use_coupon,f.user_merchant_buy_common,
						 f.user_merchant_coupon_transform_rate,f.user_merchant_coupon_buy_rate,f.user_merchant_common_buy_rate,f.user_merchant_rate from
			  (
				select c.*,d.user_avg_distance,d.user_min_distance,d.user_max_distance,d.user_median_distance,d.avg_diff_date_datereceived,d.min_diff_date_datereceived,
					   d.max_diff_date_datereceived,d.buy_use_coupon,d.buy_use_coupon_rate,d.user_coupon_transform_rate,d.count_merchant,d.buy_total,d.coupon_received from
				(
				  select a.*,b.distinct_coupon_count, b.merchant_avg_distance, b.merchant_median_distance, b.merchant_max_distance,b.merchant_user_buy_count,
						 b.merchant_min_distance, b.sales_use_coupon, b.transform_rate,b.coupon_rate,b.total_coupon,b.total_sales from
				  wepon_d1_f2 a left outer join wepon_d1_f1 b 
				  on a.merchant_id=b.merchant_id
				)c left outer join wepon_d1_f3 d
				on c.user_id=d.user_id
			  )e left outer join wepon_d1_f4 f
			  on e.user_id=f.user_id and e.merchant_id=f.merchant_id
		  )g  left outer join wepon_d1_f5 h
		  on g.user_id=h.user_id and g.coupon_id=h.coupon_id and g.date_received=h.date_received
		)i left outer join wepon_d1_f6 j 
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id
	)k left outer join wepon_d1_f7 l 
	on k.user_id=l.user_id
)t;