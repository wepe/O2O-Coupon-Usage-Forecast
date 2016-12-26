-- 数据集划分:
--               	(date_received)                              
--    	test  dataset: 20160701~20160731 ,features from 20160215~20160615  (测试集，dataset对应label窗，feature对应特征窗)
--    	train dataset: 20160515~20160615 ,features from 20160101~20160501  (训练集，dataset对应label窗，feature对应特征窗)

drop table if exists charles_test_dataset;
drop table if exists charles_test_feature;
drop table if exists charles_test_online_feature;
drop table if exists charles_train_dataset;
drop table if exists charles_train_feature;
drop table if exists charles_train_online_feature;
drop table if exists charles_test_feature_tmp;
drop table if exists charles_test_online_feature_tmp;
drop table if exists charles_train_feature_tmp;
drop table if exists charles_train_online_feature_tmp;

create table if not exists charles_test_dataset as select * from prediction_stage2;
create table if not exists charles_test_feature as select * from train_offline_stage2 where ("20160215"<=date_pay and date_pay<="20160615") or (date_pay="null" and "20160215"<=date_received and date_received<="20160615");
create table if not exists charles_test_online_feature as select * from train_online_stage2 where ("20160215"<=date_pay and date_pay<="20160615") or (date_pay="null" and "20160215"<=date_received and date_received<="20160615");

create table if not exists charles_train_dataset as select * from train_offline_stage2 where "20160515"<=date_received and date_received<="20160615";
create table if not exists charles_train_feature as select * from train_offline_stage2 where ("20160101"<=date_pay and date_pay<="20160501") or (date_pay="null" and "20160101"<=date_received and date_received<="20160501");
create table if not exists charles_train_online_feature as select * from train_online_stage2 where ("20160101"<=date_pay and date_pay<="20160501") or (date_pay="null" and "20160101"<=date_received and date_received<="20160501");

-- 提取特征
-- 1. user features
--		user_consume_counts 					用户所有消费次数(正常消费+优惠券消费)
--		user_common_consume_counts				用户正常消费次数(不使用优惠券消费)
--		user_common_consume_rate				用户正常消费占所有消费的比重
--		user_received_counts					用户领取优惠券次数
-- 		user_none_consume_counts				用户获得优惠券但没有消费的次数
-- 		user_coupon_consume_counts 				用户获得优惠券并核销次数
-- 		user_coupon_consume_rate				用户领取优惠券后进行核销率
--		user_coupon_discount_floor_30_rate		用户满0~30减的优惠券核销率
--		user_coupon_discount_floor_50_rate		用户满30~50减的优惠券核销率
--		user_coupon_discount_floor_200_rate		用户满50~200减的优惠券核销率
--		user_coupon_discount_floor_others_rate	用户其他满减的优惠券核销率
--		user_consume_discount_floor_30_rate		用户核销满0~30减的优惠券占所有核销优惠券的比重
--		user_consume_discount_floor_50_rate		用户核销30~50减的优惠券占所有核销优惠券的比重
--		user_consume_discount_floor_200_rate	用户核销50~200减的优惠券占所有核销优惠券的比重
--		user_consume_discount_floor_others_rate	用户核销其他满减的优惠券占所有核销优惠券的比重
--		user_average_discount_rate				用户核销优惠券的平均消费折率
--		user_minimum_discount_rate				用户核销优惠券的最低消费折率
--		user_maximum_discount_rate				用户核销优惠券的最高消费折率
--		user_consume_merchants					用户核销过优惠券的不同商家数量
--		user_consume_merchants_rate				用户核销过优惠券的不同商家数量占所有不同商家的比重
--		user_merchant_average_consume_counts	用户平均核销每个商家多少张优惠券
--		user_consume_coupons					用户核销过的不同优惠券数量
--		user_consume_coupons_rate				用户核销过的不同优惠券数量占所有不同优惠券的比重
--  	user_average_coupon_consume_counts		用户平均每种优惠券核销多少张
--		user_average_consume_time_rate			用户核销优惠券的平均时间率
--		user_consume_average_distance			用户核销优惠券中的平均用户-商家距离
--		user_consume_minimum_distance			用户核销优惠券中的最小用户-商家距离
--		user_consume_maximum_distance			用户核销优惠券中的最大用户-商家距离
--	user online features
--		user_online_action_counts				用户线上操作次数
--		user_online_action_0_rate				用户线上点击率	
--		user_online_action_1_rate				用户线上购买率
--		user_online_action_2_rate				用户线上领取率
--		user_online_none_consume_counts			用户线上不消费次数
--		user_online_coupon_consume_counts		用户线上优惠券核销次数
--		user_online_coupon_consume_rate			用户线上优惠券核销率
--	user online-offline features
--		user_offline_none_consume_rate			用户线下不消费次数占线上线下总的不消费次数的比重
--		user_offline_coupon_consume_rate		用户线下的优惠券核销次数占线上线下总的优惠券核销次数的比重
--		user_offline_rate 						用户线下领取的记录数量占总的记录数量的比重

drop table if exists charles_train_feature_tmp;
create table if not exists charles_train_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,coupon_type,coupon_discount_floor,
	case when distance!="null" then distance/10.0 else -2.0 end as distance_rate,
	case when date_pay="null" then -2.0 when date_consumed>15.0 then -1.0 else 1.0-date_consumed/15.0 end as date_consumed_rate,
	case when date_pay="null" then 0.0 when date_consumed>15.0 then 0.0 else 1.0 end as consume_counts,
	case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount from
	(
		select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,
			case when date_pay!="null" then datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd") else -2.0 end as date_consumed,
			case when instr(discount_rate,":")=0 then 0 else 1 end as coupon_type,
			case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
			case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
		from charles_train_feature
		where date_received!="null"
	)t;

drop table if exists charles_train_online_feature_tmp;
create table if not exists charles_train_online_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_train_online_feature;

drop table if exists charles_train_f1_t1;
create table charles_train_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(30_floor_counts) as 30_floor_total_counts,sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<30 then 1.0 else 0.0 end as 30_floor_counts,
			case when coupon_discount_floor>=30 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 then 1.0 else 0.0 end as other_floor_counts
		from charles_train_feature_tmp
	)t
	group by user_id;

drop table if exists charles_train_f1_t2;
create table charles_train_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,max(coupon_discount) as user_maximum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(30_consumed_floor_counts) as 30_consumed_floor_total_counts,sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<30 then 1.0 else 0.0 end as 30_consumed_floor_counts,
			case when coupon_discount_floor>=30 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_train_feature_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_train_f1_t3;
create table charles_train_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance,min(distance_rate) as user_consume_minimum_distance from
	(
		select user_id,distance_rate from charles_train_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_train_f1_t4;
create table charles_train_f1_t4 as
	select user_id,sum(cnt) as user_common_consume_counts from
	(select user_id,case when date_pay!="null" and coupon_id="null" then 1 else 0 end as cnt from charles_train_feature)t
	group by user_id;

drop table if exists charles_train_f1_off;
create table charles_train_f1_off as
	select e.*,f.user_common_consume_counts,e.user_coupon_consume_counts+f.user_common_consume_counts as user_consume_counts,
	case when e.user_coupon_consume_counts+f.user_common_consume_counts=0 then -2 else f.user_common_consume_counts/(e.user_coupon_consume_counts+f.user_common_consume_counts) end as user_common_consume_rate from
	(
		select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance,d.user_consume_minimum_distance from
		(
			select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
			b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
			b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
				case when a.30_floor_total_counts=0.0 then -2.0 else b.30_consumed_floor_total_counts/a.30_floor_total_counts end as user_coupon_discount_floor_30_rate,
				case when a.50_floor_total_counts=0.0 then -2.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
				case when a.200_floor_total_counts=0.0 then -2.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
				case when a.other_floor_total_counts=0.0 then -2.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.30_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_30_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
			charles_train_f1_t1 a left outer join charles_train_f1_t2 b
			on a.user_id=b.user_id
		)c left outer join charles_train_f1_t3 d
		on c.user_id=d.user_id
	)e left outer join charles_train_f1_t4 f
	on e.user_id=f.user_id;

drop table if exists charles_train_f1_on;
create table charles_train_f1_on as
	select user_id,sum(cnt) as user_online_action_counts,sum(receive_counts) as user_online_receive_counts,sum(click_counts)/sum(cnt) as user_online_action_0_rate,sum(buy_counts)/sum(cnt) as user_online_action_1_rate,sum(receive_counts)/sum(cnt) as user_online_action_2_rate,sum(none_consume_counts) as user_online_none_consume_counts,sum(consume_counts) as user_online_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_online_coupon_consume_rate from
	(
		select user_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when action="0" then 1.0 else 0.0 end as click_counts,
			case when action="1" then 1.0 else 0.0 end as buy_counts,
			case when action="2" then 1.0 else 0.0 end as receive_counts
		from charles_train_online_feature_tmp
	)t
	group by user_id;

drop table if exists charles_train_f1;
create table charles_train_f1 as
	select a.*,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -2.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -2.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -2.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_train_f1_off a left outer join charles_train_f1_on b
	on a.user_id=b.user_id;


drop table if exists charles_test_feature_tmp;
create table if not exists charles_test_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,coupon_type,coupon_discount_floor,
	case when distance!="null" then distance/10.0 else -2.0 end as distance_rate,
	case when date_pay="null" then -2.0 when date_consumed>15.0 then -1.0 else 1.0-date_consumed/15.0 end as date_consumed_rate,
	case when date_pay="null" then 0.0 when date_consumed>15.0 then 0.0 else 1.0 end as consume_counts,
	case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount from
	(
		select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,
			case when date_pay!="null" then datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd") else -2.0 end as date_consumed,
			case when instr(discount_rate,":")=0 then 0 else 1 end as coupon_type,
			case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
			case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
		from charles_test_feature
		where date_received!="null"
	)t;

drop table if exists charles_test_online_feature_tmp;
create table if not exists charles_test_online_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_test_online_feature;

drop table if exists charles_test_f1_t1;
create table charles_test_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(30_floor_counts) as 30_floor_total_counts,sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<30 then 1.0 else 0.0 end as 30_floor_counts,
			case when coupon_discount_floor>=30 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 then 1.0 else 0.0 end as other_floor_counts
		from charles_test_feature_tmp
	)t
	group by user_id;

drop table if exists charles_test_f1_t2;
create table charles_test_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,max(coupon_discount) as user_maximum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(30_consumed_floor_counts) as 30_consumed_floor_total_counts,sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<30 then 1.0 else 0.0 end as 30_consumed_floor_counts,
			case when coupon_discount_floor>=30 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_test_feature_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_test_f1_t3;
create table charles_test_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance,min(distance_rate) as user_consume_minimum_distance from
	(
		select user_id,distance_rate from charles_test_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_test_f1_t4;
create table charles_test_f1_t4 as
	select user_id,sum(cnt) as user_common_consume_counts from
	(select user_id,case when date_pay!="null" and coupon_id="null" then 1 else 0 end as cnt from charles_test_feature)t
	group by user_id;

drop table if exists charles_test_f1_off;
create table charles_test_f1_off as
	select e.*,f.user_common_consume_counts,e.user_coupon_consume_counts+f.user_common_consume_counts as user_consume_counts,case when e.user_coupon_consume_counts+f.user_common_consume_counts=0 then -2 else f.user_common_consume_counts/(e.user_coupon_consume_counts+f.user_common_consume_counts) end as user_common_consume_rate from
	(
		select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance,d.user_consume_minimum_distance from
		(
			select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
			b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
			b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
				case when a.30_floor_total_counts=0.0 then -2.0 else b.30_consumed_floor_total_counts/a.30_floor_total_counts end as user_coupon_discount_floor_30_rate,
				case when a.50_floor_total_counts=0.0 then -2.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
				case when a.200_floor_total_counts=0.0 then -2.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
				case when a.other_floor_total_counts=0.0 then -2.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.30_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_30_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
				case when a.user_coupon_consume_counts=0.0 then -2.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
			charles_test_f1_t1 a left outer join charles_test_f1_t2 b
			on a.user_id=b.user_id
		)c left outer join charles_test_f1_t3 d
		on c.user_id=d.user_id
	)e left outer join charles_test_f1_t4 f
	on e.user_id=f.user_id;

drop table if exists charles_test_f1_on;
create table charles_test_f1_on as
	select user_id,sum(cnt) as user_online_action_counts,sum(receive_counts) as user_online_receive_counts,sum(click_counts)/sum(cnt) as user_online_action_0_rate,sum(buy_counts)/sum(cnt) as user_online_action_1_rate,sum(receive_counts)/sum(cnt) as user_online_action_2_rate,sum(none_consume_counts) as user_online_none_consume_counts,sum(consume_counts) as user_online_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_online_coupon_consume_rate from
	(
		select user_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when action="0" then 1.0 else 0.0 end as click_counts,
			case when action="1" then 1.0 else 0.0 end as buy_counts,
			case when action="2" then 1.0 else 0.0 end as receive_counts
		from charles_test_online_feature_tmp
	)t
	group by user_id;

drop table if exists charles_test_f1;
create table charles_test_f1 as
	select a.*,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -2.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -2.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -2.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_test_f1_off a left outer join charles_test_f1_on b
	on a.user_id=b.user_id;

-- 2. merchant features
--		merchant_consume_counts 				商家所有消费次数
--  	merchant_common_consume_counts			商家正常消费次数
--		merchant_common_consume_rate			商家正常消费率
--		merchant_received_counts				商家优惠券被领取次数
--		merchant_none_consume_counts			商家优惠券被领取后不核销次数
--		merchant_coupon_consume_counts			商家优惠券被领取后核销次数
--		merchant_coupon_consume_rate			商家优惠券被领取后核销率
--		merchant_average_discount_rate			商家优惠券核销的平均消费折率
--		merchant_minimum_discount_rate			商家优惠券核销的最低消费折率
--		merchant_maximum_discount_rate			商家优惠券核销的最高消费折率
--		merchant_consume_users					核销商家优惠券的不同用户数量
--		merchant_consume_users_rate				核销商家优惠券的不同用户数量占领取不同的用户比重
-- 		merchant_user_average_consume_counts	商家优惠券平均每个用户核销多少张
--		merchant_consume_coupons				商家被核销过的不同优惠券数量
--   	merchant_consume_coupons_rate			商家被核销过的不同优惠券数量占所有领取过的不同优惠券数量的比重
--		merchant_average_coupon_consume_counts	商家平均每种优惠券核销多少张
--		merchant_average_consume_time_rate		商家被核销优惠券的平均时间率
--		merchant_consume_average_distance		商家被核销优惠券中的平均用户-商家距离
--		merchant_consume_maximum_distance		商家被核销优惠券中的最大用户-商家距离
--		merchant_consume_minimum_distance		商家被核销优惠券中的最小用户-商家距离

drop table if exists charles_train_f2_t1;
create table charles_train_f2_t1 as
	select merchant_id,sum(cnt) as merchant_received_counts,sum(none_consume_counts) as merchant_none_consume_counts,sum(consume_counts) as merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as merchant_coupon_consume_rate,count(distinct user_id) as merchant_total_users,count(distinct coupon_id) as merchant_total_coupons from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts
		from charles_train_feature_tmp
	)t
	group by merchant_id;

drop table if exists charles_train_f2_t2;
create table charles_train_f2_t2 as
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,max(coupon_discount) as merchant_maximum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_train_feature_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_train_f2_t3;
create table charles_train_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance,min(distance_rate) as merchant_consume_minimum_distance from
	(
		select merchant_id,distance_rate from charles_train_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_train_f2_t4;
create table charles_train_f2_t4 as 
	select merchant_id,sum(cnt) as merchant_common_consume_counts from
	(select merchant_id,case when date_pay!="null" and coupon_id="null" then 1 else 0 end as cnt from charles_train_feature)t
	group by merchant_id;

drop table if exists charles_train_f2;
create table charles_train_f2 as
	select e.*,f.merchant_common_consume_counts,e.merchant_coupon_consume_counts+f.merchant_common_consume_counts as merchant_consume_counts,
	case when e.merchant_coupon_consume_counts+f.merchant_common_consume_counts=0 then -2 else f.merchant_common_consume_counts/(e.merchant_coupon_consume_counts+f.merchant_common_consume_counts) end as merchant_common_consume_rate from
	(
		select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
		(
			select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
			charles_train_f2_t2 a right outer join charles_train_f2_t1 b
			on a.merchant_id=b.merchant_id
		)c left outer join charles_train_f2_t3 d
		on c.merchant_id=d.merchant_id
	)e left outer join charles_train_f2_t4 f 
	on e.merchant_id=f.merchant_id;


drop table if exists charles_test_f2_t1;
create table charles_test_f2_t1 as
	select merchant_id,sum(cnt) as merchant_received_counts,sum(none_consume_counts) as merchant_none_consume_counts,sum(consume_counts) as merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as merchant_coupon_consume_rate,count(distinct user_id) as merchant_total_users,count(distinct coupon_id) as merchant_total_coupons from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts
		from charles_test_feature_tmp
	)t
	group by merchant_id;

drop table if exists charles_test_f2_t2;
create table charles_test_f2_t2 as
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,max(coupon_discount) as merchant_maximum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_test_feature_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_test_f2_t3;
create table charles_test_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance,min(distance_rate) as merchant_consume_minimum_distance from
	(
		select merchant_id,distance_rate from charles_test_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_test_f2_t4;
create table charles_test_f2_t4 as 
	select merchant_id,sum(cnt) as merchant_common_consume_counts from
	(select merchant_id,case when date_pay!="null" and coupon_id="null" then 1 else 0 end as cnt from charles_test_feature)t
	group by merchant_id;

drop table if exists charles_test_f2;
create table charles_test_f2 as
	select e.*,f.merchant_common_consume_counts,e.merchant_coupon_consume_counts+f.merchant_common_consume_counts as merchant_consume_counts,
	case when e.merchant_coupon_consume_counts+f.merchant_common_consume_counts=0 then -2 else f.merchant_common_consume_counts/(e.merchant_coupon_consume_counts+f.merchant_common_consume_counts) end as merchant_common_consume_rate from
	(
		select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
		(
			select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
			charles_test_f2_t2 a right outer join charles_test_f2_t1 b
			on a.merchant_id=b.merchant_id
		)c left outer join charles_test_f2_t3 d
		on c.merchant_id=d.merchant_id
	)e left outer join charles_test_f2_t4 f 
	on e.merchant_id=f.merchant_id;

-- 3. user-merchant features
-- 		user_merchant_consume_counts 			用户消费商家次数
--		user_merchant_common_counts 			用户正常消费商家次数
-- 		user_merchant_common_rate 				用户商家正常消费率
--		user_merchant_coupon_counts				用户领取商家的优惠券次数
--		user_merchant_none_consume_counts		用户领取商家的优惠券后不核销次数
--		user_merchant_coupon_consume_counts		用户领取商家的优惠券后核销次数
--		user_merchant_coupon_consume_rate		用户领取商家的优惠券后核销率
--		user_none_consume_merchant_rate			用户对每个商家的不核销次数占用户总的不核销次数的比重
--		user_coupon_consume_merchant_rate		用户对每个商家的优惠券核销次数占用户总的核销次数的比重
--		user_none_consume_merchant_rate			用户对每个商家的不核销次数占商家总的不核销次数的比重
--		user_coupon_consume_merchant_rate		用户对每个商家的优惠券核销次数占商家总的核销次数的比重

drop table if exists charles_train_f3_t1;
create table charles_train_f3_t1 as
	select user_id,merchant_id,sum(cnt) as user_merchant_coupon_counts,sum(none_consume_counts) as user_merchant_none_consume_counts,sum(consume_counts) as user_merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_merchant_coupon_consume_rate from
	(
		select user_id,merchant_id,consume_counts,1 as cnt,1-consume_counts as none_consume_counts from charles_train_feature_tmp
	)t
	group by user_id,merchant_id;

drop table if exists charles_train_f3_t2;
create table charles_train_f3_t2 as 
	select user_id,merchant_id,sum(cnt) as user_merchant_common_counts from
	(select user_id,merchant_id,case when date_pay!="null" and coupon_id="null" then 1 else 0 end as cnt from charles_train_feature)t
	group by user_id,merchant_id;

drop table if exists charles_train_f3;
create table charles_train_f3 as
	select e.*,f.user_merchant_common_counts,e.user_merchant_coupon_counts+f.user_merchant_common_counts as user_merchant_consume_counts,
	case when e.user_merchant_coupon_counts+f.user_merchant_common_counts=0 then -2 else f.user_merchant_common_counts/(e.user_merchant_coupon_counts+f.user_merchant_common_counts) end as user_merchant_common_rate from
	(
		select c.*,case when d.merchant_consume_counts=0 then -2.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
		(
			select a.*,case when b.user_consume_counts=0 then -2.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
			charles_train_f3_t1 a left outer join
			(
				select user_id,sum(consume_counts) as user_consume_counts from charles_train_feature_tmp group by user_id
			)b
			on a.user_id=b.user_id
		)c left outer join
		(
			select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_train_feature_tmp group by merchant_id
		)d on c.merchant_id=d.merchant_id
	)e left outer join charles_train_f3_t2 f 
	on e.user_id=f.user_id and e.merchant_id=f.merchant_id;


drop table if exists charles_test_f3_t1;
create table charles_test_f3_t1 as
	select user_id,merchant_id,sum(cnt) as user_merchant_coupon_counts,sum(none_consume_counts) as user_merchant_none_consume_counts,sum(consume_counts) as user_merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_merchant_coupon_consume_rate from
	(
		select user_id,merchant_id,consume_counts,1 as cnt,1-consume_counts as none_consume_counts from charles_test_feature_tmp
	)t
	group by user_id,merchant_id;

drop table if exists charles_test_f3_t2;
create table charles_test_f3_t2 as 
	select user_id,merchant_id,sum(cnt) as user_merchant_common_counts from
	(select user_id,merchant_id,case when date_pay!="null" and coupon_id="null" then 1 else 0 end as cnt from charles_test_feature)t
	group by user_id,merchant_id;

drop table if exists charles_test_f3;
create table charles_test_f3 as
	select e.*,f.user_merchant_common_counts,e.user_merchant_coupon_counts+f.user_merchant_common_counts as user_merchant_consume_counts,case when e.user_merchant_coupon_counts+f.user_merchant_common_counts=0 then -2 else f.user_merchant_common_counts/(e.user_merchant_coupon_counts+f.user_merchant_common_counts) end as user_merchant_common_rate from
	(
		select c.*,case when d.merchant_consume_counts=0 then -2.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
		(
			select a.*,case when b.user_consume_counts=0 then -2.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
			charles_test_f3_t1 a left outer join
			(
				select user_id,sum(consume_counts) as user_consume_counts from charles_test_feature_tmp group by user_id
			)b
			on a.user_id=b.user_id
		)c left outer join
		(
			select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_test_feature_tmp group by merchant_id
		)d on c.merchant_id=d.merchant_id
	)e left outer join charles_test_f3_t2 f 
	on e.user_id=f.user_id and e.merchant_id=f.merchant_id;

-- 4. coupon features
--		coupon_type								优惠券类型(直接优惠为0, 满减为1)
--		coupon_discount 						优惠券折率
--		coupon_discount_floor 					满减优惠券的最低消费
--		coupon_history_counts 					历史出现次数
--      coupon_history_consume_counts			历史核销次数
--		coupon_history_consume_rate         	历史核销率
--		coupon_history_consume_time_rate		历史核销时间率
--		day_of_week								领取优惠券是一周的第几天
-- 		day_of_month							领取优惠券是一月的第几天
--	user_coupon_history_features
--		user_coupon_history_received_counts		历史上用户领取该优惠券次数
--		user_coupon_history_consume_counts		历史上用户消费该优惠券次数
--		user_coupon_history_consume_rate		历史上用户对该优惠券的核销率

drop table if exists charles_train_f4_t1;
create table charles_train_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts,sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_train_feature
		where date_received!="null"
	)t
	group by coupon_id;

drop table if exists charles_train_f4_t2;
create table charles_train_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_train_feature
		where date_received!="null"
	)t
	group by user_id,coupon_id;


drop table if exists charles_test_f4_t1;
create table charles_test_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts,sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_test_feature
		where date_received!="null"
	)t
	group by coupon_id;

drop table if exists charles_test_f4_t2;
create table charles_test_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_test_feature
		where date_received!="null"
	)t
	group by user_id,coupon_id;
		
-- 5. other features
--		user_received_counts					用户领取的所有优惠券数目
--		user_received_coupon_counts				用户领取的特定优惠券数目
--		user_later_received_coupons 			用户此次之后领取的所有优惠券数目
--		user_later_received_coupon_counts		用户此次之后领取的特定优惠券数目
--		merchant_received_counts				商家被领取的优惠券数目
--		merchant_received_coupon_counts			商家被领取的特定优惠券数目
--  	user_merchant_received_counts			用户领取特定商家的优惠券数目
--		user_merchants 							用户领取的不同商家数目
--		merchant_users 							商家被多少不同用户领取的数目
--  	this_day_user_received_counts			用户当天领取的优惠券数目
--  	this_day_user_received_coupon_counts	用户当天领取的特定优惠券数目
--		user_coupons							用户领取的所有优惠券种类数目
--		merchant_coupons						商家发行的所有优惠券种类数目
--		user_last_received_coupons 				用户此次之前领取的所有优惠券数目
--		user_last_received_coupon_counts		用户此次之前领取的特定优惠券数目
--		user_last_coupon_type					用户领取的上一张优惠券类型
--		user_last_date_diff						用户上一次领取的时间距离
--		user_last_same_coupon					用户领取的上一张优惠券是否与当前同种
--		user_last_high_discount					用户领取的上一张优惠券折扣是否高于现在
--		user_last_high_consume_rate				用户领取的上一张优惠券的历史核销率是否高于现在
--		user_later_coupon_type					用户领取的下一张优惠券类型
--		user_later_date_diff					用户下一次领取的时间距离
--		user_later_same_coupon					用户领取的下一张优惠券是否与当前同种
--		user_later_high_discount				用户领取的下一张优惠券折扣是否高于现在
--		user_later_high_consume_rate			用户领取的下一张优惠券的历史核销率是否高于现在
--		user_coupon_low_min_discount			当前优惠券折率是否低于用户消费过的历史最低折率
--		user_coupon_far_max_distance			当前优惠券距离是否高于用户消费过的历史最高距离
--		user_coupon_low_avg_discount			当前优惠券折率是否低于用户消费过的历史最低折率
--		user_coupon_far_avg_distance			当前优惠券距离是否高于用户消费过的历史最高距离

drop table if exists charles_train_f5_t1;
create table charles_train_f5_t1 as
	select user_id,sum(cnt) as user_received_counts from
	(
		select user_id,1 as cnt from charles_train_dataset
	)t
	group by user_id;

drop table if exists charles_train_f5_t2;
create table charles_train_f5_t2 as
	select user_id,coupon_id,sum(cnt) as user_received_coupon_counts from
	(
		select user_id,coupon_id,1 as cnt from charles_train_dataset
	)t
	group by user_id,coupon_id;

drop table if exists charles_train_f5_t3;
create table charles_train_f5_t3 as
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts,ucnt-1 as user_later_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt,row_number() over(partition by user_id order by date_received desc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_train_dataset)p
	)t; 

drop table if exists charles_train_f5_t4;
create table charles_train_f5_t4 as
	select merchant_id,sum(cnt) as merchant_received_counts from
	(
		select merchant_id,1 as cnt from charles_train_dataset
	)t
	group by merchant_id;

drop table if exists charles_train_f5_t5;
create table charles_train_f5_t5 as
	select merchant_id,coupon_id,sum(cnt) as merchant_received_coupon_counts from
	(
		select merchant_id,coupon_id,1 as cnt from charles_train_dataset
	)t
	group by merchant_id,coupon_id;

drop table if exists charles_train_f5_t6;
create table charles_train_f5_t6 as
	select user_id,merchant_id,sum(cnt) as user_merchant_received_counts from
	(
		select user_id,merchant_id,1 as cnt from charles_train_dataset
	)t
	group by user_id,merchant_id;

drop table if exists charles_train_f5_t7;
create table charles_train_f5_t7 as
	select user_id,count(distinct merchant_id) as user_merchants from
	(
		select user_id,merchant_id from charles_train_dataset
	)t
	group by user_id;

drop table if exists charles_train_f5_t8;
create table charles_train_f5_t8 as
	select merchant_id,count(distinct user_id) as merchant_users from
	(
		select user_id,merchant_id from charles_train_dataset
	)t
	group by merchant_id;

drop table if exists charles_train_f5_t9;
create table charles_train_f5_t9 as
	select user_id,date_received,sum(cnt) as this_day_user_received_counts from
	(
		select user_id,date_received,1 as cnt from charles_train_dataset
	)t
	group by user_id,date_received;

drop table if exists charles_train_f5_t10;
create table charles_train_f5_t10 as
	select user_id,date_received,coupon_id,sum(cnt) as this_day_user_received_coupon_counts from
	(
		select user_id,coupon_id,date_received,1 as cnt from charles_train_dataset
	)t
	group by user_id,date_received,coupon_id;

drop table if exists charles_train_f5_t11;
create table charles_train_f5_t11 as
	select user_id,count(*) as user_coupons from
	(select distinct user_id,merchant_id from charles_train_dataset)t
	group by user_id;
	
drop table if exists charles_train_f5_t12;
create table charles_train_f5_t12 as
	select merchant_id,count(*) as merchant_coupons from
	(select distinct user_id,merchant_id from charles_train_dataset)t
	group by merchant_id;

--new-add features
drop table if exists charles_train_f5_t13;
create table charles_train_f5_t13 as
	select user_id,coupon_id,date_received,cnt-1 as user_last_received_coupon_counts,ucnt-1 as user_last_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received asc) as cnt,row_number() over(partition by user_id order by date_received asc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_train_dataset)p
	)t; 

drop table if exists charles_train_f5_tmp;
create table charles_train_f5_tmp as
	select a.*,b.user_coupon_history_consume_rate as  user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,date_received,discount_rate,row_number() over(partition by user_id order by date_received asc) as received_tag,
		case when instr(discount_rate,":")=0 then 1.0-discount_rate else cast(split_part(discount_rate,":",2) as double)/cast(split_part(discount_rate,":",1) as double) end as coupon_discount,
		case when distance="null" then -2.0 else distance/10 end as distance_rate from
		(select distinct user_id,coupon_id,date_received,discount_rate,distance from charles_train_dataset)p
	)a left outer join charles_train_f4_t2 b
	on a.user_id=b.user_id and a.coupon_id=b.coupon_id;

drop table if exists charles_train_f5_t14;
create table charles_train_f5_t14 as
	select a.user_id,a.coupon_id,a.date_received,
	case when b.discount_rate is null then -2.0 when instr(b.discount_rate,":")=0 then 0.0 else 1.0 end as user_last_coupon_type,
	case when b.date_received is null then -2.0 else datediff(to_date(a.date_received,"yyyymmdd"),to_date(b.date_received,"yyyymmdd"),"dd") end as user_last_date_diff,
	case when b.coupon_id is null then -2 when a.coupon_id=b.coupon_id then 1 else 0 end as user_last_same_coupon,
	case when b.coupon_id is null then -2 when a.user_coupon_history_consume_rate<b.user_coupon_history_consume_rate then 1 else 0 end as user_last_high_consume_rate,
	case when b.coupon_id is null then -2 when a.coupon_discount<b.coupon_discount then 1 else 0 end as user_last_high_discount from
	(select * from charles_train_f5_tmp)a
	left outer join
	(select user_id,coupon_id,date_received,discount_rate,coupon_discount,user_coupon_history_consume_rate,received_tag+1 as received_tag from charles_train_f5_tmp)b
	on a.user_id=b.user_id and a.received_tag=b.received_tag;

drop table if exists charles_train_f5_t15;
create table charles_train_f5_t15 as
	select a.user_id,a.coupon_id,a.date_received,
	case when b.discount_rate is null then -2.0 when instr(b.discount_rate,":")=0 then 0.0 else 1.0 end as user_later_coupon_type,
	case when b.date_received is null then -2.0 else datediff(to_date(a.date_received,"yyyymmdd"),to_date(b.date_received,"yyyymmdd"),"dd") end as user_later_date_diff,
	case when b.coupon_id is null then -2 when a.coupon_id=b.coupon_id then 1 else 0 end as user_later_same_coupon,
	case when b.coupon_id is null then -2 when a.user_coupon_history_consume_rate<b.user_coupon_history_consume_rate then 1 else 0 end as user_later_high_consume_rate,
	case when b.coupon_id is null then -2 when a.coupon_discount<b.coupon_discount then 1 else 0 end as user_later_high_discount from
	(select * from charles_train_f5_tmp)a
	left outer join
	(select user_id,coupon_id,date_received,discount_rate,coupon_discount,user_coupon_history_consume_rate,received_tag-1 as received_tag from charles_train_f5_tmp)b
	on a.user_id=b.user_id and a.received_tag=b.received_tag;

drop table if exists charles_train_f5_t16;
create table charles_train_f5_t16 as
	select e.*,f.user_later_coupon_type,f.user_later_date_diff,f.user_later_same_coupon,f.user_later_high_consume_rate,f.user_later_high_discount from
	(
		select c.*,d.user_last_coupon_type,d.user_last_date_diff,d.user_last_same_coupon,d.user_last_high_consume_rate,d.user_last_high_discount from
		(
			select a.*,b.user_last_received_coupon_counts,b.user_last_received_coupons from
			(select distinct user_id,coupon_id,date_received from charles_train_dataset) 
			a left outer join charles_train_f5_t13 b
			on a.user_id=b.user_id and a.coupon_id=b.coupon_id and a.date_received=b.date_received
		)c left outer join charles_train_f5_t14 d
		on c.user_id=d.user_id and c.coupon_id=d.coupon_id and c.date_received=d.date_received
	)e left outer join charles_train_f5_t15 f
	on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received;

drop table if exists charles_train_f5_t17;
create table charles_train_f5_t17 as
	select a.user_id,a.coupon_id,
	case when b.user_average_discount_rate is null then -2 when a.coupon_discount<b.user_average_discount_rate then 1 else 0 end as user_coupon_low_avg_discount,
	case when b.user_minimum_discount_rate is null then -2 when a.coupon_discount<b.user_minimum_discount_rate then 1 else 0 end as user_coupon_low_min_discount,
	case when a.distance_rate=-2 then -2 when b.user_consume_average_distance is null then -2 when a.distance_rate>b.user_consume_average_distance then 1 else 0 end as user_coupon_far_avg_distance,
	case when a.distance_rate=-2 then -2 when b.user_consume_maximum_distance is null then -2 when a.distance_rate>b.user_consume_maximum_distance then 1 else 0 end as user_coupon_far_max_distance from
	(select distinct user_id,coupon_id,coupon_discount,distance_rate from charles_train_f5_tmp)a
	left outer join
	(select user_id,user_average_discount_rate,user_minimum_discount_rate,user_consume_average_distance,user_consume_maximum_distance from charles_train_f1)b
	on a.user_id=b.user_id;

-- 合并特征
drop table if exists charles_train_f5;
create table charles_train_f5 as
	select w.*,x.merchant_coupons from
	(
		select u.*,v.user_coupons from
		(
			select s.*,t.this_day_user_received_coupon_counts from
			(
				select q.*,r.this_day_user_received_counts from
				(
					select o.*, p.merchant_users from
					(
						select m.*, n.user_merchants from
						(
							select k.*, l.user_merchant_received_counts from
							(
								select i.*,j.merchant_received_coupon_counts from
								(
									select g.*,h.merchant_received_counts from
									(
										select e.*,f.user_later_received_coupon_counts,f.user_later_received_coupons from
										(
											select c.*,d.user_received_coupon_counts from
											(
												select a.*,b.user_received_counts from
												(select distinct user_id,merchant_id,coupon_id,date_received from charles_train_dataset) 
												a left outer join charles_train_f5_t1 b
												on a.user_id=b.user_id
											)c left outer join charles_train_f5_t2 d
											on c.user_id=d.user_id and c.coupon_id=d.coupon_id
										)e left outer join charles_train_f5_t3 f
										on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
									)g left outer join charles_train_f5_t4 h
									on g.merchant_id=h.merchant_id
								)i left outer join charles_train_f5_t5 j
								on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
							)k left outer join charles_train_f5_t6 l
							on k.user_id=l.user_id and k.merchant_id=l.merchant_id
						)m left outer join charles_train_f5_t7 n
						on m.user_id=n.user_id
					)o left outer join charles_train_f5_t8 p
					on o.merchant_id=p.merchant_id
				)q left outer join charles_train_f5_t9 r
				on q.user_id=r.user_id and q.date_received=r.date_received
			)s left outer join charles_train_f5_t10 t
			on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
		)u left outer join charles_train_f5_t11 v
		on u.user_id=v.user_id
	)w left outer join charles_train_f5_t12 x
	on w.merchant_id=x.merchant_id;


drop table if exists charles_test_f5_t1;
create table charles_test_f5_t1 as
	select user_id,sum(cnt) as user_received_counts from
	(
		select user_id,1 as cnt from charles_test_dataset
	)t
	group by user_id;

drop table if exists charles_test_f5_t2;
create table charles_test_f5_t2 as
	select user_id,coupon_id,sum(cnt) as user_received_coupon_counts from
	(
		select user_id,coupon_id,1 as cnt from charles_test_dataset
	)t
	group by user_id,coupon_id;

drop table if exists charles_test_f5_t3;
create table charles_test_f5_t3 as
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts,ucnt-1 as user_later_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt,row_number() over(partition by user_id order by date_received desc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_test_dataset)p
	)t; 

drop table if exists charles_test_f5_t4;
create table charles_test_f5_t4 as
	select merchant_id,sum(cnt) as merchant_received_counts from
	(
		select merchant_id,1 as cnt from charles_test_dataset
	)t
	group by merchant_id;

drop table if exists charles_test_f5_t5;
create table charles_test_f5_t5 as
	select merchant_id,coupon_id,sum(cnt) as merchant_received_coupon_counts from
	(
		select merchant_id,coupon_id,1 as cnt from charles_test_dataset
	)t
	group by merchant_id,coupon_id;

drop table if exists charles_test_f5_t6;
create table charles_test_f5_t6 as
	select user_id,merchant_id,sum(cnt) as user_merchant_received_counts from
	(
		select user_id,merchant_id,1 as cnt from charles_test_dataset
	)t
	group by user_id,merchant_id;

drop table if exists charles_test_f5_t7;
create table charles_test_f5_t7 as
	select user_id,count(distinct merchant_id) as user_merchants from
	(
		select user_id,merchant_id from charles_test_dataset
	)t
	group by user_id;

drop table if exists charles_test_f5_t8;
create table charles_test_f5_t8 as
	select merchant_id,count(distinct user_id) as merchant_users from
	(
		select user_id,merchant_id from charles_test_dataset
	)t
	group by merchant_id;

drop table if exists charles_test_f5_t9;
create table charles_test_f5_t9 as
	select user_id,date_received,sum(cnt) as this_day_user_received_counts from
	(
		select user_id,date_received,1 as cnt from charles_test_dataset
	)t
	group by user_id,date_received;

drop table if exists charles_test_f5_t10;
create table charles_test_f5_t10 as
	select user_id,date_received,coupon_id,sum(cnt) as this_day_user_received_coupon_counts from
	(
		select user_id,coupon_id,date_received,1 as cnt from charles_test_dataset
	)t
	group by user_id,date_received,coupon_id;

drop table if exists charles_test_f5_t11;
create table charles_test_f5_t11 as
	select user_id,count(*) as user_coupons from
	(select distinct user_id,merchant_id from charles_test_dataset)t
	group by user_id;
	
drop table if exists charles_test_f5_t12;
create table charles_test_f5_t12 as
	select merchant_id,count(*) as merchant_coupons from
	(select distinct user_id,merchant_id from charles_test_dataset)t
	group by merchant_id;

--new-add features
drop table if exists charles_test_f5_t13;
create table charles_test_f5_t13 as
	select user_id,coupon_id,date_received,cnt-1 as user_last_received_coupon_counts,ucnt-1 as user_last_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received asc) as cnt,row_number() over(partition by user_id order by date_received asc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_test_dataset)p
	)t; 

drop table if exists charles_test_f5_tmp;
create table charles_test_f5_tmp as
	select a.*,b.user_coupon_history_consume_rate as  user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,date_received,discount_rate,row_number() over(partition by user_id order by date_received asc) as received_tag,
		case when instr(discount_rate,":")=0 then 1.0-discount_rate else cast(split_part(discount_rate,":",2) as double)/cast(split_part(discount_rate,":",1) as double) end as coupon_discount,
		case when distance="null" then -2.0 else distance/10 end as distance_rate from
		(select distinct user_id,coupon_id,date_received,discount_rate,distance from charles_test_dataset)p
	)a left outer join charles_test_f4_t2 b
	on a.user_id=b.user_id and a.coupon_id=b.coupon_id;

drop table if exists charles_test_f5_t14;
create table charles_test_f5_t14 as
	select a.user_id,a.coupon_id,a.date_received,
	case when b.discount_rate is null then -2.0 when instr(b.discount_rate,":")=0 then 0.0 else 1.0 end as user_last_coupon_type,
	case when b.date_received is null then -2.0 else datediff(to_date(a.date_received,"yyyymmdd"),to_date(b.date_received,"yyyymmdd"),"dd") end as user_last_date_diff,
	case when b.coupon_id is null then -2 when a.coupon_id=b.coupon_id then 1 else 0 end as user_last_same_coupon,
	case when b.coupon_id is null then -2 when a.user_coupon_history_consume_rate<b.user_coupon_history_consume_rate then 1 else 0 end as user_last_high_consume_rate,
	case when b.coupon_id is null then -2 when a.coupon_discount<b.coupon_discount then 1 else 0 end as user_last_high_discount from
	(select * from charles_test_f5_tmp)a
	left outer join
	(select user_id,coupon_id,date_received,discount_rate,coupon_discount,user_coupon_history_consume_rate,received_tag+1 as received_tag from charles_test_f5_tmp)b
	on a.user_id=b.user_id and a.received_tag=b.received_tag;

drop table if exists charles_test_f5_t15;
create table charles_test_f5_t15 as
	select a.user_id,a.coupon_id,a.date_received,
	case when b.discount_rate is null then -2.0 when instr(b.discount_rate,":")=0 then 0.0 else 1.0 end as user_later_coupon_type,
	case when b.date_received is null then -2.0 else datediff(to_date(a.date_received,"yyyymmdd"),to_date(b.date_received,"yyyymmdd"),"dd") end as user_later_date_diff,
	case when b.coupon_id is null then -2 when a.coupon_id=b.coupon_id then 1 else 0 end as user_later_same_coupon,
	case when b.coupon_id is null then -2 when a.user_coupon_history_consume_rate<b.user_coupon_history_consume_rate then 1 else 0 end as user_later_high_consume_rate,
	case when b.coupon_id is null then -2 when a.coupon_discount<b.coupon_discount then 1 else 0 end as user_later_high_discount from
	(select * from charles_test_f5_tmp)a
	left outer join
	(select user_id,coupon_id,date_received,discount_rate,coupon_discount,user_coupon_history_consume_rate,received_tag-1 as received_tag from charles_test_f5_tmp)b
	on a.user_id=b.user_id and a.received_tag=b.received_tag;

drop table if exists charles_test_f5_t16;
create table charles_test_f5_t16 as
	select e.*,f.user_later_coupon_type,f.user_later_date_diff,f.user_later_same_coupon,f.user_later_high_consume_rate,f.user_later_high_discount from
	(
		select c.*,d.user_last_coupon_type,d.user_last_date_diff,d.user_last_same_coupon,d.user_last_high_consume_rate,d.user_last_high_discount from
		(
			select a.*,b.user_last_received_coupon_counts,b.user_last_received_coupons from
			(select distinct user_id,coupon_id,date_received from charles_test_dataset) 
			a left outer join charles_test_f5_t13 b
			on a.user_id=b.user_id and a.coupon_id=b.coupon_id and a.date_received=b.date_received
		)c left outer join charles_test_f5_t14 d
		on c.user_id=d.user_id and c.coupon_id=d.coupon_id and c.date_received=d.date_received
	)e left outer join charles_test_f5_t15 f
	on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received;

drop table if exists charles_test_f5_t17;
create table charles_test_f5_t17 as
	select a.user_id,a.coupon_id,
	case when b.user_average_discount_rate is null then -2 when a.coupon_discount<b.user_average_discount_rate then 1 else 0 end as user_coupon_low_avg_discount,
	case when b.user_minimum_discount_rate is null then -2 when a.coupon_discount<b.user_minimum_discount_rate then 1 else 0 end as user_coupon_low_min_discount,
	case when a.distance_rate=-2 then -2 when b.user_consume_average_distance is null then -2 when a.distance_rate>b.user_consume_average_distance then 1 else 0 end as user_coupon_far_avg_distance,
	case when a.distance_rate=-2 then -2 when b.user_consume_maximum_distance is null then -2 when a.distance_rate>b.user_consume_maximum_distance then 1 else 0 end as user_coupon_far_max_distance from
	(select distinct user_id,coupon_id,coupon_discount,distance_rate from charles_test_f5_tmp)a
	left outer join
	(select user_id,user_average_discount_rate,user_minimum_discount_rate,user_consume_average_distance,user_consume_maximum_distance from charles_test_f1)b
	on a.user_id=b.user_id;
	
-- 合并特征
drop table if exists charles_test_f5;
create table charles_test_f5 as
	select w.*,x.merchant_coupons from
	(
		select u.*,v.user_coupons from
		(
			select s.*,t.this_day_user_received_coupon_counts from
			(
				select q.*,r.this_day_user_received_counts from
				(
					select o.*, p.merchant_users from
					(
						select m.*, n.user_merchants from
						(
							select k.*, l.user_merchant_received_counts from
							(
								select i.*,j.merchant_received_coupon_counts from
								(
									select g.*,h.merchant_received_counts from
									(
										select e.*,f.user_later_received_coupon_counts,f.user_later_received_coupons from
										(
											select c.*,d.user_received_coupon_counts from
											(
												select a.*,b.user_received_counts from
												(select distinct user_id,merchant_id,coupon_id,date_received from charles_test_dataset) 
												a left outer join charles_test_f5_t1 b
												on a.user_id=b.user_id
											)c left outer join charles_test_f5_t2 d
											on c.user_id=d.user_id and c.coupon_id=d.coupon_id
										)e left outer join charles_test_f5_t3 f
										on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
									)g left outer join charles_test_f5_t4 h
									on g.merchant_id=h.merchant_id
								)i left outer join charles_test_f5_t5 j
								on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
							)k left outer join charles_test_f5_t6 l
							on k.user_id=l.user_id and k.merchant_id=l.merchant_id
						)m left outer join charles_test_f5_t7 n
						on m.user_id=n.user_id
					)o left outer join charles_test_f5_t8 p
					on o.merchant_id=p.merchant_id
				)q left outer join charles_test_f5_t9 r
				on q.user_id=r.user_id and q.date_received=r.date_received
			)s left outer join charles_test_f5_t10 t
			on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received
		)u left outer join charles_test_f5_t11 v
		on u.user_id=v.user_id
	)w left outer join charles_test_f5_t12 x
	on w.merchant_id=x.merchant_id;

-- 合并所有特征，生成训练数据、测试数据
drop table if exists charles_train;
create table charles_train as
	select o.*,p.user_coupon_low_avg_discount,p.user_coupon_low_min_discount,p.user_coupon_far_avg_distance,p.user_coupon_far_max_distance from
	(
		select m.*,n.user_later_coupon_type,n.user_later_date_diff,n.user_later_same_coupon,n.user_later_high_consume_rate,n.user_later_high_discount,n.user_last_coupon_type,n.user_last_date_diff,n.user_last_same_coupon,n.user_last_high_consume_rate,n.user_last_high_discount,n.user_last_received_coupon_counts,n.user_last_received_coupons from
		(
			select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
			(
				select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.user_later_received_coupons,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users,j.this_day_user_received_counts,j.this_day_user_received_coupon_counts,j.user_coupons as user_coupons,j.merchant_coupons as merchant_coupons from
				(
					select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
					(
						select e.*,f.user_merchant_consume_counts,f.user_merchant_common_counts,f.user_merchant_common_rate,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
						(
							select c.*,d.merchant_common_consume_counts,d.merchant_common_consume_rate,d.merchant_consume_counts,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_maximum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
							(
								select a.*,b.user_common_consume_counts,b.user_common_consume_rate,b.user_consume_counts,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_30_rate,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_30_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_consume_minimum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
								(
									select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,max(label) as label,day_of_week,day_of_month,
									case when 0<=coupon_discount_floor and coupon_discount_floor<=30 then 1 else 0 end as is_30_floor,
									case when 30<coupon_discount_floor and coupon_discount_floor<=50 then 1 else 0 end as is_50_floor,
									case when 50<coupon_discount_floor and coupon_discount_floor<=200 then 1 else 0 end as is_200_floor,
									case when 200<coupon_discount_floor then 1 else 0 end as is_other_floor,
									case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
									case when distance!="null" then distance/10.0 else -2.0 end as distance_rate from
									(
										select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
										weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
				  						cast(substr(date_received,7,2) as bigint) as day_of_month,
										case when date_pay="null" then 0 when datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd")>15.0 then 0 else 1 end as label,
										case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
										case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
										case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
										from charles_train_dataset
									)t
									group by user_id,merchant_id,coupon_id,date_received,discount_rate,distance,coupon_type,coupon_discount_amount,coupon_discount_floor,day_of_week,day_of_month
								)a left outer join charles_train_f1 b
								on a.user_id=b.user_id
							)c left outer join charles_train_f2 d
							on c.merchant_id=d.merchant_id
						)e left outer join charles_train_f3 f
						on e.user_id=f.user_id and e.merchant_id=f.merchant_id
					)g left outer join charles_train_f4_t1 h
					on g.coupon_id=h.coupon_id
				)i left outer join charles_train_f5 j
				on i.user_id=j.user_id and i.coupon_id=j.coupon_id and i.date_received=j.date_received
			)k left outer join charles_train_f4_t2 l
			on k.user_id=l.user_id and k.coupon_id=l.coupon_id
		)m left outer join charles_train_f5_t16 n
		on m.user_id=n.user_id and m.coupon_id=n.coupon_id and m.date_received=n.date_received
	)o left outer join charles_train_f5_t17 p
	on o.user_id=p.user_id and o.coupon_id=p.coupon_id;

drop table if exists charles_test;
create table charles_test as
	select o.*,p.user_coupon_low_avg_discount,p.user_coupon_low_min_discount,p.user_coupon_far_avg_distance,p.user_coupon_far_max_distance from
	(
		select m.*,n.user_later_coupon_type,n.user_later_date_diff,n.user_later_same_coupon,n.user_later_high_consume_rate,n.user_later_high_discount,n.user_last_coupon_type,n.user_last_date_diff,n.user_last_same_coupon,n.user_last_high_consume_rate,n.user_last_high_discount,n.user_last_received_coupon_counts,n.user_last_received_coupons from
		(
			select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
			(
				select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.user_later_received_coupons,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users,j.this_day_user_received_counts,j.this_day_user_received_coupon_counts,j.user_coupons as user_coupons,j.merchant_coupons as merchant_coupons from
				(
					select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
					(
						select e.*,f.user_merchant_consume_counts,f.user_merchant_common_counts,f.user_merchant_common_rate,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
						(
							select c.*,d.merchant_common_consume_counts,d.merchant_common_consume_rate,d.merchant_consume_counts,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_maximum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
							(
								select a.*,b.user_common_consume_counts,b.user_common_consume_rate,b.user_consume_counts,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_30_rate,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_30_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_consume_minimum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
								(
									select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,day_of_week,day_of_month,
									case when 0<=coupon_discount_floor and coupon_discount_floor<=30 then 1 else 0 end as is_30_floor,
									case when 30<coupon_discount_floor and coupon_discount_floor<=50 then 1 else 0 end as is_50_floor,
									case when 50<coupon_discount_floor and coupon_discount_floor<=200 then 1 else 0 end as is_200_floor,
									case when 200<coupon_discount_floor then 1 else 0 end as is_other_floor,
									case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
									case when distance!="null" then distance/10.0 else -2.0 end as distance_rate from
									(
										select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
										weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
				  						cast(substr(date_received,7,2) as bigint) as day_of_month,
										case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
										case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
										case when instr(discount_rate,":")=0 then -2.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
										from (select distinct user_id,merchant_id,coupon_id,discount_rate,distance,date_received from charles_test_dataset)tt
									)t
								)a left outer join charles_test_f1 b
								on a.user_id=b.user_id
							)c left outer join charles_test_f2 d
							on c.merchant_id=d.merchant_id
						)e left outer join charles_test_f3 f
						on e.user_id=f.user_id and e.merchant_id=f.merchant_id
					)g left outer join charles_test_f4_t1 h
					on g.coupon_id=h.coupon_id
				)i left outer join charles_test_f5 j
				on i.user_id=j.user_id and i.coupon_id=j.coupon_id and i.date_received=j.date_received
			)k left outer join charles_test_f4_t2 l
			on k.user_id=l.user_id and k.coupon_id=l.coupon_id
		)m left outer join charles_test_f5_t16 n
		on m.user_id=n.user_id and m.coupon_id=n.coupon_id and m.date_received=n.date_received
	)o left outer join charles_test_f5_t17 p
	on o.user_id=p.user_id and o.coupon_id=p.coupon_id;

-- fill null with -2
drop table if exists charles_train_fillna;
drop table if exists charles_test_fillna;

PAI 
-name FillMissingValues 
-project algo_public 
-Dconfigs="user_common_consume_counts,null,-2;user_common_consume_rate,null,-2;user_consume_counts,null,-2;merchant_common_consume_counts,null,-2;merchant_common_consume_rate,null,-2;merchant_consume_counts,null,-2;user_merchant_consume_counts,null,-2;user_merchant_common_counts,null,-2;user_merchant_common_rate,null,-2;user_coupon_low_avg_discount,null,-2;user_coupon_low_min_discount,null,-2;user_coupon_far_avg_distance,null,-2;user_coupon_far_max_distance,null,-2;merchant_coupons,null,-2;user_coupons,null,-2;day_of_week,null,-2;day_of_month,null,-2;coupon_type,null,-2;coupon_discount,null,-2;distance_rate,null,-2;user_received_counts,null,-2;user_none_consume_counts,null,-2;user_coupon_consume_counts,null,-2;user_coupon_consume_rate,null,-2;user_average_discount_rate,null,-2;user_minimum_discount_rate,null,-2;user_maximum_discount_rate,null,-2;user_consume_merchants,null,-2;user_consume_coupons,null,-2;user_average_consume_time_rate,null,-2;user_consume_merchants_rate,null,-2;user_consume_coupons_rate,null,-2;user_merchant_average_consume_counts,null,-2;user_average_coupon_consume_counts,null,-2;user_coupon_discount_floor_30_rate,null,-2;user_coupon_discount_floor_50_rate,null,-2;user_coupon_discount_floor_200_rate,null,-2;user_coupon_discount_floor_others_rate,null,-2;user_consume_discount_floor_30_rate,null,-2;user_consume_discount_floor_50_rate,null,-2;user_consume_discount_floor_200_rate,null,-2;user_consume_discount_floor_others_rate,null,-2;user_consume_average_distance,null,-2;user_consume_maximum_distance,null,-2;user_consume_minimum_distance,null,-2;user_online_action_counts,null,-2;user_online_action_0_rate,null,-2;user_online_action_1_rate,null,-2;user_online_action_2_rate,null,-2;user_online_none_consume_counts,null,-2;user_online_coupon_consume_counts,null,-2;user_online_coupon_consume_rate,null,-2;user_offline_none_consume_rate,null,-2;user_offline_coupon_consume_rate,null,-2;user_offline_rate,null,-2;merchant_average_discount_rate,null,-2;merchant_minimum_discount_rate,null,-2;merchant_maximum_discount_rate,null,-2;merchant_consume_users,null,-2;merchant_consume_coupons,null,-2;merchant_average_consume_time_rate,null,-2;merchant_received_counts,null,-2;merchant_none_consume_counts,null,-2;merchant_coupon_consume_counts,null,-2;merchant_coupon_consume_rate,null,-2;merchant_consume_users_rate,null,-2;merchant_consume_coupons_rate,null,-2;merchant_user_average_consume_counts,null,-2;merchant_average_coupon_consume_counts,null,-2;merchant_consume_average_distance,null,-2;merchant_consume_maximum_distance,null,-2;merchant_consume_minimum_distance,null,-2;user_merchant_coupon_counts,null,-2;user_merchant_none_consume_counts,null,-2;user_merchant_coupon_consume_counts,null,-2;user_merchant_coupon_consume_rate,null,-2;user_coupon_consume_merchant_rate,null,-2;merchant_coupon_consume_user_rate,null,-2;coupon_history_counts,null,-2;coupon_history_consume_counts,null,-2;coupon_history_consume_rate,null,-2;coupon_history_consume_time_rate,null,-2;user_coupon_history_received_counts,null,-2;user_coupon_history_consume_counts,null,-2;user_coupon_history_consume_rate,null,-2;user_dataset_received_counts,null,-2;user_received_coupon_counts,null,-2;user_later_received_coupon_counts,null,-2;user_later_received_coupons,null,-2;merchant_dataset_received_counts,null,-2;merchant_received_coupon_counts,null,-2;user_merchant_received_counts,null,-2;user_merchants,null,-2;merchant_users,null,-2;this_day_user_received_counts,null,-2;this_day_user_received_coupon_counts,null,-2"
-DoutputTableName="charles_train_fillna" 
-DinputTableName="charles_train";

PAI 
-name FillMissingValues 
-project algo_public 
-Dconfigs="user_common_consume_counts,null,-2;user_common_consume_rate,null,-2;user_consume_counts,null,-2;merchant_common_consume_counts,null,-2;merchant_common_consume_rate,null,-2;merchant_consume_counts,null,-2;user_merchant_consume_counts,null,-2;user_merchant_common_counts,null,-2;user_merchant_common_rate,null,-2;user_coupon_low_avg_discount,null,-2;user_coupon_low_min_discount,null,-2;user_coupon_far_avg_distance,null,-2;user_coupon_far_max_distance,null,-2;merchant_coupons,null,-2;user_coupons,null,-2;day_of_week,null,-2;day_of_month,null,-2;coupon_type,null,-2;coupon_discount,null,-2;distance_rate,null,-2;user_received_counts,null,-2;user_none_consume_counts,null,-2;user_coupon_consume_counts,null,-2;user_coupon_consume_rate,null,-2;user_average_discount_rate,null,-2;user_minimum_discount_rate,null,-2;user_maximum_discount_rate,null,-2;user_consume_merchants,null,-2;user_consume_coupons,null,-2;user_average_consume_time_rate,null,-2;user_consume_merchants_rate,null,-2;user_consume_coupons_rate,null,-2;user_merchant_average_consume_counts,null,-2;user_average_coupon_consume_counts,null,-2;user_coupon_discount_floor_30_rate,null,-2;user_coupon_discount_floor_50_rate,null,-2;user_coupon_discount_floor_200_rate,null,-2;user_coupon_discount_floor_others_rate,null,-2;user_consume_discount_floor_30_rate,null,-2;user_consume_discount_floor_50_rate,null,-2;user_consume_discount_floor_200_rate,null,-2;user_consume_discount_floor_others_rate,null,-2;user_consume_average_distance,null,-2;user_consume_maximum_distance,null,-2;user_consume_minimum_distance,null,-2;user_online_action_counts,null,-2;user_online_action_0_rate,null,-2;user_online_action_1_rate,null,-2;user_online_action_2_rate,null,-2;user_online_none_consume_counts,null,-2;user_online_coupon_consume_counts,null,-2;user_online_coupon_consume_rate,null,-2;user_offline_none_consume_rate,null,-2;user_offline_coupon_consume_rate,null,-2;user_offline_rate,null,-2;merchant_average_discount_rate,null,-2;merchant_minimum_discount_rate,null,-2;merchant_maximum_discount_rate,null,-2;merchant_consume_users,null,-2;merchant_consume_coupons,null,-2;merchant_average_consume_time_rate,null,-2;merchant_received_counts,null,-2;merchant_none_consume_counts,null,-2;merchant_coupon_consume_counts,null,-2;merchant_coupon_consume_rate,null,-2;merchant_consume_users_rate,null,-2;merchant_consume_coupons_rate,null,-2;merchant_user_average_consume_counts,null,-2;merchant_average_coupon_consume_counts,null,-2;merchant_consume_average_distance,null,-2;merchant_consume_maximum_distance,null,-2;merchant_consume_minimum_distance,null,-2;user_merchant_coupon_counts,null,-2;user_merchant_none_consume_counts,null,-2;user_merchant_coupon_consume_counts,null,-2;user_merchant_coupon_consume_rate,null,-2;user_coupon_consume_merchant_rate,null,-2;merchant_coupon_consume_user_rate,null,-2;coupon_history_counts,null,-2;coupon_history_consume_counts,null,-2;coupon_history_consume_rate,null,-2;coupon_history_consume_time_rate,null,-2;user_coupon_history_received_counts,null,-2;user_coupon_history_consume_counts,null,-2;user_coupon_history_consume_rate,null,-2;user_dataset_received_counts,null,-2;user_received_coupon_counts,null,-2;user_later_received_coupon_counts,null,-2;user_later_received_coupons,null,-2;merchant_dataset_received_counts,null,-2;merchant_received_coupon_counts,null,-2;user_merchant_received_counts,null,-2;user_merchants,null,-2;merchant_users,null,-2;this_day_user_received_counts,null,-2;this_day_user_received_coupon_counts,null,-2"
-DoutputTableName="charles_test_fillna" 
-DinputTableName="charles_test";

-- xgb train
drop table if exists charles_xgb_train;
create table charles_xgb_train as select * from charles_train_fillna;

drop table if exists charles_xgb_test;
create table charles_xgb_test as select * from charles_test_fillna;

select count(*) from charles_xgb_train; -- 883645
select count(*) from charles_xgb_test; -- 1024520

drop table if exists charles_xgb_test_pred;
DROP OFFLINEMODEL IF EXISTS charles_xgboost_new_1;

-- train
PAI
-name xgboost
-project algo_public
-Deta="0.1"
-Dobjective="binary:logistic" --"rank:pairwise"
-DitemDelimiter=","
-Dseed="13"
-Dnum_round="1000"
-DlabelColName="label"
-DinputTableName="charles_xgb_train"  --输入表
-DenableSparse="false"
-Dmax_depth="10"
-Dsubsample="0.6"
-Dcolsample_bytree="0.6"
-DmodelName="charles_xgboost_new_1"  --输出模型
-Dgamma="2"
-Dlambda="30" 
-DfeatureColNames="user_common_consume_counts,user_common_consume_rate,user_consume_counts,merchant_common_consume_counts,merchant_common_consume_rate,merchant_consume_counts,user_merchant_consume_counts,user_merchant_common_counts,user_merchant_common_rate,user_coupon_low_avg_discount,user_coupon_low_min_discount,user_coupon_far_avg_distance,user_coupon_far_max_distance,user_later_coupon_type,user_later_date_diff,user_later_same_coupon,user_later_high_consume_rate,user_later_high_discount,user_last_coupon_type,user_last_date_diff,user_last_same_coupon,user_last_high_consume_rate,user_last_high_discount,user_last_received_coupon_counts,user_last_received_coupons,merchant_coupons,user_coupons,day_of_week,day_of_month,coupon_type,is_30_floor,is_50_floor,is_200_floor,is_other_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_maximum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_30_rate,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_30_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_consume_minimum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_maximum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,merchant_consume_minimum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,user_later_received_coupons,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users,this_day_user_received_counts,this_day_user_received_coupon_counts"
		--特征字段
-Dbase_score="0.2"
-Dmin_child_weight="100"
-DkvDelimiter=":";

-- predict
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="user_id,coupon_id,date_received"
-DmodelName="charles_xgboost_new_1"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="charles_xgb_test_pred" --输出表
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="user_common_consume_counts,user_common_consume_rate,user_consume_counts,merchant_common_consume_counts,merchant_common_consume_rate,merchant_consume_counts,user_merchant_consume_counts,user_merchant_common_counts,user_merchant_common_rate,user_coupon_low_avg_discount,user_coupon_low_min_discount,user_coupon_far_avg_distance,user_coupon_far_max_distance,user_later_coupon_type,user_later_date_diff,user_later_same_coupon,user_later_high_consume_rate,user_later_high_discount,user_last_coupon_type,user_last_date_diff,user_last_same_coupon,user_last_high_consume_rate,user_last_high_discount,user_last_received_coupon_counts,user_last_received_coupons,merchant_coupons,user_coupons,day_of_week,day_of_month,coupon_type,is_30_floor,is_50_floor,is_200_floor,is_other_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_maximum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_30_rate,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_30_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_consume_minimum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_maximum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,merchant_consume_minimum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,user_later_received_coupons,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users,this_day_user_received_counts,this_day_user_received_coupon_counts"
		--特征字段
-DinputTableName="charles_xgb_test" --输入表
-DenableSparse="false";

drop table if exists charles_xgb_submission_new_3;
create table charles_xgb_submission_new_3 as 
	select user_id,coupon_id,date_received,
		case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	from charles_xgb_test_pred;
select * from charles_xgb_submission_new_3 limit 100;
-- 1 0.76024283
