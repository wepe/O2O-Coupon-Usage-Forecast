-- 数据集划分:
--               	(date_received)                              
--    	test  dataset: 20160701~20160731 ,features from 20160201~20160630  (测试集，dataset对应label窗，feature对应特征窗)
--    	train dataset: 20160601~20160630 ,features from 20160101~20160531  (训练集，dataset对应label窗，feature对应特征窗)
create table if not exists charles_test_dataset as select * from prediction_stage2;
create table if not exists charles_test_feature as select * from train_offline_stage2 where "20160201"<=date_received and date_received<="20160630";
create table if not exists charles_test_online_feature as select * from train_online_stage2 where "20160201"<=date_received and date_received<="20160630";

create table if not exists charles_train_dataset as select * from train_offline_stage2 where "20160601"<=date_received and date_received<="20160630";
create table if not exists charles_train_feature as select * from train_offline_stage2 where "20160101"<=date_received and date_received<="20160531";
create table if not exists charles_train_online_feature as select * from train_online_stage2 where "20160101"<=date_received and date_received<="20160531";

-- 提取特征
-- 1. user features
--		user_received_counts					用户领取优惠券次数
-- 		user_none_consume_counts				用户获得优惠券但没有消费的次数
-- 		user_coupon_consume_counts 				用户获得优惠券并核销次数
-- 		user_coupon_consume_rate				用户领取优惠券后进行核销率
--		user_coupon_discount_floor_50_rate		用户满0~50减的优惠券核销率
--		user_coupon_discount_floor_200_rate		用户满50~200减的优惠券核销率
--		user_coupon_discount_floor_500_rate		用户满200~500减的优惠券核销率
--		user_coupon_discount_floor_others_rate	用户其他满减的优惠券核销率
--		user_consume_discount_floor_50_rate		用户核销满0~50减的优惠券占所有核销优惠券的比重
--		user_consume_discount_floor_200_rate	用户核销50~200减的优惠券占所有核销优惠券的比重
--		user_consume_discount_floor_500_rate	用户核销200~500减的优惠券占所有核销优惠券的比重
--		user_consume_discount_floor_others_rate	用户核销其他满减的优惠券占所有核销优惠券的比重
--		user_average_discount_rate				用户核销优惠券的平均消费折率
--		user_minimum_discount_rate				用户核销优惠券的最低消费折率
--		user_consume_merchants					用户核销过优惠券的不同商家数量
--		user_consume_merchants_rate				用户核销过优惠券的不同商家数量占所有不同商家的比重
--		user_merchant_average_consume_counts	用户平均核销每个商家多少张优惠券
--		user_consume_coupons					用户核销过的不同优惠券数量
--		user_consume_coupons_rate				用户核销过的不同优惠券数量占所有不同优惠券的比重
--  	user_average_coupon_consume_counts		用户平均每种优惠券核销多少张
--		user_average_consume_time_rate			用户核销优惠券的平均时间率
--		user_consume_average_distance			用户核销优惠券中的平均用户-商家距离
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
	case when distance!="null" then distance/10.0 else -999.0 end as distance_rate,
	case when date_pay="null" then -999.0 when date_consumed>15.0 then -1.0 else 1.0-date_consumed/15.0 end as date_consumed_rate,
	case when date_pay="null" then 0.0 when date_consumed>15.0 then 0.0 else 1.0 end as consume_counts,
	case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount from
	(
		select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,
			case when date_pay!="null" then datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd") else 999.0 end as date_consumed,
			case when instr(discount_rate,":")=0 then 0 else 1 end as coupon_type,
			case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
			case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
		from charles_train_feature
	)t;

drop table if exists charles_train_online_feature_tmp;
create table if not exists charles_train_online_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_train_online_feature;

drop table if exists charles_train_f1_t1;
create table charles_train_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(500_floor_counts) as 500_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_floor_counts
		from charles_train_feature_tmp
	)t
	group by user_id;

drop table if exists charles_train_f1_t2;
create table charles_train_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(500_consumed_floor_counts) as 500_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_consumed_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_train_feature_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_train_f1_t3;
create table charles_train_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance from
	(
		select user_id,distance_rate from charles_train_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_train_f1_off;
create table charles_train_f1_off as
	select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance from
	(
		select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
		b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
		b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
			case when a.50_floor_total_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
			case when a.200_floor_total_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
			case when a.500_floor_total_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.500_floor_total_counts end as user_coupon_discount_floor_500_rate,
			case when a.other_floor_total_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_500_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
		charles_train_f1_t1 a left outer join charles_train_f1_t2 b
		on a.user_id=b.user_id
	)c left outer join charles_train_f1_t3 d
	on c.user_id=d.user_id;

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
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -999.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -999.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -999.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_train_f1_off a left outer join charles_train_f1_on b
	on a.user_id=b.user_id;


drop table if exists charles_test_feature_tmp;
create table if not exists charles_test_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,coupon_type,coupon_discount_floor,
	case when distance!="null" then distance/10.0 else -999.0 end as distance_rate,
	case when date_pay="null" then -999.0 when date_consumed>15.0 then -1.0 else 1.0-date_consumed/15.0 end as date_consumed_rate,
	case when date_pay="null" then 0.0 when date_consumed>15.0 then 0.0 else 1.0 end as consume_counts,
	case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount from
	(
		select user_id,merchant_id,coupon_id,date_received,date_pay,distance,discount_rate,
			case when date_pay!="null" then datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd") else 999.0 end as date_consumed,
			case when instr(discount_rate,":")=0 then 0 else 1 end as coupon_type,
			case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
			case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
		from charles_test_feature
	)t;

drop table if exists charles_test_online_feature_tmp;
create table if not exists charles_test_online_feature_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_test_online_feature;

drop table if exists charles_test_f1_t1;
create table charles_test_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(500_floor_counts) as 500_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_floor_counts
		from charles_test_feature_tmp
	)t
	group by user_id;

drop table if exists charles_test_f1_t2;
create table charles_test_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(500_consumed_floor_counts) as 500_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_consumed_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_test_feature_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_test_f1_t3;
create table charles_test_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance from
	(
		select user_id,distance_rate from charles_test_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_test_f1_off;
create table charles_test_f1_off as
	select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance from
	(
		select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
		b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
		b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
			case when a.50_floor_total_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
			case when a.200_floor_total_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
			case when a.500_floor_total_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.500_floor_total_counts end as user_coupon_discount_floor_500_rate,
			case when a.other_floor_total_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_500_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
		charles_test_f1_t1 a left outer join charles_test_f1_t2 b
		on a.user_id=b.user_id
	)c left outer join charles_test_f1_t3 d
	on c.user_id=d.user_id;

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
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -999.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -999.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -999.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_test_f1_off a left outer join charles_test_f1_on b
	on a.user_id=b.user_id;

-- 2. merchant features
--		merchant_received_counts				商家优惠券被领取次数
--		merchant_none_consume_counts			商家优惠券被领取后不核销次数
--		merchant_coupon_consume_counts			商家优惠券被领取后核销次数
--		merchant_coupon_consume_rate			商家优惠券被领取后核销率
--		merchant_average_discount_rate			商家优惠券核销的平均消费折率
--		merchant_minimum_discount_rate			商家优惠券核销的最低消费折率
--		merchant_consume_users					核销商家优惠券的不同用户数量
--		merchant_consume_users_rate				核销商家优惠券的不同用户数量占领取不同的用户比重
-- 		merchant_user_average_consume_counts	商家优惠券平均每个用户核销多少张
--		merchant_consume_coupons				商家被核销过的不同优惠券数量
--   	merchant_consume_coupons_rate			商家被核销过的不同优惠券数量占所有领取过的不同优惠券数量的比重
--		merchant_average_coupon_consume_counts	商家平均每种优惠券核销多少张
--		merchant_average_consume_time_rate		商家被核销优惠券的平均时间率
--		merchant_consume_average_distance		商家被核销优惠券中的平均用户-商家距离
--		merchant_consume_maximum_distance		商家被核销优惠券中的最大用户-商家距离

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
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_train_feature_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_train_f2_t3;
create table charles_train_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance from
	(
		select merchant_id,distance_rate from charles_train_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_train_f2;
create table charles_train_f2 as
	select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance from
	(
		select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
		charles_train_f2_t2 a right outer join charles_train_f2_t1 b
		on a.merchant_id=b.merchant_id
	)c left outer join charles_train_f2_t3 d
	on c.merchant_id=d.merchant_id;


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
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_test_feature_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_test_f2_t3;
create table charles_test_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance from
	(
		select merchant_id,distance_rate from charles_test_feature_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_test_f2;
create table charles_test_f2 as
	select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance from
	(
		select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
		charles_test_f2_t2 a right outer join charles_test_f2_t1 b
		on a.merchant_id=b.merchant_id
	)c left outer join charles_test_f2_t3 d
	on c.merchant_id=d.merchant_id;
-- 3. user-merchant features
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

drop table if exists charles_train_f3;
create table charles_train_f3 as
	select c.*,case when d.merchant_consume_counts=0 then -999.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
	(
		select a.*,case when b.user_consume_counts=0 then -999.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
		charles_train_f3_t1 a join
		(
			select user_id,sum(consume_counts) as user_consume_counts from charles_train_feature_tmp group by user_id
		)b
		on a.user_id=b.user_id
	)c join
	(
		select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_train_feature_tmp group by merchant_id
	)d
	on c.merchant_id=d.merchant_id;


drop table if exists charles_test_f3_t1;
create table charles_test_f3_t1 as
	select user_id,merchant_id,sum(cnt) as user_merchant_coupon_counts,sum(none_consume_counts) as user_merchant_none_consume_counts,sum(consume_counts) as user_merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_merchant_coupon_consume_rate from
	(
		select user_id,merchant_id,consume_counts,1 as cnt,1-consume_counts as none_consume_counts from charles_test_feature_tmp
	)t
	group by user_id,merchant_id;

drop table if exists charles_test_f3;
create table charles_test_f3 as
	select c.*,case when d.merchant_consume_counts=0 then -999.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
	(
		select a.*,case when b.user_consume_counts=0 then -999.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
		charles_test_f3_t1 a join
		(
			select user_id,sum(consume_counts) as user_consume_counts from charles_test_feature_tmp group by user_id
		)b
		on a.user_id=b.user_id
	)c join
	(
		select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_test_feature_tmp group by merchant_id
	)d
	on c.merchant_id=d.merchant_id;

-- 4. coupon features
--		coupon_type								优惠券类型(直接优惠为0, 满减为1)
--		coupon_discount 						优惠券折率
--		coupon_discount_floor 					满减优惠券的最低消费
--		coupon_history_counts 					历史出现次数
--      coupon_history_consume_counts			历史核销次数
--		coupon_history_consume_rate         	历史核销率
--		coupon_history_consume_time_rate		历史核销时间率
--	user_coupon_history_features
--		user_coupon_history_received_counts		历史上用户领取该优惠券次数
--		user_coupon_history_consume_counts		历史上用户消费该优惠券次数
--		user_coupon_history_consume_rate		历史上用户对该优惠券的核销率

drop table if exists charles_train_f4_t1;
create table charles_train_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts, sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_train_feature
	)t
	group by coupon_id;

drop table if exists charles_train_f4_t2;
create table charles_train_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_train_feature
	)t
	group by user_id,coupon_id;


drop table if exists charles_test_f4_t1;
create table charles_test_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts, sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_test_feature
	)t
	group by coupon_id;

drop table if exists charles_test_f4_t2;
create table charles_test_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_test_feature
	)t
	group by user_id,coupon_id;
		
-- 5. other features
--		user_received_counts					用户领取的所有优惠券数目
--		user_received_coupon_counts				用户领取的特定优惠券数目
--		user_later_received_coupon_counts		用户此次之后领取的特定优惠券数目
--		merchant_received_counts				商家被领取的优惠券数目
--		merchant_received_coupon_counts			商家被领取的特定优惠券数目
--  	user_merchant_received_counts			用户领取特定商家的优惠券数目
--		user_merchants 							用户领取的不同商家数目
--		merchant_users 							商家被多少不同用户领取的数目

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
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts from
	(
		select user_id,coupon_id,date_received, row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt from 
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

-- 合并特征
drop table if exists charles_train_f5;
create table charles_train_f5 as
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
						select e.*,f.user_later_received_coupon_counts from
						(
							select c.*,d.user_received_coupon_counts from
							(
								select a.*,b.user_received_counts from
								(select distinct user_id,merchant_id,coupon_id,date_received from charles_train_dataset) 
								a join charles_train_f5_t1 b
								on a.user_id=b.user_id
							)c join charles_train_f5_t2 d
							on c.user_id=d.user_id and c.coupon_id=d.coupon_id
						)e join charles_train_f5_t3 f
						on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
					)g join charles_train_f5_t4 h
					on g.merchant_id=h.merchant_id
				)i join charles_train_f5_t5 j
				on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
			)k join charles_train_f5_t6 l
			on k.user_id=l.user_id and k.merchant_id=l.merchant_id
		)m join charles_train_f5_t7 n
		on m.user_id=n.user_id
	)o join charles_train_f5_t8 p
	on o.merchant_id=p.merchant_id;


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
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts from
	(
		select user_id,coupon_id,date_received, row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt from 
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

-- 合并特征
drop table if exists charles_test_f5;
create table charles_test_f5 as
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
						select e.*,f.user_later_received_coupon_counts from
						(
							select c.*,d.user_received_coupon_counts from
							(
								select a.*,b.user_received_counts from
								(select distinct user_id,merchant_id,coupon_id,date_received from charles_test_dataset) 
								a join charles_test_f5_t1 b
								on a.user_id=b.user_id
							)c join charles_test_f5_t2 d
							on c.user_id=d.user_id and c.coupon_id=d.coupon_id
						)e join charles_test_f5_t3 f
						on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
					)g join charles_test_f5_t4 h
					on g.merchant_id=h.merchant_id
				)i join charles_test_f5_t5 j
				on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
			)k join charles_test_f5_t6 l
			on k.user_id=l.user_id and k.merchant_id=l.merchant_id
		)m join charles_test_f5_t7 n
		on m.user_id=n.user_id
	)o join charles_test_f5_t8 p
	on o.merchant_id=p.merchant_id;

-- 合并所有特征，生成训练数据、测试数据
drop table if exists charles_train;
create table charles_train as
	select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
	(
		select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users from
		(
			select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
			(
				select e.*,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
				(
					select c.*,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance from
					(
						select a.*,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_500_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_500_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
						(
							select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,max(label) as label,
							cast(coupon_discount_floor as double) as coupon_discount_floor,
							case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
							case when distance!="null" then distance/10.0 else -999.0 end as distance_rate from
							(
								select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
								case when date_pay="null" then 0 when datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd")>15.0 then 0 else 1 end as label,
								case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
								from charles_train_feature
							)t
							group by user_id,merchant_id,coupon_id,date_received,discount_rate,distance,coupon_type,coupon_discount_amount,coupon_discount_floor
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
	on k.user_id=l.user_id and k.coupon_id=l.coupon_id;

drop table if exists charles_test;
create table charles_test as
	select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
	(
		select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users from
		(
			select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
			(
				select e.*,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
				(
					select c.*,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance from
					(
						select a.*,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_500_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_500_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
						(
							select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,
							cast(coupon_discount_floor as double) as coupon_discount_floor,
							case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
							case when distance!="null" then distance/10.0 else -999.0 end as distance_rate from
							(
								select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
								case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
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
	on k.user_id=l.user_id and k.coupon_id=l.coupon_id;

-- fill null with -999
drop table if exists charles_train_fillna;
drop table if exists charles_test_fillna;

PAI 
-name FillMissingValues 
-project algo_public 
-Dconfigs="coupon_type,null,-999;coupon_discount_floor,null,-999;coupon_discount,null,-999;distance_rate,null,-999;user_received_counts,null,-999;user_none_consume_counts,null,-999;user_coupon_consume_counts,null,-999;user_coupon_consume_rate,null,-999;user_average_discount_rate,null,-999;user_minimum_discount_rate,null,-999;user_consume_merchants,null,-999;user_consume_coupons,null,-999;user_average_consume_time_rate,null,-999;user_consume_merchants_rate,null,-999;user_consume_coupons_rate,null,-999;user_merchant_average_consume_counts,null,-999;user_average_coupon_consume_counts,null,-999;user_coupon_discount_floor_50_rate,null,-999;user_coupon_discount_floor_200_rate,null,-999;user_coupon_discount_floor_500_rate,null,-999;user_coupon_discount_floor_others_rate,null,-999;user_consume_discount_floor_50_rate,null,-999;user_consume_discount_floor_200_rate,null,-999;user_consume_discount_floor_500_rate,null,-999;user_consume_discount_floor_others_rate,null,-999;user_consume_average_distance,null,-999;user_consume_maximum_distance,null,-999;user_online_action_counts,null,-999;user_online_action_0_rate,null,-999;user_online_action_1_rate,null,-999;user_online_action_2_rate,null,-999;user_online_none_consume_counts,null,-999;user_online_coupon_consume_counts,null,-999;user_online_coupon_consume_rate,null,-999;user_offline_none_consume_rate,null,-999;user_offline_coupon_consume_rate,null,-999;user_offline_rate,null,-999;merchant_average_discount_rate,null,-999;merchant_minimum_discount_rate,null,-999;merchant_consume_users,null,-999;merchant_consume_coupons,null,-999;merchant_average_consume_time_rate,null,-999;merchant_received_counts,null,-999;merchant_none_consume_counts,null,-999;merchant_coupon_consume_counts,null,-999;merchant_coupon_consume_rate,null,-999;merchant_consume_users_rate,null,-999;merchant_consume_coupons_rate,null,-999;merchant_user_average_consume_counts,null,-999;merchant_average_coupon_consume_counts,null,-999;merchant_consume_average_distance,null,-999;merchant_consume_maximum_distance,null,-999;user_merchant_coupon_counts,null,-999;user_merchant_none_consume_counts,null,-999;user_merchant_coupon_consume_counts,null,-999;user_merchant_coupon_consume_rate,null,-999;user_coupon_consume_merchant_rate,null,-999;merchant_coupon_consume_user_rate,null,-999;coupon_history_counts,null,-999;coupon_history_consume_counts,null,-999;coupon_history_consume_rate,null,-999;coupon_history_consume_time_rate,null,-999;user_coupon_history_received_counts,null,-999;user_coupon_history_consume_counts,null,-999;user_coupon_history_consume_rate,null,-999;user_dataset_received_counts,null,-999;user_received_coupon_counts,null,-999;user_later_received_coupon_counts,null,-999;merchant_dataset_received_counts,null,-999;merchant_received_coupon_counts,null,-999;user_merchant_received_counts,null,-999;user_merchants,null,-999;merchant_users,null,-999"
-DoutputTableName="charles_train_fillna" 
-DinputTableName="charles_train";

PAI 
-name FillMissingValues 
-project algo_public 
-Dconfigs="coupon_type,null,-999;coupon_discount_floor,null,-999;coupon_discount,null,-999;distance_rate,null,-999;user_received_counts,null,-999;user_none_consume_counts,null,-999;user_coupon_consume_counts,null,-999;user_coupon_consume_rate,null,-999;user_average_discount_rate,null,-999;user_minimum_discount_rate,null,-999;user_consume_merchants,null,-999;user_consume_coupons,null,-999;user_average_consume_time_rate,null,-999;user_consume_merchants_rate,null,-999;user_consume_coupons_rate,null,-999;user_merchant_average_consume_counts,null,-999;user_average_coupon_consume_counts,null,-999;user_coupon_discount_floor_50_rate,null,-999;user_coupon_discount_floor_200_rate,null,-999;user_coupon_discount_floor_500_rate,null,-999;user_coupon_discount_floor_others_rate,null,-999;user_consume_discount_floor_50_rate,null,-999;user_consume_discount_floor_200_rate,null,-999;user_consume_discount_floor_500_rate,null,-999;user_consume_discount_floor_others_rate,null,-999;user_consume_average_distance,null,-999;user_consume_maximum_distance,null,-999;user_online_action_counts,null,-999;user_online_action_0_rate,null,-999;user_online_action_1_rate,null,-999;user_online_action_2_rate,null,-999;user_online_none_consume_counts,null,-999;user_online_coupon_consume_counts,null,-999;user_online_coupon_consume_rate,null,-999;user_offline_none_consume_rate,null,-999;user_offline_coupon_consume_rate,null,-999;user_offline_rate,null,-999;merchant_average_discount_rate,null,-999;merchant_minimum_discount_rate,null,-999;merchant_consume_users,null,-999;merchant_consume_coupons,null,-999;merchant_average_consume_time_rate,null,-999;merchant_received_counts,null,-999;merchant_none_consume_counts,null,-999;merchant_coupon_consume_counts,null,-999;merchant_coupon_consume_rate,null,-999;merchant_consume_users_rate,null,-999;merchant_consume_coupons_rate,null,-999;merchant_user_average_consume_counts,null,-999;merchant_average_coupon_consume_counts,null,-999;merchant_consume_average_distance,null,-999;merchant_consume_maximum_distance,null,-999;user_merchant_coupon_counts,null,-999;user_merchant_none_consume_counts,null,-999;user_merchant_coupon_consume_counts,null,-999;user_merchant_coupon_consume_rate,null,-999;user_coupon_consume_merchant_rate,null,-999;merchant_coupon_consume_user_rate,null,-999;coupon_history_counts,null,-999;coupon_history_consume_counts,null,-999;coupon_history_consume_rate,null,-999;coupon_history_consume_time_rate,null,-999;user_coupon_history_received_counts,null,-999;user_coupon_history_consume_counts,null,-999;user_coupon_history_consume_rate,null,-999;user_dataset_received_counts,null,-999;user_received_coupon_counts,null,-999;user_later_received_coupon_counts,null,-999;merchant_dataset_received_counts,null,-999;merchant_received_coupon_counts,null,-999;user_merchant_received_counts,null,-999;user_merchants,null,-999;merchant_users,null,-999"
-DoutputTableName="charles_test_fillna" 
-DinputTableName="charles_test";

-- xgb train
drop table if exists charles_xgb_train;
create table charles_xgb_train as select * from charles_train_fillna;

drop table if exists charles_xgb_test;
create table charles_xgb_test as select * from charles_test_fillna;

select count(*) from charles_xgb_train; -- 9229221
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
-Dnum_round="400"
-DlabelColName="label"
-DinputTableName="charles_xgb_train"  --输入表
-DenableSparse="false"
-Dmax_depth="8"
-Dsubsample="0.8"
-Dcolsample_bytree="0.7"
-DmodelName="charles_xgboost_new_1"  --输出模型
-Dgamma="0"
-Dlambda="10" 
-DfeatureColNames="coupon_type,coupon_discount_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_500_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_500_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users"
		--特征字段
-Dbase_score="0.5"
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
-DfeatureColNames="coupon_type,coupon_discount_floor,coupon_discount,distance_rate,user_received_counts,user_none_consume_counts,user_coupon_consume_counts,user_coupon_consume_rate,user_average_discount_rate,user_minimum_discount_rate,user_consume_merchants,user_consume_coupons,user_average_consume_time_rate,user_consume_merchants_rate,user_consume_coupons_rate,user_merchant_average_consume_counts,user_average_coupon_consume_counts,user_coupon_discount_floor_50_rate,user_coupon_discount_floor_200_rate,user_coupon_discount_floor_500_rate,user_coupon_discount_floor_others_rate,user_consume_discount_floor_50_rate,user_consume_discount_floor_200_rate,user_consume_discount_floor_500_rate,user_consume_discount_floor_others_rate,user_consume_average_distance,user_consume_maximum_distance,user_online_action_counts,user_online_action_0_rate,user_online_action_1_rate,user_online_action_2_rate,user_online_none_consume_counts,user_online_coupon_consume_counts,user_online_coupon_consume_rate,user_offline_none_consume_rate,user_offline_coupon_consume_rate,user_offline_rate,merchant_average_discount_rate,merchant_minimum_discount_rate,merchant_consume_users,merchant_consume_coupons,merchant_average_consume_time_rate,merchant_received_counts,merchant_none_consume_counts,merchant_coupon_consume_counts,merchant_coupon_consume_rate,merchant_consume_users_rate,merchant_consume_coupons_rate,merchant_user_average_consume_counts,merchant_average_coupon_consume_counts,merchant_consume_average_distance,merchant_consume_maximum_distance,user_merchant_coupon_counts,user_merchant_none_consume_counts,user_merchant_coupon_consume_counts,user_merchant_coupon_consume_rate,user_coupon_consume_merchant_rate,merchant_coupon_consume_user_rate,coupon_history_counts,coupon_history_consume_counts,coupon_history_consume_rate,coupon_history_consume_time_rate,user_dataset_received_counts,user_received_coupon_counts,user_later_received_coupon_counts,merchant_dataset_received_counts,merchant_received_coupon_counts,user_merchant_received_counts,user_merchants,merchant_users"
		--特征字段
-DinputTableName="charles_xgb_test" --输入表
-DenableSparse="false";

drop table if exists charles_xgb_submission_new_1;
create table charles_xgb_submission_new_1 as 
	select user_id,coupon_id,date_received,
		case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	from charles_xgb_test_pred;
	
drop table if exists charles_xgb_submission_new_view_1;
create table charles_xgb_submission_new_view_1 as
	select sum(cnt) as counts,sum(pred_1_cnt) as pred_1_counts,sum(pred_0_cnt) as pred_0_counts from
	(
		select 1 as cnt,
		case when probability>=0.5 then 1 else 0 end as pred_1_cnt,
		case when probability>=0.5 then 0 else 1 end as pred_0_cnt
		from charles_xgb_submission_new_1
	)t;
select * from charles_xgb_submission_new_view_1;
-- counts,pred_1_counts,pred_0_counts
-- 1024520,30860,993660
