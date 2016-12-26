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

-- ############## for dataset3 ##############
drop table if exists charles_d3_tmp;
create table if not exists charles_d3_tmp as
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
		from charles_feature3
	)t;

drop table if exists charles_d3_online_tmp;
create table if not exists charles_d3_online_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_online_feature3;

drop table if exists charles_d3_f1_t1;
create table charles_d3_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(500_floor_counts) as 500_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_floor_counts
		from charles_d3_tmp
	)t
	group by user_id;

drop table if exists charles_d3_f1_t2;
create table charles_d3_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,max(coupon_discount) as user_maximum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(500_consumed_floor_counts) as 500_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_consumed_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_d3_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_d3_f1_t3;
create table charles_d3_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance,min(distance_rate) as user_consume_minimum_distance from
	(
		select user_id,distance_rate from charles_d3_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_d3_f1_off;
create table charles_d3_f1_off as
	select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance,d.user_consume_minimum_distance from
	(
		select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
		b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
		b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
			case when a.50_floor_total_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
			case when a.200_floor_total_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
			case when a.500_floor_total_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.500_floor_total_counts end as user_coupon_discount_floor_500_rate,
			case when a.other_floor_total_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_500_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
		charles_d3_f1_t1 a left outer join charles_d3_f1_t2 b
		on a.user_id=b.user_id
	)c left outer join charles_d3_f1_t3 d
	on c.user_id=d.user_id;

drop table if exists charles_d3_f1_on;
create table charles_d3_f1_on as
	select user_id,sum(cnt) as user_online_action_counts,sum(receive_counts) as user_online_receive_counts,sum(click_counts)/sum(cnt) as user_online_action_0_rate,sum(buy_counts)/sum(cnt) as user_online_action_1_rate,sum(receive_counts)/sum(cnt) as user_online_action_2_rate,sum(none_consume_counts) as user_online_none_consume_counts,sum(consume_counts) as user_online_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_online_coupon_consume_rate from
	(
		select user_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when action="0" then 1.0 else 0.0 end as click_counts,
			case when action="1" then 1.0 else 0.0 end as buy_counts,
			case when action="2" then 1.0 else 0.0 end as receive_counts
		from charles_d3_online_tmp
	)t
	group by user_id;

drop table if exists charles_d3_f1;
create table charles_d3_f1 as
	select a.*,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -999.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -999.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -999.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_d3_f1_off a left outer join charles_d3_f1_on b
	on a.user_id=b.user_id;

-- ############## for dataset2 ##############
drop table if exists charles_d2_tmp;
create table if not exists charles_d2_tmp as
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
		from charles_feature2
	)t;

drop table if exists charles_d2_online_tmp;
create table if not exists charles_d2_online_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_online_feature2;

drop table if exists charles_d2_f1_t1;
create table charles_d2_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(500_floor_counts) as 500_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_floor_counts
		from charles_d2_tmp
	)t
	group by user_id;

drop table if exists charles_d2_f1_t2;
create table charles_d2_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,max(coupon_discount) as user_maximum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(500_consumed_floor_counts) as 500_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_consumed_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_d2_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_d2_f1_t3;
create table charles_d2_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance,min(distance_rate) as user_consume_minimum_distance from
	(
		select user_id,distance_rate from charles_d2_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_d2_f1_off;
create table charles_d2_f1_off as
	select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance,d.user_consume_minimum_distance from
	(
		select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
		b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
		b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
			case when a.50_floor_total_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
			case when a.200_floor_total_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
			case when a.500_floor_total_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.500_floor_total_counts end as user_coupon_discount_floor_500_rate,
			case when a.other_floor_total_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_500_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
		charles_d2_f1_t1 a left outer join charles_d2_f1_t2 b
		on a.user_id=b.user_id
	)c left outer join charles_d2_f1_t3 d
	on c.user_id=d.user_id;

drop table if exists charles_d2_f1_on;
create table charles_d2_f1_on as
	select user_id,sum(cnt) as user_online_action_counts,sum(receive_counts) as user_online_receive_counts,sum(click_counts)/sum(cnt) as user_online_action_0_rate,sum(buy_counts)/sum(cnt) as user_online_action_1_rate,sum(receive_counts)/sum(cnt) as user_online_action_2_rate,sum(none_consume_counts) as user_online_none_consume_counts,sum(consume_counts) as user_online_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_online_coupon_consume_rate from
	(
		select user_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when action="0" then 1.0 else 0.0 end as click_counts,
			case when action="1" then 1.0 else 0.0 end as buy_counts,
			case when action="2" then 1.0 else 0.0 end as receive_counts
		from charles_d2_online_tmp
	)t
	group by user_id;

drop table if exists charles_d2_f1;
create table charles_d2_f1 as
	select a.*,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -999.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -999.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -999.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_d2_f1_off a left outer join charles_d2_f1_on b
	on a.user_id=b.user_id;

-- ############## for dataset1 ##############
drop table if exists charles_d1_tmp;
create table if not exists charles_d1_tmp as
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
		from charles_feature1
	)t;

drop table if exists charles_d1_online_tmp;
create table if not exists charles_d1_online_tmp as
	select user_id,merchant_id,coupon_id,date_received,date_pay,discount_rate,action,
		case when date_pay!="null" and date_received!="null" and datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") <=15 then 1.0 else 0.0 end as consume_counts
	from charles_online_feature1;

drop table if exists charles_d1_f1_t1;
create table charles_d1_f1_t1 as
	select user_id,sum(cnt) as user_received_counts,sum(none_consume_counts) as user_none_consume_counts,sum(consume_counts) as user_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_consume_rate,count(distinct merchant_id) as user_total_merchants,count(distinct coupon_id) as user_total_coupons,
		   sum(50_floor_counts) as 50_floor_total_counts,sum(200_floor_counts) as 200_floor_total_counts,sum(500_floor_counts) as 500_floor_total_counts,sum(other_floor_counts) as other_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_floor_counts
		from charles_d1_tmp
	)t
	group by user_id;

drop table if exists charles_d1_f1_t2;
create table charles_d1_f1_t2 as
	select user_id,avg(coupon_discount) as user_average_discount_rate,min(coupon_discount) as user_minimum_discount_rate,max(coupon_discount) as user_maximum_discount_rate,count(distinct merchant_id) as user_consume_merchants,count(distinct coupon_id) as user_consume_coupons,avg(date_consumed_rate) as user_average_consume_time_rate,
		   sum(50_consumed_floor_counts) as 50_consumed_floor_total_counts,sum(200_consumed_floor_counts) as 200_consumed_floor_total_counts,sum(500_consumed_floor_counts) as 500_consumed_floor_total_counts,sum(other_consumed_floor_counts) as other_consumed_floor_total_counts from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate,
			case when coupon_discount_floor>=0 and coupon_discount_floor<50 then 1.0 else 0.0 end as 50_consumed_floor_counts,
			case when coupon_discount_floor>=50 and coupon_discount_floor<200 then 1.0 else 0.0 end as 200_consumed_floor_counts,
			case when coupon_discount_floor>=200 and coupon_discount_floor<500 then 1.0 else 0.0 end as 500_consumed_floor_counts,
			case when coupon_discount_floor>=500 then 1.0 else 0.0 end as other_consumed_floor_counts
		from charles_d1_tmp
		where consume_counts=1.0
	)t
	group by user_id;

drop table if exists charles_d1_f1_t3;
create table charles_d1_f1_t3 as
	select user_id,avg(distance_rate) as user_consume_average_distance,max(distance_rate) as user_consume_maximum_distance,min(distance_rate) as user_consume_minimum_distance from
	(
		select user_id,distance_rate from charles_d1_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by user_id;

drop table if exists charles_d1_f1_off;
create table charles_d1_f1_off as
	select c.*,d.user_consume_average_distance,d.user_consume_maximum_distance,d.user_consume_minimum_distance from
	(
		select a.user_id,a.user_received_counts,a.user_none_consume_counts,a.user_coupon_consume_counts,a.user_coupon_consume_rate,
		b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,
		b.user_consume_merchants/a.user_total_merchants as user_consume_merchants_rate,b.user_consume_coupons/a.user_total_coupons as user_consume_coupons_rate,a.user_coupon_consume_counts/a.user_total_merchants as user_merchant_average_consume_counts,a.user_coupon_consume_counts/a.user_total_coupons as user_average_coupon_consume_counts,
			case when a.50_floor_total_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.50_floor_total_counts end as user_coupon_discount_floor_50_rate,
			case when a.200_floor_total_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.200_floor_total_counts end as user_coupon_discount_floor_200_rate,
			case when a.500_floor_total_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.500_floor_total_counts end as user_coupon_discount_floor_500_rate,
			case when a.other_floor_total_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.other_floor_total_counts end as user_coupon_discount_floor_others_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.50_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_50_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.200_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_200_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.500_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_500_rate,
			case when a.user_coupon_consume_counts=0.0 then -999.0 else b.other_consumed_floor_total_counts/a.user_coupon_consume_counts end as user_consume_discount_floor_others_rate from
		charles_d1_f1_t1 a left outer join charles_d1_f1_t2 b
		on a.user_id=b.user_id
	)c left outer join charles_d1_f1_t3 d
	on c.user_id=d.user_id;

drop table if exists charles_d1_f1_on;
create table charles_d1_f1_on as
	select user_id,sum(cnt) as user_online_action_counts,sum(receive_counts) as user_online_receive_counts,sum(click_counts)/sum(cnt) as user_online_action_0_rate,sum(buy_counts)/sum(cnt) as user_online_action_1_rate,sum(receive_counts)/sum(cnt) as user_online_action_2_rate,sum(none_consume_counts) as user_online_none_consume_counts,sum(consume_counts) as user_online_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_online_coupon_consume_rate from
	(
		select user_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts,
			case when action="0" then 1.0 else 0.0 end as click_counts,
			case when action="1" then 1.0 else 0.0 end as buy_counts,
			case when action="2" then 1.0 else 0.0 end as receive_counts
		from charles_d1_online_tmp
	)t
	group by user_id;

drop table if exists charles_d1_f1;
create table charles_d1_f1 as
	select a.*,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,
		case when b.user_online_none_consume_counts+a.user_none_consume_counts=0 then -999.0 else a.user_none_consume_counts/(b.user_online_none_consume_counts+a.user_none_consume_counts) end as user_offline_none_consume_rate,
		case when b.user_online_coupon_consume_counts+a.user_coupon_consume_counts=0 then -999.0 else a.user_coupon_consume_counts/(b.user_online_coupon_consume_counts+a.user_coupon_consume_counts) end as user_offline_coupon_consume_rate,
		case when b.user_online_receive_counts+a.user_received_counts=0 then -999.0 else a.user_received_counts/(b.user_online_receive_counts+a.user_received_counts) end as user_offline_rate from
	charles_d1_f1_off a left outer join charles_d1_f1_on b
	on a.user_id=b.user_id;
