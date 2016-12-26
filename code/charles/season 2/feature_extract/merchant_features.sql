-- 2. merchant features
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

-- ############## for dataset3 ##############
drop table if exists charles_d3_f2_t1;
create table charles_d3_f2_t1 as
	select merchant_id,sum(cnt) as merchant_received_counts,sum(none_consume_counts) as merchant_none_consume_counts,sum(consume_counts) as merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as merchant_coupon_consume_rate,count(distinct user_id) as merchant_total_users,count(distinct coupon_id) as merchant_total_coupons from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts
		from charles_d3_tmp
	)t
	group by merchant_id;

drop table if exists charles_d3_f2_t2;
create table charles_d3_f2_t2 as
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,max(coupon_discount) as merchant_maximum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_d3_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_d3_f2_t3;
create table charles_d3_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance,min(distance_rate) as merchant_consume_minimum_distance from
	(
		select merchant_id,distance_rate from charles_d3_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_d3_f2;
create table charles_d3_f2 as
	select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
	(
		select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
		charles_d3_f2_t2 a right outer join charles_d3_f2_t1 b
		on a.merchant_id=b.merchant_id
	)c left outer join charles_d3_f2_t3 d
	on c.merchant_id=d.merchant_id;

-- ############## for dataset2 ##############
drop table if exists charles_d2_f2_t1;
create table charles_d2_f2_t1 as
	select merchant_id,sum(cnt) as merchant_received_counts,sum(none_consume_counts) as merchant_none_consume_counts,sum(consume_counts) as merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as merchant_coupon_consume_rate,count(distinct user_id) as merchant_total_users,count(distinct coupon_id) as merchant_total_coupons from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts
		from charles_d2_tmp
	)t
	group by merchant_id;

drop table if exists charles_d2_f2_t2;
create table charles_d2_f2_t2 as
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,max(coupon_discount) as merchant_maximum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_d2_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_d2_f2_t3;
create table charles_d2_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance,min(distance_rate) as merchant_consume_minimum_distance from
	(
		select merchant_id,distance_rate from charles_d2_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_d2_f2;
create table charles_d2_f2 as
	select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
	(
		select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
		charles_d2_f2_t2 a right outer join charles_d2_f2_t1 b
		on a.merchant_id=b.merchant_id
	)c left outer join charles_d2_f2_t3 d
	on c.merchant_id=d.merchant_id;
	
-- ############## for dataset1 ##############
drop table if exists charles_d1_f2_t1;
create table charles_d1_f2_t1 as
	select merchant_id,sum(cnt) as merchant_received_counts,sum(none_consume_counts) as merchant_none_consume_counts,sum(consume_counts) as merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as merchant_coupon_consume_rate,count(distinct user_id) as merchant_total_users,count(distinct coupon_id) as merchant_total_coupons from
	(
		select user_id,merchant_id,coupon_id,1 as cnt,1.0-consume_counts as none_consume_counts,consume_counts
		from charles_d1_tmp
	)t
	group by merchant_id;

drop table if exists charles_d1_f2_t2;
create table charles_d1_f2_t2 as
	select merchant_id,avg(coupon_discount) as merchant_average_discount_rate,min(coupon_discount) as merchant_minimum_discount_rate,max(coupon_discount) as merchant_maximum_discount_rate,count(distinct user_id) as merchant_consume_users,count(distinct coupon_id) as merchant_consume_coupons,avg(date_consumed_rate) as merchant_average_consume_time_rate from
	(
		select user_id,merchant_id,coupon_id,coupon_discount,date_consumed_rate
		from charles_d1_tmp
		where consume_counts=1.0
	)t
	group by merchant_id;

drop table if exists charles_d1_f2_t3;
create table charles_d1_f2_t3 as
	select merchant_id,avg(distance_rate) as merchant_consume_average_distance,max(distance_rate) as merchant_consume_maximum_distance,min(distance_rate) as merchant_consume_minimum_distance from
	(
		select merchant_id,distance_rate from charles_d1_tmp where consume_counts=1.0 and distance!="null"
	)t
	group by merchant_id;

drop table if exists charles_d1_f2;
create table charles_d1_f2 as
	select c.*,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
	(
		select a.*,b.merchant_received_counts,b.merchant_none_consume_counts,b.merchant_coupon_consume_counts,b.merchant_coupon_consume_rate,a.merchant_consume_users/b.merchant_total_users as merchant_consume_users_rate,a.merchant_consume_coupons/b.merchant_total_coupons as merchant_consume_coupons_rate,b.merchant_coupon_consume_counts/b.merchant_total_users as merchant_user_average_consume_counts,b.merchant_coupon_consume_counts/b.merchant_total_coupons as merchant_average_coupon_consume_counts from
		charles_d1_f2_t2 a right outer join charles_d1_f2_t1 b
		on a.merchant_id=b.merchant_id
	)c left outer join charles_d1_f2_t3 d
	on c.merchant_id=d.merchant_id;
