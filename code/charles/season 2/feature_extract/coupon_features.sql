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

-- ############## for dataset3 ##############
drop table if exists charles_d3_f4_t1;
create table charles_d3_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts, sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_feature3
	)t
	group by coupon_id;

drop table if exists charles_d3_f4_t2;
create table charles_d3_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_feature3
	)t
	group by user_id,coupon_id;

-- ############## for dataset2 ##############
drop table if exists charles_d2_f4_t1;
create table charles_d2_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts, sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_feature2
	)t
	group by coupon_id;

drop table if exists charles_d2_f4_t2;
create table charles_d2_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_feature2
	)t
	group by user_id,coupon_id;

-- ############## for dataset1 ##############
drop table if exists charles_d1_f4_t1;
create table charles_d1_f4_t1 as
	select coupon_id,sum(cnt) as coupon_history_counts, sum(consume_counts) as coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as coupon_history_consume_rate,1-avg(date_consumed)/15.0 as coupon_history_consume_time_rate from
	(
		select coupon_id,1 as cnt,
			case when date_pay!="null" then datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd") else 15.0 end as date_consumed,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_feature1
	)t
	group by coupon_id;

drop table if exists charles_d1_f4_t2;
create table charles_d1_f4_t2 as
	select user_id,coupon_id,sum(cnt) as user_coupon_history_received_counts, sum(consume_counts) as user_coupon_history_consume_counts,sum(consume_counts)/sum(cnt) as user_coupon_history_consume_rate from
	(
		select user_id,coupon_id,1 as cnt,
			case when date_pay=="null" then 0.0 when datediff(to_date(date_received,"yyyymmdd"),to_date(date_pay,"yyyymmdd"),"dd")>15.0 then 0.0 else 1.0 end as consume_counts
		from charles_feature1
	)t
	group by user_id,coupon_id;
