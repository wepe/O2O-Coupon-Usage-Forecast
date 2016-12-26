-- 3. user-merchant features
--		user_merchant_coupon_counts				用户领取商家的优惠券次数
--		user_merchant_none_consume_counts		用户领取商家的优惠券后不核销次数
--		user_merchant_coupon_consume_counts		用户领取商家的优惠券后核销次数
--		user_merchant_coupon_consume_rate		用户领取商家的优惠券后核销率
--		user_none_consume_merchant_rate			用户对每个商家的不核销次数占用户总的不核销次数的比重
--		user_coupon_consume_merchant_rate		用户对每个商家的优惠券核销次数占用户总的核销次数的比重
--		user_none_consume_merchant_rate			用户对每个商家的不核销次数占商家总的不核销次数的比重
--		user_coupon_consume_merchant_rate		用户对每个商家的优惠券核销次数占商家总的核销次数的比重

-- ############## for dataset3 ##############
drop table if exists charles_d3_f3_t1;
create table charles_d3_f3_t1 as
	select user_id,merchant_id,sum(cnt) as user_merchant_coupon_counts,sum(none_consume_counts) as user_merchant_none_consume_counts,sum(consume_counts) as user_merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_merchant_coupon_consume_rate from
	(
		select user_id,merchant_id,consume_counts,1 as cnt,1-consume_counts as none_consume_counts from charles_d3_tmp
	)t
	group by user_id,merchant_id;

drop table if exists charles_d3_f3;
create table charles_d3_f3 as
	select c.*,case when d.merchant_consume_counts=0 then -999.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
	(
		select a.*,case when b.user_consume_counts=0 then -999.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
		charles_d3_f3_t1 a join
		(
			select user_id,sum(consume_counts) as user_consume_counts from charles_d3_tmp group by user_id
		)b
		on a.user_id=b.user_id
	)c join
	(
		select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_d3_tmp group by merchant_id
	)d
	on c.merchant_id=d.merchant_id;

-- ############## for dataset2 ##############
drop table if exists charles_d2_f3_t1;
create table charles_d2_f3_t1 as
	select user_id,merchant_id,sum(cnt) as user_merchant_coupon_counts,sum(none_consume_counts) as user_merchant_none_consume_counts,sum(consume_counts) as user_merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_merchant_coupon_consume_rate from
	(
		select user_id,merchant_id,consume_counts,1 as cnt,1-consume_counts as none_consume_counts from charles_d2_tmp
	)t
	group by user_id,merchant_id;

drop table if exists charles_d2_f3;
create table charles_d2_f3 as
	select c.*,case when d.merchant_consume_counts=0 then -999.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
	(
		select a.*,case when b.user_consume_counts=0 then -999.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
		charles_d2_f3_t1 a join
		(
			select user_id,sum(consume_counts) as user_consume_counts from charles_d2_tmp group by user_id
		)b
		on a.user_id=b.user_id
	)c join
	(
		select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_d2_tmp group by merchant_id
	)d
	on c.merchant_id=d.merchant_id;

-- ############## for dataset1 ##############
drop table if exists charles_d1_f3_t1;
create table charles_d1_f3_t1 as
	select user_id,merchant_id,sum(cnt) as user_merchant_coupon_counts,sum(none_consume_counts) as user_merchant_none_consume_counts,sum(consume_counts) as user_merchant_coupon_consume_counts,sum(consume_counts)/sum(cnt) as user_merchant_coupon_consume_rate from
	(
		select user_id,merchant_id,consume_counts,1 as cnt,1-consume_counts as none_consume_counts from charles_d1_tmp
	)t
	group by user_id,merchant_id;

drop table if exists charles_d1_f3;
create table charles_d1_f3 as
	select c.*,case when d.merchant_consume_counts=0 then -999.0 else c.user_merchant_coupon_consume_counts/d.merchant_consume_counts end as merchant_coupon_consume_user_rate from
	(
		select a.*,case when b.user_consume_counts=0 then -999.0 else a.user_merchant_coupon_consume_counts/b.user_consume_counts end as user_coupon_consume_merchant_rate from
		charles_d1_f3_t1 a join
		(
			select user_id,sum(consume_counts) as user_consume_counts from charles_d1_tmp group by user_id
		)b
		on a.user_id=b.user_id
	)c join
	(
		select merchant_id,sum(consume_counts) as merchant_consume_counts from charles_d1_tmp group by merchant_id
	)d
	on c.merchant_id=d.merchant_id;
	