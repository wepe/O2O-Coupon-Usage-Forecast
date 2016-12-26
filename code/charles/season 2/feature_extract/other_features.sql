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

-- ############## for dataset3 ##############
drop table if exists charles_d3_f5_t1;
create table charles_d3_f5_t1 as
	select user_id,sum(cnt) as user_received_counts from
	(
		select user_id,1 as cnt from charles_dataset3
	)t
	group by user_id;

drop table if exists charles_d3_f5_t2;
create table charles_d3_f5_t2 as
	select user_id,coupon_id,sum(cnt) as user_received_coupon_counts from
	(
		select user_id,coupon_id,1 as cnt from charles_dataset3
	)t
	group by user_id,coupon_id;

drop table if exists charles_d3_f5_t3;
create table charles_d3_f5_t3 as
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts,ucnt-1 as user_later_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt,row_number() over(partition by user_id order by date_received desc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_dataset3)p
	)t; 

drop table if exists charles_d3_f5_t4;
create table charles_d3_f5_t4 as
	select merchant_id,sum(cnt) as merchant_received_counts from
	(
		select merchant_id,1 as cnt from charles_dataset3
	)t
	group by merchant_id;

drop table if exists charles_d3_f5_t5;
create table charles_d3_f5_t5 as
	select merchant_id,coupon_id,sum(cnt) as merchant_received_coupon_counts from
	(
		select merchant_id,coupon_id,1 as cnt from charles_dataset3
	)t
	group by merchant_id,coupon_id;

drop table if exists charles_d3_f5_t6;
create table charles_d3_f5_t6 as
	select user_id,merchant_id,sum(cnt) as user_merchant_received_counts from
	(
		select user_id,merchant_id,1 as cnt from charles_dataset3
	)t
	group by user_id,merchant_id;

drop table if exists charles_d3_f5_t7;
create table charles_d3_f5_t7 as
	select user_id,count(distinct merchant_id) as user_merchants from
	(
		select user_id,merchant_id from charles_dataset3
	)t
	group by user_id;

drop table if exists charles_d3_f5_t8;
create table charles_d3_f5_t8 as
	select merchant_id,count(distinct user_id) as merchant_users from
	(
		select user_id,merchant_id from charles_dataset3
	)t
	group by merchant_id;

drop table if exists charles_d3_f5_t9;
create table charles_d3_f5_t9 as
	select user_id,date_received,sum(cnt) as this_day_user_received_counts from
	(
		select user_id,date_received,1 as cnt from charles_dataset3
	)t
	group by user_id,date_received;

drop table if exists charles_d3_f5_t10;
create table charles_d3_f5_t10 as
	select user_id,date_received,coupon_id,sum(cnt) as this_day_user_received_coupon_counts from
	(
		select user_id,coupon_id,date_received,1 as cnt from charles_dataset3
	)t
	group by user_id,date_received,coupon_id;

-- 合并特征
drop table if exists charles_d3_f5;
create table charles_d3_f5 as
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
										(select distinct user_id,merchant_id,coupon_id,date_received from charles_dataset3) 
										a join charles_d3_f5_t1 b
										on a.user_id=b.user_id
									)c join charles_d3_f5_t2 d
									on c.user_id=d.user_id and c.coupon_id=d.coupon_id
								)e join charles_d3_f5_t3 f
								on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
							)g join charles_d3_f5_t4 h
							on g.merchant_id=h.merchant_id
						)i join charles_d3_f5_t5 j
						on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
					)k join charles_d3_f5_t6 l
					on k.user_id=l.user_id and k.merchant_id=l.merchant_id
				)m join charles_d3_f5_t7 n
				on m.user_id=n.user_id
			)o join charles_d3_f5_t8 p
			on o.merchant_id=p.merchant_id
		)q join charles_d3_f5_t9 r
		on q.user_id=r.user_id and q.date_received=r.date_received
	)s join charles_d3_f5_t10 t
	on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received;

-- ############## for dataset2 ##############
drop table if exists charles_d2_f5_t1;
create table charles_d2_f5_t1 as
	select user_id,sum(cnt) as user_received_counts from
	(
		select user_id,1 as cnt from charles_dataset2
	)t
	group by user_id;

drop table if exists charles_d2_f5_t2;
create table charles_d2_f5_t2 as
	select user_id,coupon_id,sum(cnt) as user_received_coupon_counts from
	(
		select user_id,coupon_id,1 as cnt from charles_dataset2
	)t
	group by user_id,coupon_id;

drop table if exists charles_d2_f5_t3;
create table charles_d2_f5_t3 as
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts,ucnt-1 as user_later_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt,row_number() over(partition by user_id order by date_received desc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_dataset2)p
	)t; 

drop table if exists charles_d2_f5_t4;
create table charles_d2_f5_t4 as
	select merchant_id,sum(cnt) as merchant_received_counts from
	(
		select merchant_id,1 as cnt from charles_dataset2
	)t
	group by merchant_id;

drop table if exists charles_d2_f5_t5;
create table charles_d2_f5_t5 as
	select merchant_id,coupon_id,sum(cnt) as merchant_received_coupon_counts from
	(
		select merchant_id,coupon_id,1 as cnt from charles_dataset2
	)t
	group by merchant_id,coupon_id;

drop table if exists charles_d2_f5_t6;
create table charles_d2_f5_t6 as
	select user_id,merchant_id,sum(cnt) as user_merchant_received_counts from
	(
		select user_id,merchant_id,1 as cnt from charles_dataset2
	)t
	group by user_id,merchant_id;

drop table if exists charles_d2_f5_t7;
create table charles_d2_f5_t7 as
	select user_id,count(distinct merchant_id) as user_merchants from
	(
		select user_id,merchant_id from charles_dataset2
	)t
	group by user_id;

drop table if exists charles_d2_f5_t8;
create table charles_d2_f5_t8 as
	select merchant_id,count(distinct user_id) as merchant_users from
	(
		select user_id,merchant_id from charles_dataset2
	)t
	group by merchant_id;

drop table if exists charles_d2_f5_t9;
create table charles_d2_f5_t9 as
	select user_id,date_received,sum(cnt) as this_day_user_received_counts from
	(
		select user_id,date_received,1 as cnt from charles_dataset2
	)t
	group by user_id,date_received;

drop table if exists charles_d2_f5_t10;
create table charles_d2_f5_t10 as
	select user_id,date_received,coupon_id,sum(cnt) as this_day_user_received_coupon_counts from
	(
		select user_id,coupon_id,date_received,1 as cnt from charles_dataset2
	)t
	group by user_id,date_received,coupon_id;

-- 合并特征
drop table if exists charles_d2_f5;
create table charles_d2_f5 as
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
										(select distinct user_id,merchant_id,coupon_id,date_received from charles_dataset2) 
										a join charles_d2_f5_t1 b
										on a.user_id=b.user_id
									)c join charles_d2_f5_t2 d
									on c.user_id=d.user_id and c.coupon_id=d.coupon_id
								)e join charles_d2_f5_t3 f
								on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
							)g join charles_d2_f5_t4 h
							on g.merchant_id=h.merchant_id
						)i join charles_d2_f5_t5 j
						on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
					)k join charles_d2_f5_t6 l
					on k.user_id=l.user_id and k.merchant_id=l.merchant_id
				)m join charles_d2_f5_t7 n
				on m.user_id=n.user_id
			)o join charles_d2_f5_t8 p
			on o.merchant_id=p.merchant_id
		)q join charles_d2_f5_t9 r
		on q.user_id=r.user_id and q.date_received=r.date_received
	)s join charles_d2_f5_t10 t
	on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received;

-- ############## for dataset1 ##############
drop table if exists charles_d1_f5_t1;
create table charles_d1_f5_t1 as
	select user_id,sum(cnt) as user_received_counts from
	(
		select user_id,1 as cnt from charles_dataset1
	)t
	group by user_id;

drop table if exists charles_d1_f5_t2;
create table charles_d1_f5_t2 as
	select user_id,coupon_id,sum(cnt) as user_received_coupon_counts from
	(
		select user_id,coupon_id,1 as cnt from charles_dataset1
	)t
	group by user_id,coupon_id;

drop table if exists charles_d1_f5_t3;
create table charles_d1_f5_t3 as
	select user_id,coupon_id,date_received,cnt-1 as user_later_received_coupon_counts,ucnt-1 as user_later_received_coupons from
	(
		select user_id,coupon_id,date_received,row_number() over(partition by user_id,coupon_id order by date_received desc) as cnt,row_number() over(partition by user_id order by date_received desc) as ucnt from 
		(select distinct user_id,coupon_id,date_received from charles_dataset1)p
	)t; 

drop table if exists charles_d1_f5_t4;
create table charles_d1_f5_t4 as
	select merchant_id,sum(cnt) as merchant_received_counts from
	(
		select merchant_id,1 as cnt from charles_dataset1
	)t
	group by merchant_id;

drop table if exists charles_d1_f5_t5;
create table charles_d1_f5_t5 as
	select merchant_id,coupon_id,sum(cnt) as merchant_received_coupon_counts from
	(
		select merchant_id,coupon_id,1 as cnt from charles_dataset1
	)t
	group by merchant_id,coupon_id;

drop table if exists charles_d1_f5_t6;
create table charles_d1_f5_t6 as
	select user_id,merchant_id,sum(cnt) as user_merchant_received_counts from
	(
		select user_id,merchant_id,1 as cnt from charles_dataset1
	)t
	group by user_id,merchant_id;

drop table if exists charles_d1_f5_t7;
create table charles_d1_f5_t7 as
	select user_id,count(distinct merchant_id) as user_merchants from
	(
		select user_id,merchant_id from charles_dataset1
	)t
	group by user_id;

drop table if exists charles_d1_f5_t8;
create table charles_d1_f5_t8 as
	select merchant_id,count(distinct user_id) as merchant_users from
	(
		select user_id,merchant_id from charles_dataset1
	)t
	group by merchant_id;

drop table if exists charles_d1_f5_t9;
create table charles_d1_f5_t9 as
	select user_id,date_received,sum(cnt) as this_day_user_received_counts from
	(
		select user_id,date_received,1 as cnt from charles_dataset1
	)t
	group by user_id,date_received;

drop table if exists charles_d1_f5_t10;
create table charles_d1_f5_t10 as
	select user_id,date_received,coupon_id,sum(cnt) as this_day_user_received_coupon_counts from
	(
		select user_id,coupon_id,date_received,1 as cnt from charles_dataset1
	)t
	group by user_id,date_received,coupon_id;

-- 合并特征
drop table if exists charles_d1_f5;
create table charles_d1_f5 as
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
										(select distinct user_id,merchant_id,coupon_id,date_received from charles_dataset1) 
										a join charles_d1_f5_t1 b
										on a.user_id=b.user_id
									)c join charles_d1_f5_t2 d
									on c.user_id=d.user_id and c.coupon_id=d.coupon_id
								)e join charles_d1_f5_t3 f
								on e.user_id=f.user_id and e.coupon_id=f.coupon_id and e.date_received=f.date_received
							)g join charles_d1_f5_t4 h
							on g.merchant_id=h.merchant_id
						)i join charles_d1_f5_t5 j
						on i.merchant_id=j.merchant_id and i.coupon_id=j.coupon_id
					)k join charles_d1_f5_t6 l
					on k.user_id=l.user_id and k.merchant_id=l.merchant_id
				)m join charles_d1_f5_t7 n
				on m.user_id=n.user_id
			)o join charles_d1_f5_t8 p
			on o.merchant_id=p.merchant_id
		)q join charles_d1_f5_t9 r
		on q.user_id=r.user_id and q.date_received=r.date_received
	)s join charles_d1_f5_t10 t
	on s.user_id=t.user_id and s.coupon_id=t.coupon_id and s.date_received=t.date_received;

