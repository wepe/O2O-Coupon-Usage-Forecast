-- 提取特征
-- 1. user features
-- 2. merchant features
-- 3. user-merchant features
-- 4. coupon features
-- 5. other features

-- ############## for dataset3 ##############
drop table if exists charles_df3;
create table charles_df3 as
	select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
	(
		select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.user_later_received_coupons,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users,j.this_day_user_received_counts,j.this_day_user_received_coupon_counts from
		(
			select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
			(
				select e.*,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
				(
					select c.*,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_maximum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
					(
						select a.*,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_500_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_500_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_consume_minimum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
						(
							select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,day_of_week,day_of_month,
							cast(coupon_discount_floor as double) as coupon_discount_floor,
							case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
							case when distance!="null" then distance/10.0 else -999.0 end as distance_rate from
							(
								select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
								weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
		  						cast(substr(date_received,7,2) as bigint) as day_of_month,
								case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
								from (select distinct user_id,merchant_id,coupon_id,discount_rate,distance,date_received from charles_dataset3)tt
							)t
						)a left outer join charles_d3_f1 b
						on a.user_id=b.user_id
					)c left outer join charles_d3_f2 d
					on c.merchant_id=d.merchant_id
				)e left outer join charles_d3_f3 f
				on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g left outer join charles_d3_f4_t1 h
			on g.coupon_id=h.coupon_id
		)i left outer join charles_d3_f5 j
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id and i.date_received=j.date_received
	)k left outer join charles_d3_f4_t2 l
	on k.user_id=l.user_id and k.coupon_id=l.coupon_id;

-- ############## for dataset2 ##############
drop table if exists charles_df2;
create table charles_df2 as
	select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
	(
		select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.user_later_received_coupons,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users,j.this_day_user_received_counts,j.this_day_user_received_coupon_counts from
		(
			select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
			(
				select e.*,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
				(
					select c.*,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_maximum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
					(
						select a.*,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_500_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_500_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_consume_minimum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
						(
							select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,max(label) as label,day_of_week,day_of_month,
							cast(coupon_discount_floor as double) as coupon_discount_floor,
							case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
							case when distance!="null" then distance/10.0 else -999.0 end as distance_rate from
							(
								select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
								weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
		  						cast(substr(date_received,7,2) as bigint) as day_of_month,
								case when date_pay="null" then 0 when datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd")>15.0 then 0 else 1 end as label,
								case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
								from charles_dataset2
							)t
							group by user_id,merchant_id,coupon_id,date_received,discount_rate,distance,coupon_type,coupon_discount_amount,coupon_discount_floor,day_of_week,day_of_month
						)a left outer join charles_d2_f1 b
						on a.user_id=b.user_id
					)c left outer join charles_d2_f2 d
					on c.merchant_id=d.merchant_id
				)e left outer join charles_d2_f3 f
				on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g left outer join charles_d2_f4_t1 h
			on g.coupon_id=h.coupon_id
		)i left outer join charles_d2_f5 j
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id and i.date_received=j.date_received
	)k left outer join charles_d2_f4_t2 l
	on k.user_id=l.user_id and k.coupon_id=l.coupon_id;

-- ############## for dataset1 ##############
drop table if exists charles_df1;
create table charles_df1 as
	select k.*,l.user_coupon_history_received_counts as user_coupon_history_received_counts,l.user_coupon_history_consume_counts as user_coupon_history_consume_counts,l.user_coupon_history_consume_rate as user_coupon_history_consume_rate from
	(
		select i.*,j.user_received_counts as user_dataset_received_counts,j.user_received_coupon_counts,j.user_later_received_coupon_counts,j.user_later_received_coupons,j.merchant_received_counts as merchant_dataset_received_counts,j.merchant_received_coupon_counts,j.user_merchant_received_counts,j.user_merchants,j.merchant_users,j.this_day_user_received_counts,j.this_day_user_received_coupon_counts from
		(
			select g.*,h.coupon_history_counts,h.coupon_history_consume_counts,h.coupon_history_consume_rate,h.coupon_history_consume_time_rate from
			(
				select e.*,f.user_merchant_coupon_counts,f.user_merchant_none_consume_counts,f.user_merchant_coupon_consume_counts,f.user_merchant_coupon_consume_rate,f.user_coupon_consume_merchant_rate,f.merchant_coupon_consume_user_rate from
				(
					select c.*,d.merchant_average_discount_rate,d.merchant_minimum_discount_rate,d.merchant_maximum_discount_rate,d.merchant_consume_users,d.merchant_consume_coupons,d.merchant_average_consume_time_rate,d.merchant_received_counts,d.merchant_none_consume_counts,d.merchant_coupon_consume_counts,d.merchant_coupon_consume_rate,d.merchant_consume_users_rate,d.merchant_consume_coupons_rate,d.merchant_user_average_consume_counts,d.merchant_average_coupon_consume_counts,d.merchant_consume_average_distance,d.merchant_consume_maximum_distance,d.merchant_consume_minimum_distance from
					(
						select a.*,b.user_received_counts,b.user_none_consume_counts,b.user_coupon_consume_counts,b.user_coupon_consume_rate,b.user_average_discount_rate,b.user_minimum_discount_rate,b.user_maximum_discount_rate,b.user_consume_merchants,b.user_consume_coupons,b.user_average_consume_time_rate,b.user_consume_merchants_rate,b.user_consume_coupons_rate,b.user_merchant_average_consume_counts,b.user_average_coupon_consume_counts,b.user_coupon_discount_floor_50_rate,b.user_coupon_discount_floor_200_rate,b.user_coupon_discount_floor_500_rate,b.user_coupon_discount_floor_others_rate,b.user_consume_discount_floor_50_rate,b.user_consume_discount_floor_200_rate,b.user_consume_discount_floor_500_rate,b.user_consume_discount_floor_others_rate,b.user_consume_average_distance,b.user_consume_maximum_distance,b.user_consume_minimum_distance,b.user_online_action_counts,b.user_online_action_0_rate,b.user_online_action_1_rate,b.user_online_action_2_rate,b.user_online_none_consume_counts,b.user_online_coupon_consume_counts,b.user_online_coupon_consume_rate,b.user_offline_none_consume_rate,b.user_offline_coupon_consume_rate,b.user_offline_rate from
						(
							select user_id,merchant_id,coupon_id,date_received,discount_rate,coupon_type,max(label) as label,day_of_week,day_of_month,
							cast(coupon_discount_floor as double) as coupon_discount_floor,
							case when coupon_type=1 then coupon_discount_amount/coupon_discount_floor else 1.0-discount_rate end as coupon_discount,
							case when distance!="null" then distance/10.0 else -999.0 end as distance_rate from
							(
								select user_id,merchant_id,coupon_id,date_received,discount_rate,distance,
								weekday(to_date(date_received,"yyyymmdd")) as day_of_week,
		  						cast(substr(date_received,7,2) as bigint) as day_of_month,
								case when date_pay="null" then 0 when datediff(to_date(date_pay,"yyyymmdd"),to_date(date_received,"yyyymmdd"),"dd")>15.0 then 0 else 1 end as label,
								case when instr(discount_rate,":")=0 then 0.0 else 1.0 end as coupon_type,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",2) end as coupon_discount_amount,
								case when instr(discount_rate,":")=0 then -999.0 else split_part(discount_rate,":",1) end as coupon_discount_floor
								from charles_dataset1
							)t
							group by user_id,merchant_id,coupon_id,date_received,discount_rate,distance,coupon_type,coupon_discount_amount,coupon_discount_floor,day_of_week,day_of_month
						)a left outer join charles_d1_f1 b
						on a.user_id=b.user_id
					)c left outer join charles_d1_f2 d
					on c.merchant_id=d.merchant_id
				)e left outer join charles_d1_f3 f
				on e.user_id=f.user_id and e.merchant_id=f.merchant_id
			)g left outer join charles_d1_f4_t1 h
			on g.coupon_id=h.coupon_id
		)i left outer join charles_d1_f5 j
		on i.user_id=j.user_id and i.coupon_id=j.coupon_id and i.date_received=j.date_received
	)k left outer join charles_d1_f4_t2 l
	on k.user_id=l.user_id and k.coupon_id=l.coupon_id;
  