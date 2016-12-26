--record view
select count(*) from charles_train_offline_stage2; --15982387
select count(*) from charles_train_online_stage2; --783492172
select count(*) from charles_prediction_stage2; --1032126

--user view
select count(distinct user_id) from charles_train_offline_stage2; --4906687
select count(distinct user_id) from charles_train_online_stage2; --11179551
select count(distinct user_id) from charles_prediction_stage2; --696470  103万条记录待预测，70万不同用户，大部分用户都是单条记录

select count(*) from --696459 11个用户没有出现在训练集线下数据里
(select distinct user_id from charles_train_offline_stage2)a
inner join 
(select distinct user_id from charles_prediction_stage2)b
on a.user_id=b.user_id;

select count(*) from -- 3837443  线下用户中大概有3/4同时有线上活动
(select distinct user_id from charles_train_offline_stage2)a
inner join 
(select distinct user_id from charles_train_online_stage2)b
on a.user_id=b.user_id;

select count(*) from --585913 预测记录中大部分是线上线下都活跃的用户，感觉需要考虑一下提取线上特征
(select distinct user_id from charles_train_offline_stage2)a
inner join 
(select distinct user_id from charles_train_online_stage2)b
on a.user_id=b.user_id
inner join 
(select distinct user_id from charles_prediction_stage2)c
on a.user_id=c.user_id;

--merchant view
select count(distinct merchant_id) from charles_train_offline_stage2; --8742
select count(distinct merchant_id) from charles_train_online_stage2; --49999
select count(distinct merchant_id) from charles_prediction_stage2; --1943

select count(*) from --1943 预测集中商家被训练集线下商家全覆盖
(select distinct merchant_id from charles_train_offline_stage2)a
inner join 
(select distinct merchant_id from charles_prediction_stage2)b
on a.merchant_id=b.merchant_id;

select count(*) from --0 --线上线下商家ID仍不交叉
(select distinct merchant_id from charles_train_offline_stage2)a
inner join 
(select distinct merchant_id from charles_train_online_stage2)b
on a.merchant_id=b.merchant_id;

select count(*) from --0
(select distinct merchant_id from charles_train_offline_stage2)a
inner join 
(select distinct merchant_id from charles_train_online_stage2)b
on a.merchant_id=b.merchant_id
inner join 
(select distinct merchant_id from charles_prediction_stage2)c
on a.merchant_id=c.merchant_id;

--coupon view
select count(distinct coupon_id) from charles_train_offline_stage2; --10064
select count(distinct coupon_id) from charles_train_online_stage2; --233562
select count(distinct coupon_id) from charles_prediction_stage2; --2548

select count(*) from --1658 待预测优惠券中很多在线下优惠券中出现过
(select distinct coupon_id from charles_train_offline_stage2)a
inner join 
(select distinct coupon_id from charles_prediction_stage2)b
on a.coupon_id=b.coupon_id;

select count(*) from -- 1 应该是null
(select distinct coupon_id from charles_train_offline_stage2)a
inner join 
(select distinct coupon_id from charles_train_online_stage2)b
on a.coupon_id=b.coupon_id;

select count(*) from -- 0
(select distinct coupon_id from charles_train_offline_stage2)a
inner join 
(select distinct coupon_id from charles_train_online_stage2)b
on a.coupon_id=b.coupon_id
inner join 
(select distinct coupon_id from charles_prediction_stage2)c
on a.coupon_id=c.coupon_id;


select count(*) from charles_dataset3; --1032126
select count(*) from charles_feature3; --4882751
select count(*) from charles_online_feature3; --37044158

select count(*) from charles_dataset2; --2354100
select count(*) from charles_feature2; --3853258
select count(*) from charles_online_feature2; --34894809

select count(*) from charles_dataset1; --1241841
select count(*) from charles_feature1; --6008750
select count(*) from charles_online_feature1; --30425303

select count(*) from --1024520
(
	select * from charles_prediction_stage2
	group by user_id,coupon_id,date_received
)t;
