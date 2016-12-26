
-- level2的训练集，dataset2
create table wepon_level2_d2 as
select a.user_id,a.coupon_id,a.date_received,a.label,a.rf4,b.rf3,c.rf2,d.rf1,e.xgb1,f.xgb2,g.xgb3,h.xgb4,i.gbdt1,j.gbdt2,k.gbdt3,l.gbdt4
from
	(
	  select distinct user_id,coupon_id,date_received,label,probability as rf4 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf4_d2_pred
	  )t
	)a
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as rf3 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf3_d2_pred
	  )t
	)b
	on a.user_id=b.user_id and a.coupon_id=b.coupon_id and a.date_received=b.date_received and a.label=b.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as rf2 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf2_d2_pred
	  )t
	)c
	on a.user_id=c.user_id and a.coupon_id=c.coupon_id and a.date_received=c.date_received and a.label=c.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as rf1 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf1_d2_pred
	  )t
	)d
	on a.user_id=d.user_id and a.coupon_id=d.coupon_id and a.date_received=d.date_received and a.label=d.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as xgb1 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb1_d2_pred
	  )t
	)e
	on a.user_id=e.user_id and a.coupon_id=e.coupon_id and a.date_received=e.date_received and a.label=e.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as xgb2 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb2_d2_pred
	  )t
	)f
	on a.user_id=f.user_id and a.coupon_id=f.coupon_id and a.date_received=f.date_received and a.label=f.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as xgb3 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb3_d2_pred
	  )t
	)g
	on a.user_id=g.user_id and a.coupon_id=g.coupon_id and a.date_received=g.date_received and a.label=g.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as xgb4 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb4_d2_pred
	  )t
	)h
	on a.user_id=h.user_id and a.coupon_id=h.coupon_id and a.date_received=h.date_received and a.label=h.label
left outer join

	(
	  select distinct user_id,coupon_id,date_received,label,probability as gbdt1 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt1_d2_pred
	  )t
	)i
	on a.user_id=i.user_id and a.coupon_id=i.coupon_id and a.date_received=i.date_received and a.label=i.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as gbdt2 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt2_d2_pred
	  )t
	)j
	on a.user_id=j.user_id and a.coupon_id=j.coupon_id and a.date_received=j.date_received and a.label=j.label
left outer join
	(
	  select distinct user_id,coupon_id,date_received,label,probability as gbdt3 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt3_d2_pred
	  )t
	)k
	on a.user_id=k.user_id and a.coupon_id=k.coupon_id and a.date_received=k.date_received and a.label=k.label
left outer join

	(
	  select distinct user_id,coupon_id,date_received,label,probability as gbdt4 from 
	  (
	  select user_id,coupon_id,date_received,label,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt4_d2_pred
	  )t
	)l
	on a.user_id=l.user_id and a.coupon_id=l.coupon_id and a.date_received=l.date_received and a.label=l.label;
	

-- level2的测试集，dataset3
create table wepon_level2_d3 as
select a.user_id,a.coupon_id,a.date_received,a.rf4,b.rf3,c.rf2,d.rf1,e.xgb1,f.xgb2,g.xgb3,h.xgb4,i.gbdt1,j.gbdt2,k.gbdt3,l.gbdt4
from
	(
	  select distinct user_id,coupon_id,date_received,probability as rf4 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf4_d3_pred
	  )t
	)a
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as rf3 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf3_d3_pred
	  )t
	)b
	on a.user_id=b.user_id and a.coupon_id=b.coupon_id and a.date_received=b.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as rf2 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf2_d3_pred
	  )t
	)c
	on a.user_id=c.user_id and a.coupon_id=c.coupon_id and a.date_received=c.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as rf1 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_rf1_d3_pred
	  )t
	)d
	on a.user_id=d.user_id and a.coupon_id=d.coupon_id and a.date_received=d.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as xgb1 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb1_d3_pred
	  )t
	)e
	on a.user_id=e.user_id and a.coupon_id=e.coupon_id and a.date_received=e.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as xgb2 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb2_d3_pred
	  )t
	)f
	on a.user_id=f.user_id and a.coupon_id=f.coupon_id and a.date_received=f.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as xgb3 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb3_d3_pred
	  )t
	)g
	on a.user_id=g.user_id and a.coupon_id=g.coupon_id and a.date_received=g.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as xgb4 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_xgb4_d3_pred
	  )t
	)h
	on a.user_id=h.user_id and a.coupon_id=h.coupon_id and a.date_received=h.date_received
left outer join

	(
	  select distinct user_id,coupon_id,date_received,probability as gbdt1 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt1_d3_pred
	  )t
	)i
	on a.user_id=i.user_id and a.coupon_id=i.coupon_id and a.date_received=i.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as gbdt2 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt2_d3_pred
	  )t
	)j
	on a.user_id=j.user_id and a.coupon_id=j.coupon_id and a.date_received=j.date_received
left outer join
	(
	  select distinct user_id,coupon_id,date_received,probability as gbdt3 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt3_d3_pred
	  )t
	)k
	on a.user_id=k.user_id and a.coupon_id=k.coupon_id and a.date_received=k.date_received
left outer join

	(
	  select distinct user_id,coupon_id,date_received,probability as gbdt4 from 
	  (
	  select user_id,coupon_id,date_received,case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability
	  from wepon_level1_gbdt4_d3_pred
	  )t
	)l
	on a.user_id=l.user_id and a.coupon_id=l.coupon_id and a.date_received=l.date_received;

-- 加上原始的特征

create table wepon_level2_d2_origin as
select a.*,b.rf1,b.rf2,b.rf3,b.rf4,b.xgb1,b.xgb2,b.xgb3,b.xgb4,b.gbdt1,b.gbdt2,b.gbdt3,b.gbdt4
from wepon_d2_fillna a left outer join wepon_level2_d2 b 
on a.user_id=b.user_id and a.coupon_id =b.coupon_id and a.date_received =b.date_received and a.label=b.label;


create table wepon_level2_d3_origin as
select a.*,b.rf1,b.rf2,b.rf3,b.rf4,b.xgb1,b.xgb2,b.xgb3,b.xgb4,b.gbdt1,b.gbdt2,b.gbdt3,b.gbdt4
from wepon_d3_fillna a left outer join wepon_level2_d3 b 
on a.user_id=b.user_id and a.coupon_id =b.coupon_id and a.date_received =b.date_received;