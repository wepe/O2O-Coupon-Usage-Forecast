drop table if exists ty_features;
create table ty_features as
select uf.*,mf.*,ucf.*,umf.*
from
ty_ucf_feature ucf
join
ty_mf_feature mf
on(ucf.ucf_merchant_id = mf.mf_merchant_id and ucf.ucf_which = mf.mf_which)
join
ty_uf_feature uf
on(ucf.ucf_user_id = uf.uf_user_id and ucf.ucf_which = uf.uf_which)
join
ty_umf_feature umf
on(ucf.ucf_user_id = umf.umf_user_id and ucf.ucf_merchant_id = umf.umf_merchant_id and ucf.ucf_which = umf.umf_which)
;



drop table if exists ty_train_offline;
create table ty_train_offline as
select * from ty_features where ucf_which > 1;

drop table if exists ty_test_offline;
create table ty_test_offline as
select * from ty_features where ucf_which = 1;

drop table if exists ty_train_online;
create table ty_train_online as
select * from ty_features where ucf_which > 0;

drop table if exists ty_test_online;
create table ty_test_online as
select * from ty_features where ucf_which = 0;