drop table if exists wepon_lr_pred;
DROP OFFLINEMODEL IF EXISTS wepon_lr_1;


-- train
PAI
-name LogisticRegression
-project algo_public
-DmodelName="wepon_lr_1"
-DregularizedLevel="1.0"
-DmaxIter="100"
-DregularizedType="l2"
-Depsilon="0.000001" 
-DlabelColName="label"
-DgoodValue="1"
-DinputTableName="wepon_level2_d2"
-DfeatureColNames="rf1,rf2,rf3,rf4,xgb1,xgb2,xgb3,xgb4,gbdt1,gbdt2,gbdt3,gbdt4";


-- predict
PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DappendColNames="user_id,coupon_id,date_received"
-DmodelName="wepon_lr_1"
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28"
-DoutputTableName="wepon_lr_pred"
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DfeatureColNames="rf1,rf2,rf3,rf4,xgb1,xgb2,xgb3,xgb4,gbdt1,gbdt2,gbdt3,gbdt4"
-DinputTableName="wepon_level2_d3"
-DenableSparse="false";

drop table o2o_result;
create table o2o_result as select user_id,coupon_id,date_received,
case when prediction_result=0 then 1.0-prediction_score else prediction_score end as probability from wepon_lr_pred;
select count(*) from o2o_result;
