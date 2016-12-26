drop table if exists wepon_level1_rf4_d2_pred;
drop table if exists wepon_level1_rf4_d3_pred;
DROP OFFLINEMODEL IF EXISTS wepon_level1_rf4;

--train
PAI 
-name randomforests 
-project algo_public
-DinputTableName="wepon_d1_fillna" --������
-DmodelName="wepon_level1_rf4"  --����ģ��
-DlabelColName="label" 
-DfeatureColNames="days_distance,coupon_count,distinct_coupon_count,day_of_month,is_man_jian,discount_man,discount_jian,discount_rate,label_coupon_feature_receive_count,label_coupon_feature_buy_count,label_coupon_feature_rate,merchant_avg_distance,merchant_median_distance,merchant_max_distance,merchant_user_buy_count,merchant_min_distance,sales_use_coupon,transform_rate,coupon_rate,total_coupon,total_sales,user_avg_distance,user_min_distance,user_max_distance,user_median_distance,avg_diff_date_datereceived,min_diff_date_datereceived,max_diff_date_datereceived,buy_use_coupon,buy_use_coupon_rate,user_coupon_transform_rate,count_merchant,buy_total,coupon_received,user_merchant_buy_total,user_merchant_received,user_merchant_any,user_merchant_buy_use_coupon,user_merchant_buy_common,user_merchant_coupon_transform_rate,user_merchant_coupon_buy_rate,user_merchant_common_buy_rate,user_merchant_rate,this_month_user_receive_all_coupon_count,this_month_user_receive_same_coupon_count,this_day_user_receive_all_coupon_count,this_day_user_receive_same_coupon_count,this_month_user_receive_same_coupon_lastone,this_month_user_receive_same_coupon_firstone,label_merchant_user_count,label_user_merchant_count,label_merchant_coupon_count,label_merchant_coupon_type_count,label_user_merchant_coupon_count,label_same_coupon_count_later,label_coupon_count_later,label_user_coupon_feature_receive_count,label_user_coupon_feature_buy_count,label_user_coupon_feature_rate,weekday1,weekday2,weekday3,weekday4,weekday5,weekday6,weekday7"
-DtreeNum="700"
-DalgorithmTypes="30,650"
-DrandomColNum="42"
-DminNumObj="450"
-DmaxTreeDeep="8"
-DmaxRecordSize="700000";

--predict  wepon_d2_fillna

PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DmodelName="wepon_level1_rf4"   --ģ��
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28" 
-DoutputTableName="wepon_level1_rf4_d2_pred" --������
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DinputTableName="wepon_d2_fillna" --������
-DenableSparse="false"
-DappendColNames="user_id,coupon_id,date_received,label";

--predict wepon_d3_fillna

PAI
-name prediction
-project algo_public
-DdetailColName="prediction_detail"
-DmodelName="wepon_level1_rf4"   --ģ��
-DitemDelimiter=","
-DresultColName="prediction_result"
-Dlifecycle="28" 
-DoutputTableName="wepon_level1_rf4_d3_pred" --������
-DscoreColName="prediction_score"
-DkvDelimiter=":"
-DinputTableName="wepon_d3_fillna" --������
-DenableSparse="false"
-DappendColNames="user_id,coupon_id,date_received";
