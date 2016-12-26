/*
Table 1: ## charles_train_offline_stage2 ## 用户线下消费和优惠券领取行为
           field    description
0        User_id    用户ID
1    Merchant_id    商户ID
2      Coupon_id    优惠券ID：null表示无优惠券消费，此时Discount_rate和Date_received字段无意义
3  Discount_rate    优惠率：x \in [0,1]代表折扣率；x:y表示满x减y。单位是元
4       Distance    user经常活动的地点离该merchant的最近门店距离是x*500米（如果是连锁店，则取最近的一家门店），x\in[0,10]；null表示无此信息，0表示低于500米，10表示大于5公里；
5  Date_received    领取优惠券日期
6       Date_pay    消费日期：如果Date=null & Coupon_id != null，该记录表示领取优惠券但没有使用，即负样本；如果Date!=null & Coupon_id = null，则表示普通消费日期；如果Date!=null & Coupon_id != null，则表示用优惠券消费日期，即正样本；

Table 2: ## charles_train_online_stage2 ## 用户线上点击/消费和优惠券领取行为
           field    description
0        User_id    用户ID
1    Merchant_id    商户ID
2         Action    0 点击， 1购买，2领取优惠券
3      Coupon_id    优惠券ID：null表示无优惠券消费，此时Discount_rate和Date_received字段无意义。“fixed”表示该交易是限时低价活动。
4  Discount_rate    优惠率：x \in [0,1]代表折扣率；x:y表示满x减y；“fixed”表示低价限时优惠；
5  Date_received    领取优惠券日期
6       Date_pay    消费日期：如果Date=null & Coupon_id != null，该记录表示领取优惠券但没有使用；如果Date!=null & Coupon_id = null，则表示普通消费日期；如果Date!=null & Coupon_id != null，则表示用优惠券消费日期；

Table 3：## charles_prediction_stage2 ## 用户O2O优惠券使用预测样本
           field    description
0        User_id    用户ID
1    Merchant_id    商户ID
2      Coupon_id    优惠券ID
3  Discount_rate    优惠率：x \in [0,1]代表折扣率；x:y表示满x减y.
4       Distance    user经常活动的地点离该merchant的最近门店距离是x*500米（如果是连锁店，则取最近的一家门店），x\in[0,10]；null表示无此信息，0表示低于500米，10表示大于5公里；
5  Date_received    领取优惠券日期

Table 4：## submission ## 提交文件字段，其中user_id,coupon_id和date_received均来自Table 3,而Probability为预测值
           field    description
0        User_id    用户ID
1      Coupon_id    优惠券ID
2  Date_received    领取优惠券日期
3    Probability    15天内用券概率，由参赛选手给出
*/