#! /usr/bin/env python2.7
# -*- coding: utf-8 -*-
# File: data_view.py
# Date: 2016-10-14
# Author: Chaos <xinchaoxt@gmail.com>
import pandas as pd
from collections import Counter
from config import *
import matplotlib.pyplot as plt
from time import time


class DataView:
    def __init__(self, file_path=offline_train_file_path):
        self.file_path = file_path
        df = pd.read_csv(self.file_path)
        self.data = df
        self.fields = df.columns.tolist()

    @property
    def user_list(self):
        return self.data[user_label].tolist()

    @property
    def user_set(self):
        return set(self.data[user_label].tolist())

    @property
    def merchant_list(self):
        return self.data[merchant_label].tolist()

    @property
    def merchant_set(self):
        return set(self.data[merchant_label].tolist())

    @property
    def coupon_list(self):
        return self.data[coupon_label].tolist()

    @property
    def coupon_set(self):
        return set(self.data[coupon_label].tolist())

    @property
    def coupon_consumption_list(self):
        fullcut_list = self.data[discount_label][self.data[discount_label].str.contains(':')].tolist()
        return [x.split(':')[0] for x in fullcut_list]

    @property
    def continuous_users_diff(self):
        user = '-1'
        cnt = 0
        for idx in xrange(self.data.shape[0]):
            if self.data.iloc[idx][user_label] != user:
                cnt += 1
                user = self.data.iloc[idx][user_label]
        return cnt

    @property
    def received_data_distribution(self):
        return dict(self.data.groupby(date_received_label)[date_received_label].count())

    def filter_by_received_time(self, start_time, end_time):
        return self.data[self.data[date_received_label].map(lambda x: True if start_time <= str(x) <= end_time else False)]


def get_time_diff(date_received, date_consumed):
    # 计算时间差
    month_diff = int(date_consumed[-4:-2]) - int(date_received[-4:-2])
    if month_diff == 0:
        return int(date_consumed[-2:]) - int(date_received[-2:])
    else:
        return int(date_consumed[-2:]) - int(date_received[-2:]) + month_diff * 30


if __name__ == '__main__':
    train_offline_data = DataView(offline_train_file_path)
    test_offline_data = DataView(offline_test_file_path)
    train_online_data = DataView(online_train_file_path)

    # user view
    train_offline_user_list, train_offline_user_set = train_offline_data.user_list, train_offline_data.user_set
    test_offline_user_list, test_offline_user_set = test_offline_data.user_list, test_offline_data.user_set
    train_online_user_list, train_online_user_set = train_online_data.user_list, train_online_data.user_set

    print len(train_offline_user_list), len(train_offline_user_set)
    print len(test_offline_user_list), len(test_offline_user_set)
    print len(train_online_user_list), len(train_online_user_set)
    train_user_set = train_online_user_set | train_offline_user_set
    train_active_user_set = train_online_user_set & train_offline_user_set
    print len(train_active_user_set), len(train_user_set)

    print len(test_offline_user_set - train_user_set)
    print len(test_offline_user_set - train_active_user_set)
    print len(test_offline_user_set - train_offline_user_set)
    print test_offline_user_set - train_offline_user_set
    # 1754884 539438
    # 113640 76309
    # 11429826 762858
    # 267448 1034848
    # 1
    # 33155
    # 2
    # set([2495873, 1286474])
    # 线下用户少于线上
    # 训练集所有用户中,约1/4的用户参与了线上及线下的活动,约1/4仅参与了线下,约1/2仅参与了线上
    # 测试集中用户几乎被训练集中参与过线下活动的用户全部覆盖,有一小半是活跃用户<线上线下都参加>

    # merchant view
    train_offline_merchant_list, train_offline_merchant_set = train_offline_data.merchant_list, train_offline_data.merchant_set
    test_offline_merchant_list, test_offline_merchant_set = test_offline_data.merchant_list, test_offline_data.merchant_set
    train_online_merchant_list, train_online_merchant_set = train_online_data.merchant_list, train_online_data.merchant_set

    print len(train_offline_merchant_list), len(train_offline_merchant_set)
    print len(test_offline_merchant_list), len(test_offline_merchant_set)
    print len(train_online_merchant_list), len(train_online_merchant_set)
    train_merchant_set = train_online_merchant_set | train_offline_merchant_set
    print len(train_online_merchant_set & train_offline_merchant_set), len(train_merchant_set)

    print len(test_offline_merchant_set - train_merchant_set)
    print len(test_offline_merchant_set - train_offline_merchant_set)
    print test_offline_merchant_set - train_offline_merchant_set
    # 1754884 8415
    # 113640 1559
    # 11429826 7999
    # 0 16414
    # 1
    # 1
    # set([5920])
    # 线下商家略多于线上,但线上的活动记录数据更多
    # 商家数远小于用户数<这不是显然么并没有什么卵用囧>
    # 线上与线下的商家完全没有交集<可能是同一商家在线上与线下的ID不同,应该不会完全无交集,我猜:)>
    # 测试集中商家几乎被训练集中线下商家全部覆盖

    # coupon view
    train_offline_coupon_list, train_offline_coupon_set = train_offline_data.coupon_list, train_offline_data.coupon_set
    test_offline_coupon_list, test_offline_coupon_set = test_offline_data.coupon_list, test_offline_data.coupon_set
    train_online_coupon_list, train_online_coupon_set = train_online_data.coupon_list, train_online_data.coupon_set

    print len(train_offline_coupon_list), len(train_offline_coupon_set)
    print len(test_offline_coupon_list), len(test_offline_coupon_set)
    print len(train_online_coupon_list), len(train_online_coupon_set)
    train_coupon_set = train_online_coupon_set | train_offline_coupon_set
    print len(train_online_coupon_set & train_offline_coupon_set), len(train_coupon_set)
    print train_online_coupon_set & train_offline_coupon_set

    print len(test_offline_coupon_set - train_coupon_set)
    print len(test_offline_coupon_set - train_offline_coupon_set)
    # 1754884 9739
    # 113640 2050
    # 11429826 27748
    # 1 37486
    # set(['null'])
    # 2050
    # 2050
    # 线上优惠券使用频繁
    # 线上线下优惠券无交集<null不代表共有优惠券>
    # 测试集中全部是全新的优惠券<ID肯定是不同的,所以对于优惠券的特征描述应当是其商家、折扣率等,与ID无关。>

    # coupon consumption
    print len(train_offline_data.coupon_consumption_list), Counter(train_offline_data.coupon_consumption_list).items()
    print len(test_offline_data.coupon_consumption_list), Counter(test_offline_data.coupon_consumption_list).items()
    print len(train_online_data.coupon_consumption_list), Counter(train_online_data.coupon_consumption_list).items()
    # 1020010 [('150', 23729), ('200', 146602), ('20', 143232), ('10', 43767), ('300', 29264), ('30', 301082), ('50', 87397), ('5', 2526), ('100', 242411)]
    # 111074 [('150', 1077), ('20', 18413), ('200', 5925), ('300', 165), ('30', 63773), ('50', 10605), ('5', 171), ('100', 6840), ('500', 1), ('10', 4104)]
    # 740811 [('150', 78269), ('200', 66736), ('20', 58383), ('10', 47951), ('300', 60255), ('30', 73409), ('50', 150019), ('5', 42828), ('100', 95698), ('800', 14000), ('500', 43643), ('1000', 9620)]
    # 测试集中满减优惠券所占比重很高
    figure, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 8), facecolor='w', edgecolor='k')
    ax1.hist([int(x) for x in train_offline_data.coupon_consumption_list])
    ax2.hist([int(x) for x in test_offline_data.coupon_consumption_list])
    ax3.hist([int(x) for x in train_online_data.coupon_consumption_list])
    figure.suptitle('coupon consumption statistics', fontsize=16)
    ax1.set_title('train offline data')
    ax2.set_title('test offline data')
    ax3.set_title('train online data')
    plt.savefig('coupon_consumption')
    plt.show()

    data_sets = [train_offline_data.data, train_online_data.data, test_offline_data]
    for data in data_sets:
        print sum(pd.Series(list(map(lambda x, y: True if x == 'null' and y != 'null' else False, data[coupon_label], data[date_received_label]))))
        print sum(pd.Series(list(map(lambda x, y: True if x == 'null' and y != 'null' else False, data[coupon_label], data[discount_label]))))
    # 0 0 0 0 0 0
    # 所有数据中,当Coupon_id == null时, Discount_rate和Date_received一定为null

    print train_offline_data.continuous_users_diff, len(train_offline_data.user_set)
    print train_online_data.continuous_users_diff, len(train_online_data.user_set)
    print test_offline_data.continuous_users_diff, len(test_offline_data.user_set)
    # 几乎所有数据都已经是按照user id做过group的

    train_dates = train_offline_data.received_data_distribution.items()
    train_dates.sort(key=lambda x: x[0])
    print train_dates
    test_dates = test_offline_data.received_data_distribution.items()
    test_dates.sort(key=lambda x: x[0])
    print test_dates
    # [('20160101', 554), ('20160102', 542), ('20160103', 536), ('20160104', 577), ('20160105', 691), ('20160106', 808), ('20160107', 746), ('20160108', 970), ('20160109', 1003), ('20160110', 869), ('20160111', 712), ('20160112', 773), ('20160113', 1459), ('20160114', 1520), ('20160115', 1665), ('20160116', 1517), ('20160117', 1546), ('20160118', 1177), ('20160119', 1283), ('20160120', 1166), ('20160121', 1194), ('20160122', 1236), ('20160123', 24045), ('20160124', 39481), ('20160125', 65904), ('20160126', 26027), ('20160127', 18893), ('20160128', 34334), ('20160129', 71658), ('20160130', 33226), ('20160131', 35427), ('20160201', 16371), ('20160202', 11253), ('20160203', 17494), ('20160204', 14450), ('20160205', 4659), ('20160206', 3115), ('20160207', 33319), ('20160208', 1967), ('20160209', 1173), ('20160210', 3365), ('20160211', 3093), ('20160212', 3730), ('20160213', 4007), ('20160214', 4013), ('20160215', 701), ('20160216', 685), ('20160217', 618), ('20160218', 543), ('20160219', 669), ('20160220', 848), ('20160221', 670), ('20160222', 667), ('20160223', 554), ('20160224', 633), ('20160225', 780), ('20160226', 902), ('20160227', 1073), ('20160228', 1216), ('20160229', 865), ('20160301', 902), ('20160302', 578), ('20160303', 566), ('20160304', 768), ('20160305', 933), ('20160306', 1000), ('20160307', 609), ('20160308', 786), ('20160309', 694), ('20160310', 956), ('20160311', 786), ('20160312', 1006), ('20160313', 935), ('20160314', 619), ('20160315', 788), ('20160316', 715), ('20160317', 700), ('20160318', 840), ('20160319', 1026), ('20160320', 977), ('20160321', 9923), ('20160322', 9826), ('20160323', 9754), ('20160324', 9283), ('20160325', 11265), ('20160326', 13719), ('20160327', 13341), ('20160328', 2289), ('20160329', 2754), ('20160330', 2715), ('20160331', 2850), ('20160401', 3883), ('20160402', 3380), ('20160403', 3210), ('20160404', 3450), ('20160405', 3656), ('20160406', 3245), ('20160407', 4848), ('20160408', 3766), ('20160409', 3557), ('20160410', 3861), ('20160411', 3620), ('20160412', 4366), ('20160413', 3952), ('20160414', 4724), ('20160415', 5360), ('20160416', 5400), ('20160417', 4348), ('20160418', 4902), ('20160419', 4424), ('20160420', 4651), ('20160421', 5035), ('20160422', 7381), ('20160423', 4730), ('20160424', 6490), ('20160425', 4965), ('20160426', 4579), ('20160427', 5565), ('20160428', 6104), ('20160429', 5634), ('20160430', 5008), ('20160501', 3121), ('20160502', 3276), ('20160503', 1885), ('20160504', 2154), ('20160505', 3371), ('20160506', 2667), ('20160507', 2731), ('20160508', 3746), ('20160509', 3919), ('20160510', 2887), ('20160511', 4215), ('20160512', 3710), ('20160513', 4788), ('20160514', 5397), ('20160515', 5860), ('20160516', 5092), ('20160517', 7797), ('20160518', 9440), ('20160519', 10215), ('20160520', 14796), ('20160521', 19859), ('20160522', 13299), ('20160523', 11008), ('20160524', 10998), ('20160525', 13576), ('20160526', 8285), ('20160527', 6721), ('20160528', 13276), ('20160529', 5720), ('20160530', 5076), ('20160531', 6463), ('20160601', 7443), ('20160602', 9149), ('20160603', 7664), ('20160604', 5602), ('20160605', 5624), ('20160606', 6519), ('20160607', 5487), ('20160608', 6146), ('20160609', 6315), ('20160610', 5709), ('20160611', 5211), ('20160612', 4005), ('20160613', 7861), ('20160614', 4755), ('20160615', 3475), ('null', 701602)]
    # [(20160701, 3808), (20160702, 3831), (20160703, 5491), (20160704, 5121), (20160705, 4693), (20160706, 4574), (20160707, 2234), (20160708, 1416), (20160709, 1616), (20160710, 4100), (20160711, 4521), (20160712, 4349), (20160713, 4082), (20160714, 3655), (20160715, 3290), (20160716, 3002), (20160717, 3213), (20160718, 3284), (20160719, 3128), (20160720, 2984), (20160721, 3044), (20160722, 3613), (20160723, 4853), (20160724, 4507), (20160725, 4746), (20160726, 4701), (20160727, 4156), (20160728, 3906), (20160729, 3370), (20160730, 2258), (20160731, 2094)]

    start_time = time()
    df = train_offline_data.data
    frame = pd.Series(list(map(lambda x, y, z: 1 if x != 'null' and y != 'null' and get_time_diff(z, y) <= 15 else 0, df[coupon_label], df[date_consumed_label], df[date_received_label])))
    frame.name = 'Label'
    df = df.join(frame)
    print time() - start_time
    grouped = df.groupby([user_label, coupon_label], as_index=False, sort=False)
    print time() - start_time
    grouped = grouped.apply(lambda x: x.sort_values(by=date_received_label).reset_index())
    print time() - start_time
    df = grouped.reset_index().drop(['index', 'level_0'], axis=1).rename(columns={'level_1': 'received_order'})
    print time() - start_time
    df.to_csv('./data/ccf_data_revised/train_offline_add_labels.csv', index=False)
    print time() - start_time
    # 1.30519890785
    # 1.37534999847
    # 3371.15719891
    # 3372.98513198
    # 3378.25308204
