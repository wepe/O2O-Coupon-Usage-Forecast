import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class UserMerchantFeatureReducer extends ReducerBase {
    private Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        List<String>[] data = new List[213];
        while (values.hasNext()) {
            Record val = values.next();
            int index = val.getBigint("day_index").intValue();
            String line = val.getString("coupon_id") + "," + val.getString("discount_rate") + "," + val.getString("distance") + "," + val.getString("date_received") + "," + val.getString("date_pay");
            if (data[index] == null) data[index] = new ArrayList<String>();
            data[index].add(line);
        }
        int days[] = new int[]{30, 60};
        String splits[];
        Util util = Util.getInstance();
        for (int i = 0; i <= 4; i++) {
            result.setString("umf_user_id", key.getString("user_id"));
            result.setString("umf_merchant_id", key.getString("merchant_id"));
            result.setBigint("umf_which", (long) i);
            int feature_end = 181 - i * Util.WINDOW_LENGTH;
            for (int j = 0; j < days.length; j++) {
                int rec_j = 0;
                int rec_use_j = 0;
                int rec_use_in_15 = 0;
                int consume_j = 0;
                double use_days = 0;
                double use_days_sum = 0;
                Set<String> rec_set = new HashSet<String>();
                Set<String> rec_use_set = new HashSet<String>();
                for (int k = feature_end; k > feature_end - days[j] && k > 0; k--) {
                    if (data[k] == null) continue;
                    for (String record : data[k]) {
                        splits = record.split(",");
                        if (splits[0].compareTo("null") == 0) {
//                                普通消费
                            consume_j++;
                        } else {
                            rec_j++;
                            rec_set.add(splits[0]);
                            if (splits[4].compareTo("null") == 0) {
//                                优惠券未使用
                            } else {
//                                    使用优惠券
                                try {
                                    use_days_sum += util.getDeltaDate(splits[3], splits[4]);
                                    use_days++;
                                    if (util.getDeltaDate(splits[3], splits[4]) <= 15) {
                                        rec_use_in_15++;
                                    }
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                rec_use_j++;
                                consume_j++;
                                rec_use_set.add(splits[0]);
                            }
                        }
                    }
                }
//                    统计型
                result.setBigint("umf_rec_" + days[j], (long) rec_j);
                result.setBigint("umf_rec_use_" + days[j], (long) rec_use_j);
                result.setBigint("umf_rec_use_in_15_" + days[j], (long) rec_use_in_15);
                result.setBigint("umf_consume_" + days[j], (long) consume_j);
                result.setBigint("umf_rec_use_type_" + days[j], (long) rec_use_set.size());
                result.setBigint("umf_rec_type_" + days[j], (long) rec_set.size());
//                    比值型
                result.setDouble("umf_rate_use_rec_" + days[j], util.getRate(rec_use_j, rec_j));
                result.setDouble("umf_rate_use_in_15_rec_" + days[j], util.getRate(rec_use_in_15, rec_j));
                result.setDouble("umf_rate_use_in_15_use_" + days[j], util.getRate(rec_use_in_15, rec_use_j));
                result.setDouble("umf_use_day_avg_" + days[j], util.getRate(use_days_sum, use_days));
                result.setDouble("umf_rate_rec_consume_" + days[j], util.getRate(rec_set.size(), consume_j));
                result.setDouble("umf_rate_rec_use_all_" + days[j], util.getRate(rec_use_set.size(), rec_set.size()));
                result.setDouble("umf_rate_rec_use_type_all_" + days[j], util.getRate(rec_use_set.size(), rec_use_j));
            }
            int label_end = 181 - (i - 1) * Util.WINDOW_LENGTH;
            int rec_j = 0;
            Set<String> couponSet = new HashSet<String>();
            for (int k = label_end; k > label_end - 31 && k > 0; k--) {
                if (data[k] == null) continue;
                for (String record : data[k]) {
                    splits = record.split(",");
                    if (splits[0].compareTo("null") == 0) continue;
                    couponSet.add(splits[0]);
                    rec_j++;
                }
            }
//                    统计型
            result.setBigint("umf_rec_future", (long) rec_j);
            result.setBigint("umf_rectype_future", (long) couponSet.size());
            context.write(result);

        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    public static void main(String[] args) {
        int days[] = new int[]{30, 60};
        StringBuffer sb = new StringBuffer();
        sb.append("umf_user_id string,\r\n");
        sb.append("umf_merchant_id string,\r\n");
        sb.append("umf_which bigint,\r\n");
        for (int j = 0; j < days.length; j++) {
            sb.append("umf_rec_" + days[j] + " bigint,\r\n");
            sb.append("umf_rec_use_" + days[j] + " bigint,\r\n");
            sb.append("umf_consume_" + days[j] + " bigint,\r\n");
            sb.append("umf_rec_use_type_" + days[j] + " bigint,\r\n");
            sb.append("umf_rec_type_" + days[j] + " bigint,\r\n");
            sb.append("umf_rec_use_in_15_" + days[j] + " bigint,\r\n");
            sb.append("umf_rate_use_rec_" + days[j] + " double,\r\n");
            sb.append("umf_rate_use_in_15_rec_" + days[j] + " double,\r\n");
            sb.append("umf_rate_use_in_15_use_" + days[j] + " double,\r\n");
            sb.append("umf_use_day_avg_" + days[j] + " double,\r\n");
            sb.append("umf_rate_rec_consume_" + days[j] + " double,\r\n");
            sb.append("umf_rate_rec_use_all_" + days[j] + " double,\r\n");
            sb.append("umf_rate_rec_use_type_all_" + days[j] + " double,\r\n");
        }
        sb.append("umf_rec_future bigint,\r\n");
        sb.append("umf_rectype_future bigint,\r\n");
        System.out.printf(sb.toString());
    }
}