import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class MerchantFeatureReducer extends ReducerBase {

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
            String line = val.getString("user_id") + "," + val.getString("coupon_id") + "," + val.getString("discount_rate") + "," + val.getString("distance") + "," + val.getString("date_received") + "," + val.getString("date_pay");
            if (data[index] == null) data[index] = new ArrayList<String>();
            data[index].add(line);
        }
        int days[] = new int[]{30, 60};
        String splits[];
        Util util = Util.getInstance();
        for (int i = 0; i <= 4; i++) {
            result.setString("mf_merchant_id", key.getString("merchant_id"));
            result.setBigint("mf_which", (long) i);
            int end = 181 - i * Util.WINDOW_LENGTH;
            for (int j = 0; j < days.length; j++) {
                int rec_j = 0;
                int rec_use_j = 0;
                int consume_j = 0;
                int rec_use_in_15 = 0;
                double use_days = 0;
                double use_days_sum = 0;
                double n_use_distance = 0;
                double distances = 0;
                Set<String> consume_merchant_set = new HashSet<String>();
                Set<String> rec_use_merchant_set = new HashSet<String>();
                for (int k = end; k > end - days[j] && k > 0; k--) {
                    if (data[k] == null) continue;
                    for (String record : data[k]) {
                        splits = record.split(",");
                        if (splits[1].compareTo("null") == 0) {
//                                普通消费
                            consume_j++;
                            consume_merchant_set.add(splits[0]);
                        } else {
                            rec_j++;
                            if (splits[5].compareTo("null") == 0) {
//                                优惠券未使用

                            } else {
//                                    使用优惠券
                                if (splits[3].compareTo("null") != 0) {
                                    n_use_distance++;
                                    distances += Double.parseDouble(splits[3]);
                                }
                                try {
                                    use_days_sum += util.getDeltaDate(splits[4], splits[5]);
                                    use_days++;
                                    if (util.getDeltaDate(splits[4], splits[5]) <= 15) {
                                        rec_use_in_15++;
                                    }
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                                consume_j++;
                                rec_use_j++;
                                consume_merchant_set.add(splits[0]);
                                rec_use_merchant_set.add(splits[0]);
                            }
                        }
                    }
                }
//                    统计型
                result.setBigint("mf_send_" + days[j], (long) rec_j);
                result.setBigint("mf_send_use_" + days[j], (long) rec_use_j);
                result.setBigint("mf_consumed_" + days[j], (long) consume_j);
                result.setBigint("mf_consume_user_" + days[j], (long) consume_merchant_set.size());
                result.setBigint("mf_rec_use_in_15_" + days[j], (long) rec_use_in_15);
//                    比值型
                result.setDouble("mf_rate_use_in_15_rec_" + days[j], util.getRate(rec_use_in_15, rec_j));
                result.setDouble("mf_rate_use_in_15_use_" + days[j], util.getRate(rec_use_in_15, rec_use_j));
                result.setDouble("mf_use_day_avg_" + days[j], util.getRate(use_days_sum, use_days));
                result.setDouble("mf_use_distance_avg_" + days[j], util.getRate(distances, n_use_distance));
                result.setDouble("mf_rate_use_send_" + days[j], util.getRate(rec_use_j, rec_j));
                result.setDouble("mf_rate_consume_user_consume_" + days[j], util.getRate(consume_merchant_set.size(), consume_j));
                result.setDouble("mf_rate_send_use_consume_user_" + days[j], util.getRate(rec_use_merchant_set.size(), consume_merchant_set.size()));
                result.setDouble("mf_rate_send_use_user_" + days[j], util.getRate(rec_use_j, rec_use_merchant_set.size()));
            }
            int label_end = 181 - (i - 1) * Util.WINDOW_LENGTH;
            int rec_j = 0;
            Set<String> couponSet = new HashSet<String>();
            Set<String> userSet = new HashSet<String>();
            for (int k = label_end; k > label_end - 31 && k > 0; k--) {
                if (data[k] == null) continue;
                for (String record : data[k]) {
                    splits = record.split(",");
                    if (splits[1].compareTo("null") == 0) continue;
                    couponSet.add(splits[1]);
                    userSet.add(splits[0]);
                    rec_j++;
                }
            }
//                    统计型
            result.setBigint("mf_rec_future", (long) rec_j);
            result.setBigint("mf_rec_cptype_future", (long) couponSet.size());
            result.setBigint("mf_rec_utype_future", (long) userSet.size());
            context.write(result);
        }
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    public static void main(String[] args) {
        int days[] = new int[]{30, 60};
        StringBuffer sb = new StringBuffer();
        sb.append("mf_merchant_id string,\r\n");
        sb.append("mf_which bigint,\r\n");
        for (int j = 0; j < days.length; j++) {
            sb.append("mf_send_" + days[j] + " bigint,\r\n");
            sb.append("mf_send_use_" + days[j] + " bigint,\r\n");
            sb.append("mf_consumed_" + days[j] + " bigint,\r\n");
            sb.append("mf_consume_user_" + days[j] + " bigint,\r\n");
            sb.append("mf_rec_use_in_15_" + days[j] + " bigint,\r\n");
            sb.append("mf_rate_use_in_15_rec_" + days[j] + " double,\r\n");
            sb.append("mf_rate_use_in_15_use_" + days[j] + " double,\r\n");
            sb.append("mf_use_day_avg_" + days[j] + " double,\r\n");
            sb.append("mf_use_distance_avg_" + days[j] + " double,\r\n");
            sb.append("mf_rate_use_send_" + days[j] + " double,\r\n");
            sb.append("mf_rate_consume_user_consume_" + days[j] + " double,\r\n");
            sb.append("mf_rate_send_use_consume_user_" + days[j] + " double,\r\n");
            sb.append("mf_rate_send_use_user_" + days[j] + " double,\r\n");
        }
        sb.append("mf_rec_future bigint,\r\n");
        sb.append("mf_rec_cptype_future bigint,\r\n");
        sb.append("mf_rec_utype_future bigint,\r\n");
        System.out.printf(sb.toString());
    }
}