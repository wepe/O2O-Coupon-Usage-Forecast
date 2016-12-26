import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class UserCouponFeatureReducer extends ReducerBase {

    private Record result;
    private static DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static Calendar calendar = Calendar.getInstance();

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
            String line = val.getString("merchant_id") + "," + val.getString("discount_rate") + "," + val.getString("distance") + "," + val.getString("date_received") + "," + val.getString("date_pay");
            if (data[index] == null) data[index] = new ArrayList<String>();
            data[index].add(line);
        }
        int days[] = new int[]{30, 60};
        int week[] = new int[7];
        String splits[];
        Util util = Util.getInstance();
        for (int i = 0; i <= 4; i++) {
            result.setString("ucf_user_id", key.getString("user_id"));
            result.setString("ucf_coupon_id", key.getString("coupon_id"));
            result.setBigint("ucf_which", (long) i);
            int feature_end = 181 - i * Util.WINDOW_LENGTH;
            for (int j = 0; j < days.length; j++) {
                int rec_j = 0;
                int rec_use_j = 0;
                int rec_use_in_15 = 0;
                double use_days = 0;
                double use_days_sum = 0;
                for (int k = feature_end; k > feature_end - days[j] && k > 0; k--) {
                    if (data[k] == null) continue;
                    for (String record : data[k]) {
                        splits = record.split(",");
                        rec_j++;
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
                        }
                    }
                }
//                    统计型
                result.setBigint("ucf_rec_" + days[j], (long) rec_j);
                result.setBigint("ucf_rec_use_" + days[j], (long) rec_use_j);
                result.setBigint("ucf_rec_use_in_15_" + days[j], (long) rec_use_in_15);
//                    比值型
                result.setDouble("ucf_rate_use_rec_" + days[j], util.getRate(rec_use_j, rec_j));
                result.setDouble("ucf_rate_use_in_15_rec_" + days[j], util.getRate(rec_use_in_15, rec_j));
                result.setDouble("ucf_rate_use_in_15_use_" + days[j], util.getRate(rec_use_in_15, rec_use_j));
                result.setDouble("ucf_use_day_avg_" + days[j], util.getRate(use_days_sum, use_days));
            }
            int label_end = 181 - (i - 1) * Util.WINDOW_LENGTH;
            int rec_j = 0;
            for (int k = label_end; k > label_end - 31 && k > 0; k--) {
                if (data[k] == null) continue;
                rec_j += data[k].size();
            }
//                    统计型
            result.setBigint("ucf_rec_future", (long) rec_j);
//            生成label
            for (int k = label_end; k > label_end - 31 && k > 0; k--) {
                if (data[k] == null) continue;
                int count = 0;
                int ndays = 1;
                for (int j = k - 1; j > label_end - 31 && j > 0; j--)
                    if (data[j] != null) count += data[j].size();
                    else if (count == 0) ndays++;
                result.setBigint("ucf_count_rec_bef", (long) count);
                result.setBigint("ucf_days_rec_bef", (long) ndays);
                count = 0;
                ndays = 1;
                for (int j = k + 1; j < k + 15 && j <= label_end; j++)
                    if (data[j] != null) count += data[j].size();
                    else if (count == 0) ndays++;
                result.setBigint("ucf_count_rec_after", (long) count);
                result.setBigint("ucf_days_rec_after", (long) ndays);
                for (int j = 0; j < data[k].size(); j++) {
                    splits = data[k].get(j).split(",");
                    result.setString("ucf_merchant_id", splits[0]);
                    if (splits[1].contains(":")) {
                        double price = Double.parseDouble(splits[1].split(":")[0]);
                        double reducePrice = Double.parseDouble(splits[1].split(":")[1]);
                        result.setDouble("ucf_man", Double.parseDouble(splits[1].split(":")[0]));
                        result.setDouble("ucf_disc_rate", (price - reducePrice) / price);
                    } else {
                        result.setDouble("ucf_man", 0d);
                        result.setDouble("ucf_disc_rate", Double.parseDouble(splits[1]));
                    }
                    if (splits[2].compareTo("null") == 0) result.setDouble("ucf_distance", -1d);
                    else result.setDouble("ucf_distance", Double.parseDouble(splits[2]));
                    try {
                        if (splits[4].compareTo("null") != 0)
//                                使用优惠券
                        {
                            result.setDouble("label_reg", 8 / (1 + Math.log(util.getDeltaDate(splits[3], splits[4]) + 1)));
                            if (util.getDeltaDate(splits[3], splits[4]) <= 15)
                                result.setBigint("label", 1L);
                            else
                                result.setBigint("label", 0L);
                        } else {
                            result.setBigint("label", 0L);
                            result.setDouble("label_reg", 0d);
                        }
                    } catch (Exception e) {
                        result.setBigint("label", -1L);
                    }
                    result.setString("ucf_date_received", splits[3]);
                    result.setString("ucf_date_pay", splits[4]);
                    Arrays.fill(week, 0);
                    try {
                        Date date = df.parse(splits[3]);
                        calendar.setTime(date);
                        int w = calendar.get(Calendar.DAY_OF_WEEK) - 1;
                        week[w] = 1;
                        for (int ww = 0; ww < 7; ww++)
                            result.setBigint("ucf_week_" + ww, (long) week[ww]);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    context.write(result);
                }
            }
        }

    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

    public static void main(String[] args) {
        int days[] = new int[]{30, 60};
        StringBuffer sb = new StringBuffer();
        sb.append("ucf_user_id string,\r\n");
        sb.append("ucf_merchant_id string,\r\n");
        sb.append("ucf_coupon_id string,\r\n");
        sb.append("ucf_date_received string,\r\n");
        sb.append("ucf_date_pay string,\r\n");
        sb.append("ucf_which bigint,\r\n");
        for (int j = 0; j < days.length; j++) {
            sb.append("ucf_rec_" + days[j] + " bigint,\r\n");
            sb.append("ucf_rec_use_" + days[j] + " bigint,\r\n");
            sb.append("ucf_rec_use_in_15_" + days[j] + " bigint,\r\n");
            sb.append("ucf_rate_use_rec_" + days[j] + " double,\r\n");
            sb.append("ucf_rate_use_in_15_rec_" + days[j] + " double,\r\n");
            sb.append("ucf_rate_use_in_15_use_" + days[j] + " double,\r\n");
            sb.append("ucf_use_day_avg_" + days[j] + " double,\r\n");
        }
        for (int i = 0; i < 7; i++)
            sb.append("ucf_week_" + i + " bigint,\r\n");
        sb.append("ucf_count_rec_bef bigint,\r\n");
        sb.append("ucf_count_rec_after bigint,\r\n");
        sb.append("ucf_days_rec_bef bigint,\r\n");
        sb.append("ucf_days_rec_after bigint,\r\n");
        sb.append("ucf_man double,\r\n");
        sb.append("ucf_disc_rate double,\r\n");
        sb.append("ucf_distance double,\r\n");
        sb.append("ucf_rec_future bigint,\r\n");
        sb.append("label bigint\r\n");
        System.out.printf(sb.toString());
    }
}