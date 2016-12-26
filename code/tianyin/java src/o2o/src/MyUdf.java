import com.aliyun.odps.udf.UDF;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyUdf extends UDF {
    private static DateFormat df = new SimpleDateFormat("yyyyMMdd");

    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Long evaluate(String date_received, String date_pay, String date_start) {
        long index = 0;
        try {
            Date date1 = df.parse(date_start);
            if (date_received.compareTo("null") != 0) {
                Date date2 = df.parse(date_received);
                long diff = date2.getTime() - date1.getTime();
                index = (int) (diff / (1000 * 60 * 60 * 24));
            } else {
                Date date2 = df.parse(date_pay);
                long diff = date2.getTime() - date1.getTime();
                index = (int) (diff / (1000 * 60 * 60 * 24));
            }
        } catch (Exception e) {

        }
        return index;
    }
}