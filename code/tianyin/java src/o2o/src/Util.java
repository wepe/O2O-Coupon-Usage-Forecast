import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by chenjing on 2016/11/18.
 */
public class Util {
    private static Util ourInstance = new Util();
    private static DateFormat df = new SimpleDateFormat("yyyyMMdd");
    public static final int WINDOW_LENGTH = 31;

    public static Util getInstance() {
        return ourInstance;
    }

    private Util() {
    }

    public double getRate(double d1, double d2) {
        if (d2 == 0) return -1;
        return d1 / d2;
    }


    public int getDeltaDate(String start, String end) throws ParseException {
        Date date1 = df.parse(start);
        Date date2 = df.parse(end);
        long diff = date2.getTime() - date1.getTime();
        int days = (int) (diff / (1000 * 60 * 60 * 24));
        return days;
    }

}
