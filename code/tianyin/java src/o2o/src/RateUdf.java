import com.aliyun.odps.udf.UDF;

public class RateUdf extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public Double evaluate(Double fenzi, Double fenmu) {
        return Util.getInstance().getRate(fenzi, fenmu);
    }
}