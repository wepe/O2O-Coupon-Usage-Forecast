import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

public class EvaluateMapper extends MapperBase {

    private Record key;
    private Record value;

    @Override
    public void setup(TaskContext context) throws IOException {
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        key.setString("coupon_id", record.getString("coupon_id"));
        value.setBigint("label", record.getBigint("label"));
        value.setDouble("prediction_score", record.getDouble("prediction_score"));
        context.write(key, value);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}