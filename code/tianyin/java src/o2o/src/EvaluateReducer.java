import java.io.IOException;
import java.util.*;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;

public class EvaluateReducer extends ReducerBase {

    private Record result;

    @Override
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        List<Double> listp = new ArrayList<Double>();
        List<Double> listn = new ArrayList<Double>();
        while (values.hasNext()) {
            Record val = values.next();
            int label = val.getBigint("label").intValue();
            if (label == 1)
                listp.add(val.getDouble("prediction_score"));
            else
                listn.add(val.getDouble("prediction_score"));
        }
        if (listn.size() == 0 || listp.size() == 0) {
            return;
        }
        double score = 0;
        for (int i = 0; i < listp.size(); i++)
            for (int j = 0; j < listn.size(); j++) {
                if (listp.get(i) > listn.get(j)) score += 1;
                else if (listp.get(i) == listn.get(j)) score += 0.5;
            }
        double auc = score / listn.size() / listp.size();
//        if (auc < 0.5) auc = 1 - auc;
        result.setString("coupon_id", key.getString("coupon_id"));
        result.setDouble("auc", auc);
        context.write(result);
    }

    @Override
    public void cleanup(TaskContext context) throws IOException {

    }

}