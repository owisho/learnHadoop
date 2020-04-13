package per.owisho.learn.hadoop.mapreduce.reducejoin.eg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyReducer extends Reducer<LongWritable, Emplyee, NullWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Emplyee> values, Context context) throws IOException, InterruptedException {
        Emplyee dept = null;
        List<Emplyee> list = new ArrayList<>();
        for (Emplyee tmp : values) {
            if (tmp.getFlag() == 0) {
                Emplyee emplyee = new Emplyee(tmp);
                list.add(emplyee);
            } else {
                dept = new Emplyee(tmp);
            }
        }
        if (dept != null) {
            for (Emplyee emp : list) {
                emp.setDeptName(dept.getDeptName());
                context.write(NullWritable.get(), new Text(emp.toString()));
            }
        }
    }
}
