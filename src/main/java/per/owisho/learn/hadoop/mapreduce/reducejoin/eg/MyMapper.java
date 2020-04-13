package per.owisho.learn.hadoop.mapreduce.reducejoin.eg;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, LongWritable, Emplyee> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] arr = val.split("\t");
        System.out.println("arr.length=" + arr.length + "arr[0]=" + arr[0]);
        if (arr.length <= 3) {//dept
            Emplyee e = new Emplyee();
            e.setDeptNo(arr[0]);
            e.setDeptName(arr[1]);
            e.setFlag(1);
            context.write(new LongWritable(Long.valueOf(e.getDeptNo())), e);
        } else {//emp
            Emplyee e = new Emplyee();
            e.setEmpNo(arr[0]);
            e.setEmpName(arr[1]);
            e.setDeptNo(arr[7]);
            e.setFlag(0);
            context.write(new LongWritable(Long.valueOf(e.getDeptNo())), e);
        }
    }
}
