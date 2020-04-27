package per.owisho.learn.hadoop.test3;

import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

public class MobileFlowApp {

    static Splitter sp = Splitter.on("\t").trimResults();

    static class MyMapper extends Mapper<IntWritable, Text, Text, MobileFlow> {
        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            //TODO 搞懂splitter源码
            Iterable<String> itr = sp.split(content);

            String[] arr = content.split("\t");
            String msisdn = arr[1];
            String upPackNumStr = arr[6];
            String downPackNumStr = arr[7];
            String upPayLoadStr = arr[8];
            String downPayLoadStr = arr[9];

            Long upPackNum = Long.parseLong(upPackNumStr);
            Long downPackNum = Long.parseLong(downPackNumStr);
            Long upPayLoad = Long.parseLong(upPayLoadStr);
            Long downPayLoad = Long.parseLong(downPayLoadStr);
            MobileFlow flow = new MobileFlow(msisdn, upPackNum, downPackNum, upPayLoad, downPayLoad);
            context.write(new Text(msisdn), flow);
        }
    }

    static class MyReducer extends Reducer<Text, MobileFlow, NullWritable, MobileFlow> {
        @Override
        protected void reduce(Text key, Iterable<MobileFlow> values, Context context) throws IOException, InterruptedException {
            MobileFlow result = null;
            Iterator<MobileFlow> itr = values.iterator();
            while (itr.hasNext()) {
                MobileFlow next = itr.next();
                if (result == null) {
                    result = new MobileFlow(next);
                } else {
                    result.setUpPackNum(result.getUpPackNum() + next.getUpPackNum());
                    result.setDownPackNum(result.getDownPackNum() + next.getDownPackNum());
                    result.setUpPayLoad(result.getUpPayLoad() + next.getUpPayLoad());
                    result.setDownPayLoad(result.getDownPayLoad() + next.getDownPayLoad());
                }
            }

            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        String INPUT_FILE = "hdfs://hadoop000:8020/inputMobileTestInfo";
        String OUTPUT_FILE = "hdfs://hadoop000:8020/outputMobileTestInfo";

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_FILE), conf);
        if (fileSystem.exists(new Path(OUTPUT_FILE))) {
            fileSystem.delete(new Path(OUTPUT_FILE), true);
        }

        Job job = Job.getInstance(conf, "Mobile Info Test");
        FileInputFormat.setInputPaths(job, INPUT_FILE);
        job.setJarByClass(MobileFlow.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MobileFlow.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MobileFlow.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
