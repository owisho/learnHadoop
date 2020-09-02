package per.owisho.learn.hadoop.mapreduce.reducejoin.test;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class MyTestApp {

    public static void main(String[] args) throws Exception {
        String INPUT_PATH = "hdfs://localhost:9000/testJoin";
        String OUTPUT_PATH = "hdfs://localhost:9000/outputTestJoin";
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }
        Job job = Job.getInstance(conf, "Reduce Join");
        //设置主类
        job.setJarByClass(MyTestApp.class);
        //设置Map和Reduce处理类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ArticleInfo.class);
        //设置Reduce输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ArticleInfo.class);
        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
