package per.owisho.learn.hadoop.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;

public class RecordReaderApp {

    public static class MyRecordReader extends RecordReader<LongWritable, Text> {

        private long start;//起始位置

        private long end;//结束位置

        private long pos;//当前位置

        private FSDataInputStream fin = null;//文件输入流

        private LongWritable key = null;//key

        private Text value = null;//value

        private LineReader reader = null;//定义行阅读器（hadoop.util包下的类）

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;//获取分片
            start = fileSplit.getStart();//获取起始位置
            end = start + fileSplit.getLength();//获取结束位置
            Configuration conf = context.getConfiguration();//创建配置
            Path path = fileSplit.getPath();//获取文件路径
            FileSystem fileSystem = path.getFileSystem(conf);//根据路径获取文件系统
            fin = fileSystem.open(path);//打开文件输入流
            fin.seek(start);//找到开始位置开始读取
            reader = new LineReader(fin);//创建阅读器
            pos = 1;//将当前位置设置为1
        }

        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            if (value == null) {
                value = new Text();
            }
            if (reader.readLine(value) == 0) {
                return false;
            }
            pos++;
            return true;
        }

        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        public void close() throws IOException {
            fin.close();
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MyReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("奇数行还是偶数行：" + key);
            long sum = 0;//定义求和的变量
            for (Text val : values) {//遍历value求和
                sum += Long.parseLong(val.toString());//累加
            }
            //判断奇偶数
            if (key.get() == 0) {
                outKey.set("奇数之和为：");
            } else {
                outKey.set("偶数之和为：");
            }
            //设置value
            outValue.set(sum);
            //把结果写出去
            context.write(outKey, outValue);
        }
    }

    //driver
    public static void main(String[] args) throws Exception {
        String INPUT_PATH = "hdfs://hadoop000:8020/recordreader";
        String OUTPUT_PATH = "hdfs://hadoop000:8020/outputrecordreader";
        Configuration conf = new Configuration();
        final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }
        Job job = Job.getInstance(conf, "Record Reader App");
        job.setJarByClass(RecordReaderApp.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setInputFormatClass(MyInputFormat.class);
        //设置自定义Mapper类和map函数输出数据的key和value类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //设置分区和reduce数量（reduce的数量和分区的数量对应，因为分区为一个，所以reduce的数量也是一个）
        job.setPartitionerClass(MyPartitioner.class);
        job.setNumReduceTasks(2);
        //shuffle把数据从Map端拷贝到Reduce端
        //指定Reducer类和输出key和value类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //指定输出的路径和设置输出的格式化类
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);
        //提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
