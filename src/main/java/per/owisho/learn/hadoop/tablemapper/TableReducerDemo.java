package per.owisho.learn.hadoop.tablemapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

public class TableReducerDemo {

    static class MyMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //取出每行中的所有单元，实际上只扫描了一列(info:name)
            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                context.write(new Text(Bytes.toString(CellUtil.cloneValue(cell))), new IntWritable(1));
            }
        }
    }

    static class MyReducer extends TableReducer<Text, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int playCount = 0;
            for (IntWritable num : values) {
                playCount += num.get();
            }
            //为Put操作指定行键
            Put put = new Put(Bytes.toBytes(key.toString()));
            //为Put操作指定列和值
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("rank"), Bytes.toBytes(playCount));
            context.write(key, put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        GenericOptionsParser gop = new GenericOptionsParser(conf,args);
        Job job = Job.getInstance(conf,"top-music");
        //Map Reduce程序作业基本配置
        job.setJarByClass(TableReducerDemo.class);
        job.setNumReduceTasks(2);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
        //使用HBase提供的工具类来设置job
        TableMapReduceUtil.initTableMapperJob("music",scan,MyMapper.class,Text.class,IntWritable.class,job);
        TableMapReduceUtil.initTableReducerJob("namelist",MyReducer.class,job);
        job.waitForCompletion(true);
    }

}
