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
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

public class TableMapperDemo2 {

    static class MyMapper extends TableMapper<Text, Put> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                Put put = new Put(CellUtil.cloneValue(cell));
                //details:rank=0,在行键中插入该列键/值
                put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("rank"), Bytes.toBytes(0));//初始值0
                //此处与MyMapper、作业的输出键/值类型对应
                context.write(new Text(Bytes.toString(CellUtil.cloneValue(cell))), put);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        Job job = Job.getInstance(conf, "hbase-mapreduce-api");
        //Map Reduce程序作业基本配置
        job.setJarByClass(TableMapperDemo2.class);
        //指定输出格式
        job.setOutputFormatClass(TableOutputFormat.class);
        //指定输出目的表名
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "namelist");
        //使用HBase提供的工具类来设置job
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        //作业输出的键/值类型应和MyMapper中的输出键/值类型对应
        TableMapReduceUtil.initTableMapperJob("music", scan, MyMapper.class, Text.class, Put.class, job);
        job.waitForCompletion(true);
    }

}
