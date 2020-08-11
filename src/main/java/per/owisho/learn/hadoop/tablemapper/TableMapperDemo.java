package per.owisho.learn.hadoop.tablemapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TableMapperDemo {

    static class MyMapper extends TableMapper<Text,Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            for(Cell cell:cells){
                String outValue = String.format("RowKey: %s Family: %s Qualifier: %s cellValue: %s ",
                        Bytes.toString(key.get()),//行键
                        Bytes.toString(CellUtil.cloneFamily(cell)),//列族
                        Bytes.toString(CellUtil.cloneQualifier(cell)),//列修饰符
                        Bytes.toString(CellUtil.cloneValue(cell))//单元格值
                        );
                context.write(new Text(CellUtil.getCellKeyAsString(cell)),new Text(outValue));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        //设置被读取的表
        conf.set(TableInputFormat.INPUT_TABLE,"music");
        //设置被扫描的列，多个列以空格分割。列格式为family:qualifier
        conf.set(TableInputFormat.SCAN_COLUMNS,"info:name info:gender");
        Job job = Job.getInstance(conf,"hbase-mapreduce-api");
        //Map Reduce 程序作业基本配置
        job.setJarByClass(TableMapperDemo.class);//通过驱动类的jar文件查找
        job.setInputFormatClass(TableInputFormat.class);//输入格式
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);//Mapper输出键类型
        job.setMapOutputValueClass(Text.class);//Mapper输出值类型

        List<String> libjars = new ArrayList<>();
        //添加依赖文件（执行"hadoop jar"命令所在节点的本地文件）
        libjars.add("file:/mnt/app/hadoop-2.9.2/share/hadoop/common/lib/zookeeper-3.4.6.jar");
        libjars.add("file:/mnt/app/hadoop-2.9.2/share/hadoop/common/hadoop-common-2.9.2.jar");
        libjars.add("file:/mnt/app/hadoop-2.9.2/share/hadoop/common/lib/guava-11.0.2.jar");
        libjars.add("file:/mnt/app/hadoop-2.9.2/share/hadoop/common/lib/protobuf-java-2.5.0.jar");
        libjars.add("file:/mnt/app/hadoop-2.9.2/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.9.2.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/hbase-server-1.3.6.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/hbase-hadoop-compat-1.3.6.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/hbase-client-1.3.6.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/metrics-core-2.2.0.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/netty-all-4.0.50.Final.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/htrace-core-3.1.0-incubating.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/hbase-common-1.3.6.jar");
        libjars.add("file:/mnt/app/hbase-1.3.6/lib/hbase-protocol-1.3.6.jar");
        //为作业指定依赖文件，使用"job"的配置属性"tmpjars"，以逗号分割
        job.getConfiguration().set("tmpjars",StringUtils.join(libjars,","));
        //作业输出路径（默认由fs.defaultFS指定具体文件系统），输出格式：默认
        Path output = new Path("/output2/music3");
        if(FileSystem.get(conf).exists(output)){
            FileSystem.get(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);
        job.waitForCompletion(true);
    }

}
