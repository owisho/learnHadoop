package per.owisho.learn.hadoop.tablemapper;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

public class ParamsTableMapperDemo {

    static class MyMapper extends TableMapper<Text,Text> {
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

        GenericOptionsParser parser = new GenericOptionsParser(conf,args);
        String[] otherArgs = parser.getRemainingArgs();
        //设置被读取的表
        conf.set(TableInputFormat.INPUT_TABLE,"music");
        //设置被扫描的列，多个列以空格分割。列格式为family:qualifier
        conf.set(TableInputFormat.SCAN_COLUMNS,"info:name info:gender");
        Job job = Job.getInstance(conf,"hbase-mapreduce-api");
        //Map Reduce 程序作业基本配置
        job.setJarByClass(ParamsTableMapperDemo.class);//通过驱动类的jar文件查找
        job.setInputFormatClass(TableInputFormat.class);//输入格式
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);//Mapper输出键类型
        job.setMapOutputValueClass(Text.class);//Mapper输出值类型

        //作业输出路径（默认由fs.defaultFS指定具体文件系统），输出格式：默认
        Path output = new Path("/output2/music3");
        if(FileSystem.get(conf).exists(output)){
            FileSystem.get(conf).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);
        job.waitForCompletion(true);
    }

}
