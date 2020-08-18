package per.owisho.learn.hadoop.tablemapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class TopMusic {

    static final String TABLE_MUSIC = "music";
    static final String TABLE_NAMELIST = "namelist";
    static final String OUTPUT_PATH = "topmusic";

    static class ScanMusicMapper extends TableMapper<Text, IntWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                if (Bytes.toString(CellUtil.cloneFamily(cell)).equals("info") && Bytes.toString(CellUtil.cloneQualifier(cell)).equals("name")) {
                    context.write(new Text(Bytes.toString(CellUtil.cloneValue(cell))), new IntWritable(1));
                }
            }
        }
    }

    /**
     * 汇总每首歌曲播放总次数
     */
    static class IntNumberReducer extends TableReducer<Text, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int playCount = 0;
            for (IntWritable num : values) {
                playCount += num.get();
            }
            //为Put操作指定行键
            Put put = new Put(Bytes.toBytes(key.toString()));
            //为Put操作指定列键
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("rank"), Bytes.toBytes(playCount));
            context.write(key, put);
        }
    }

    static class ScanMusicNameMapper extends TableMapper<IntWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            List<Cell> cells = value.listCells();
            for (Cell cell : cells) {
                context.write(new IntWritable(Bytes.toInt(CellUtil.cloneValue(cell))), new Text(Bytes.toString(key.get())));
            }
        }
    }

    /**
     * 实现降序排序
     */
    private static class IntWritableDecreaseingComparator extends IntWritable.Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    /**
     * 配置作业：播放统计
     */
    static boolean musicCount(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "music-count");
        //MapReduce程序作业基本配置
        job.setJarByClass(TopMusic.class);
        job.setNumReduceTasks(2);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        //使用HBase提供的工具类来设置job
        TableMapReduceUtil.initTableMapperJob(TABLE_MUSIC, scan, ScanMusicMapper.class, Text.class, IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAMELIST, IntNumberReducer.class, job);
        return job.waitForCompletion(true);
    }

    /**
     * 配置作业：排序
     */
    static boolean sortMusic(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = Job.getInstance(conf, "sort-music");
        job.setJarByClass(TopMusic.class);
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(IntWritableDecreaseingComparator.class);
        TableMapReduceUtil.initTableMapperJob(TABLE_NAMELIST, new Scan(), ScanMusicNameMapper.class, IntWritable.class, Text.class, job);
        Path output = new Path(OUTPUT_PATH);
        if (FileSystem.get(conf).exists(output)) {
            FileSystem.get(conf).delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        return job.waitForCompletion(true);
    }

    /**
     * 查看输出文件
     */
    static void showResult() throws IllegalArgumentException, IOException {
        FileSystem fs = FileSystem.get(conf);
        InputStream in = null;
        try {
            in = fs.open(new Path(OUTPUT_PATH + "/part-r-00000"));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    static Configuration conf = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        GenericOptionsParser gop = new GenericOptionsParser(conf, args);
        String[] otherArgs = gop.getRemainingArgs();
        if (musicCount(otherArgs)) {
            if (sortMusic(otherArgs)) {
                showResult();
            }
        }
    }
}
