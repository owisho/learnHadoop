package per.owisho.learn.hadoop.recordreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<LongWritable,Text> {
    public int getPartition(LongWritable key, Text value, int numPartitions) {
        if(key.get()%2==0){
            key.set(1);
            return 1;
        }else{
            key.set(0);
            return 0;
        }
    }
}
