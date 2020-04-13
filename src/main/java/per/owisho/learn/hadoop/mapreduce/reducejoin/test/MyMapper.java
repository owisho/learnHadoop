package per.owisho.learn.hadoop.mapreduce.reducejoin.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, Text, ArticleInfo> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        String[] arr = val.split("\t");
        ArticleInfo info = new ArticleInfo();
        if (arr.length < 8) {//realInfo file
            arr = val.split(",");
            if (arr.length != 7)
                return;
            info.setArticleId(arr[5]);
            info.setEntryId(arr[6]);
            String click10str = arr[0];
            int click10 = StringUtils.isEmpty(click10str) ? 0 : Integer.parseInt(click10str);
            info.setRealClick10(click10);
            String click11str = arr[2];
            int click11 = StringUtils.isEmpty(click11str) ? 0 : Integer.parseInt(click11str);
            info.setRealClick11(click11);
            info.setFlag(1);
        } else {
            String impressionsStr = arr[0];
            int impressions = StringUtils.isEmpty(impressionsStr) ? 0 : Integer.parseInt(impressionsStr);
            info.setImpressions(impressions);
            String click11Str = arr[1];
            int click11 = StringUtils.isEmpty(click11Str) ? 0 : Integer.parseInt(click11Str);
            info.setClick11(click11);
            String click10Str = arr[2];
            int click10 = StringUtils.isEmpty(click10Str) ? 0 : Integer.parseInt(click10Str);
            info.setClick10(click10);
            String deltaStr = arr[3];
            int delta = StringUtils.isEmpty(deltaStr) ? 0 : Integer.parseInt(deltaStr);
            info.setDeltaData(delta);
            info.setArticleId(arr[5]);
            info.setEntryId(arr[7]);
            info.setFlag(0);
        }
        context.write(new Text(info.getArticleId()), info);
    }
}
