package per.owisho.learn.hadoop.mapreduce.reducejoin.test;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, ArticleInfo, NullWritable, ArticleInfo> {

    @Override
    protected void reduce(Text key, Iterable<ArticleInfo> values, Context context) throws IOException, InterruptedException {

        ArticleInfo articleInfo = new ArticleInfo();
        for (ArticleInfo info : values) {
            int flag = info.getFlag();
            if (0 == flag) {//
                articleInfo.setImpressions(info.getImpressions());
                articleInfo.setClick10(info.getClick10());
                articleInfo.setClick11(info.getClick11());
                articleInfo.setDeltaData(info.getDeltaData());
                articleInfo.setEntryId(info.getEntryId());
                articleInfo.setArticleId(info.getArticleId());
            } else {
                articleInfo.setRealClick11(info.getRealClick11());
                articleInfo.setRealClick10(info.getRealClick10());
            }
        }
        context.write(NullWritable.get(), articleInfo);

    }
}
