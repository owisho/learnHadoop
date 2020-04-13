package per.owisho.learn.hadoop.mapreduce.reducejoin.test;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArticleInfo implements WritableComparable {

    private int impressions;
    private int click11;
    private int realClick11;
    private int click10;
    private int realClick10;
    private int deltaData;
    private String articleId;
    private String entryId;
    private int flag;//0-无真实点击的数据，1-有真实点击的数据

    @Override
    public String toString() {
        return this.impressions + "," + this.click11 + "," + this.realClick11 + "," + this.click10 + "," + this.realClick10 + "," + this.deltaData + "," + this.articleId + "," + this.entryId;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(impressions);
        out.writeInt(click11);
        out.writeInt(realClick11);
        out.writeInt(click10);
        out.writeInt(realClick10);
        out.writeInt(deltaData);
        out.writeUTF(articleId);
        out.writeUTF(entryId);
        out.writeInt(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.impressions = in.readInt();
        this.click11 = in.readInt();
        this.realClick11 = in.readInt();
        this.click10 = in.readInt();
        this.realClick10 = in.readInt();
        this.deltaData = in.readInt();
        this.articleId = in.readUTF();
        this.entryId = in.readUTF();
        this.flag = in.readInt();
    }

    public ArticleInfo() {
    }

    public ArticleInfo(ArticleInfo info) {
        this.impressions = info.impressions;
        this.click11 = info.click11;
        this.realClick11 = info.realClick11;
        this.click10 = info.click10;
        this.realClick10 = info.realClick10;
        this.deltaData = info.deltaData;
        this.articleId = info.articleId;
        this.entryId = info.entryId;
        this.flag = info.flag;
    }

    public int getImpressions() {
        return impressions;
    }

    public void setImpressions(int impressions) {
        this.impressions = impressions;
    }

    public int getClick11() {
        return click11;
    }

    public void setClick11(int click11) {
        this.click11 = click11;
    }

    public int getRealClick11() {
        return realClick11;
    }

    public void setRealClick11(int realClick11) {
        this.realClick11 = realClick11;
    }

    public int getClick10() {
        return click10;
    }

    public void setClick10(int click10) {
        this.click10 = click10;
    }

    public int getRealClick10() {
        return realClick10;
    }

    public void setRealClick10(int realClick10) {
        this.realClick10 = realClick10;
    }

    public int getDeltaData() {
        return deltaData;
    }

    public void setDeltaData(int deltaData) {
        this.deltaData = deltaData;
    }

    public String getArticleId() {
        return articleId;
    }

    public void setArticleId(String articleId) {
        this.articleId = articleId;
    }

    public String getEntryId() {
        return entryId;
    }

    public void setEntryId(String entryId) {
        this.entryId = entryId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

}
