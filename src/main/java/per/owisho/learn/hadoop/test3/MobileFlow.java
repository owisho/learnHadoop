package per.owisho.learn.hadoop.test3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MobileFlow implements WritableComparable<MobileFlow> {

    private String msisdn;
    private Long upPackNum;
    private Long downPackNum;
    private Long upPayLoad;
    private Long downPayLoad;

    public MobileFlow() {
    }

    public MobileFlow(String msisdn, Long upPackNum, Long downPackNum, Long upPayLoad, Long downPayLoad) {
        this.msisdn = msisdn;
        this.upPackNum = upPackNum;
        this.downPackNum = downPackNum;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
    }

    public MobileFlow(MobileFlow info) {
        this.msisdn = info.msisdn;
        this.upPackNum = info.upPackNum;
        this.downPackNum = info.downPackNum;
        this.upPayLoad = info.upPayLoad;
        this.downPayLoad = info.downPayLoad;
    }

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public Long getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(Long upPackNum) {
        this.upPackNum = upPackNum;
    }

    public Long getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(Long downPackNum) {
        this.downPackNum = downPackNum;
    }

    public Long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(Long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public Long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(Long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    @Override
    public int compareTo(MobileFlow o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(msisdn);
        out.writeLong(upPackNum);
        out.writeLong(downPackNum);
        out.writeLong(upPackNum);
        out.writeLong(downPackNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.msisdn = in.readUTF();
        this.upPackNum = in.readLong();
        this.downPackNum = in.readLong();
        this.upPayLoad = in.readLong();
        this.downPayLoad = in.readLong();
    }
}
