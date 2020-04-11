package per.owisho.learn.hadoop.mapreduce.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Emplyee implements WritableComparable {

    private String empNo = "";
    private String empName = "";
    private String deptNo = "";
    private String deptName = "";
    private int flag;

    public Emplyee() {
    }

    public Emplyee(String empNo, String empName, String deptNo, String deptName, int flag) {
        this.empNo = empNo;
        this.empName = empName;
        this.deptNo = deptNo;
        this.deptName = deptName;
        this.flag = flag;
    }

    public Emplyee(Emplyee e) {
        this.empNo = e.empNo;
        this.empName = e.empName;
        this.deptNo = e.empNo;
        this.deptName = e.empName;
        this.flag = e.flag;
    }

    public String getEmpNo() {
        return empNo;
    }

    public void setEmpNo(String empNo) {
        this.empNo = empNo;
    }

    public String getEmpName() {
        return empName;
    }

    public void setEmpName(String empName) {
        this.empName = empName;
    }

    public String getDeptNo() {
        return deptNo;
    }

    public void setDeptNo(String deptNo) {
        this.deptNo = deptNo;
    }

    public String getDeptName() {
        return deptName;
    }

    public void setDeptName(String deptName) {
        this.deptName = deptName;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int compareTo(Object o) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.empNo);
        out.writeUTF(this.empName);
        out.writeUTF(this.deptNo);
        out.writeUTF(this.deptName);
        out.writeInt(this.flag);
    }

    public void readFields(DataInput in) throws IOException {
        this.empNo = in.readUTF();
        this.empName = in.readUTF();
        this.deptNo = in.readUTF();
        this.deptName = in.readUTF();
        this.flag = in.readInt();
    }

    @Override
    public String toString() {
        return this.empName + ", " + this.empName + ", " + this.deptNo + ", " + this.deptName;
    }
}
