package per.owisho.learn.hadoop.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements WritableComparable<Person> {

    private Text name = new Text();
    private IntWritable age = new IntWritable();
    private Text sex = new Text();

    public Person(String name, int age, String sex) {
        this.name.set(name);
        this.age.set(age);
        this.sex.set(sex);
    }

    public Person(Text name, IntWritable age, Text sex) {
        this.name = name;
        this.age = age;
        this.sex = sex;
    }

    public Person() {
    }

    public void set(String name, int age, String sex) {
        this.name.set(name);
        this.age.set(age);
        this.sex.set(sex);
    }

    public int compareTo(Person o) {
        int result = 0;
        int comp1 = name.compareTo(o.name);
        if (comp1 != 0) {
            return comp1;
        }
        int comp2 = age.compareTo(o.age);
        if (comp2 != 0) {
            return comp2;
        }
        int comp3 = sex.compareTo(o.sex);
        if (comp3 != 0) {
            return comp3;
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        name.write(out);
        age.write(out);
        sex.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        name.readFields(in);
        age.readFields(in);
        sex.readFields(in);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((age == null) ? 0 : age.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((sex == null) ? 0 : sex.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Person other = (Person) obj;
        if (age == null) {
            if (other.age != null) {
                return false;
            }
        } else if (!age.equals(other.age)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (sex == null) {
            if (other.sex != null) {
                return false;
            }
        } else if (!sex.equals(other.sex)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Person[name=" + name + ",age=" + age + ",sex=" + sex + "]";
    }
}
