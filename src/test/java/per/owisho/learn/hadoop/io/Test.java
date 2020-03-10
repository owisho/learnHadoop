package per.owisho.learn.hadoop.io;

public class Test {
    public static void main(String[] args) throws Exception {
        Person person = new Person("zhangsan",27,"man");
        byte[] values = HadoopSerializationUtil.serialize(person);
        Person p = new Person();
        HadoopSerializationUtil.deserialize(p,values);
        System.out.println(p);
    }
}
