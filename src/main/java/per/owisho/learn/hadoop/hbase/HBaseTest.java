package per.owisho.learn.hadoop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTest {

    //读取配置文件
    static Configuration cfg = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException {
        create("scores","grade","course");
        describe("scores");
        addColumnFamily("scores","f1","f2");
        describe("scores");
        removeColumnFamily("scores","f1");
        describe("scores");
        list();
    }

    //列出数据库中所有表
    public static void list() throws IOException {
        //创建数据库连接
        Connection conn = ConnectionFactory.createConnection(cfg);

        //Admin用于管理HBase数据库的表信息
        Admin admin = conn.getAdmin();
        System.out.println("list table:");

        for (TableName name : admin.listTableNames())
            System.out.println(name);

        conn.close();
    }

    //创建表
    public static void create(String tableName, String... familyNames) throws IOException {
        Connection conn = ConnectionFactory.createConnection(cfg);

        Admin admin = conn.getAdmin();

        TableName tableToCreate = TableName.valueOf(tableName);
        boolean b = admin.tableExists(tableToCreate);
        if (b) {
            admin.disableTable(tableToCreate);
            admin.deleteTable(tableToCreate);
        }

        HTableDescriptor htd = new HTableDescriptor(tableToCreate);
        for (String family : familyNames)
            htd.addFamily(new HColumnDescriptor(family));
        admin.createTable(htd);

        conn.close();
    }

    //修改表--增加列族
    public static void addColumnFamily(String tableName, String... familyNames) throws IOException {
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();

        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        for (String familyName : familyNames)
            htd.addFamily(new HColumnDescriptor(familyName));

        admin.modifyTable(htd);

        conn.close();
    }

    //修改表--删减列族
    public static void removeColumnFamily(String tableName, String... familyNames) throws IOException {
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();

        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        for (String familyName : familyNames)
            htd.removeFamily(Bytes.toBytes(familyName));

        admin.modifyTable(htd);

        conn.close();
    }

    //查看表接口
    public static void describe(String tableName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();

        TableName tn = TableName.valueOf(tableName);
        HTableDescriptor htd = admin.getTableDescriptor(tn);
        System.out.println("===descibe " + tableName + " :===");
        for(HColumnDescriptor hcd:htd.getColumnFamilies()){
            System.out.println(hcd.getNameAsString());
        }
        System.out.println("================");
        conn.close();
    }

}
