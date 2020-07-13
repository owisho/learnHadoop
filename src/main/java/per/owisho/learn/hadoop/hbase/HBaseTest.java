package per.owisho.learn.hadoop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseTest {

    //读取配置文件
    static Configuration cfg = HBaseConfiguration.create();

    public static void main(String[] args) throws IOException {
        list();
    }

    //列出数据库中所有表
    public static void list() throws IOException{
        //创建数据库连接
        Connection conn = ConnectionFactory.createConnection(cfg);

        //Admin用于管理HBase数据库的表信息
        Admin admin = conn.getAdmin();
        System.out.println("list table:");

        for(TableName name:admin.listTableNames())
            System.out.println(name);

        conn.close();
    }

}
