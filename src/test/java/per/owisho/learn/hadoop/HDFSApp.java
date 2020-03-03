package per.owisho.learn.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://139.9.112.228:8020";

    Configuration configuration = null;

    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp()");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
    }

    @After
    public void tearDown() throws Exception {
        fileSystem = null;
        configuration = null;
        System.out.println("HDFSApp.tearDown()");
    }

    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello world".getBytes());
        output.flush();
        output.close();
    }

    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        System.out.println(fileSystem.rename(oldPath, newPath));
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("C:\\Users\\owisho\\Documents\\test\\test.txt");
        Path dist = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dist);
    }

    @Test
    public void listFiles() throws Exception {
        FileStatus[] listStatus = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : listStatus) {
            String isDir = fileStatus.isDirectory() ? "文件夾" : "文件";
            String permission = fileStatus.getPermission().toString();
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + permission + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void getFileBlockLocations() throws Exception{
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/test/b.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
        for(BlockLocation block:blocks){
            for(String host:block.getHosts()){
                System.out.println(host);
            }
        }
    }

}
