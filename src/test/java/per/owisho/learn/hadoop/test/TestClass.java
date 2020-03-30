package per.owisho.learn.hadoop.test;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;


public class TestClass {

    public static void main(String[] args) throws URISyntaxException, IOException {
        readFileTest();
    }

    /**
     * read file Test
     */
    public static void readFileTest() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/outputwc/part-r-00000"));
        ByteBuffer byteBuffer = ByteBuffer.allocate(5);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while (fsDataInputStream.read(byteBuffer) > 0) {
            byteArrayOutputStream.write(byteBuffer.array(),0,byteBuffer.position());
            byteBuffer.clear();
        }
        String txt = byteArrayOutputStream.toString("UTF-8");
        System.out.println(txt);
    }

}
