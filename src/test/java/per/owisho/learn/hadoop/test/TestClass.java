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
        FileSystem fileSystem = FileSystem.get(new URI("http://hadoop000:8020"), new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        while (fsDataInputStream.read(byteBuffer) > 0) {
            byteArrayOutputStream.write(byteBuffer.array());
            byteBuffer.clear();
        }
        String txt = byteArrayOutputStream.toString("UTF-8");
        System.out.println(txt);
    }

}
