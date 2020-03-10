package per.owisho.learn.hadoop.mapfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;

public class MapFileWriter {

    static Configuration configuration = new Configuration();
    private static String url = "hdfs://hadoop000:8020";

    public static void main(String[] args) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(url), configuration);
        Path outPath = new Path("MyMapFile.map");
        Text key = new Text();
        key.set("mymapkey");
        Text value = new Text();
        value.set("mymapvalue");
        MapFile.Writer writer = new MapFile.Writer(configuration, fs, outPath.toString(), Text.class, Text.class);
        writer.append(key, value);
        IOUtils.closeStream(writer);
    }

}
