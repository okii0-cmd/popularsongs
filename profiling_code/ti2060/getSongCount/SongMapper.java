import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class SongMapper extends Mapper<LongWritable , Text, Text, IntWritable> {
    private Text song = new Text();
    private final static IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        String songTitle = columns[1];
        song.set(songTitle);
        context.write(song, one);
    }
}






