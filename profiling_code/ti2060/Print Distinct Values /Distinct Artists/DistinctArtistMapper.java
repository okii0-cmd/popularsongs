import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class DistinctArtistMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text outKey = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        String artist = columns[2];
        outKey.set(artist);
        context.write(outKey, NullWritable.get());
    }
}






