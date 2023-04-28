import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class StatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private final Text OutKey = new Text();
    private final DoubleWritable VALUE = new DoubleWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        OutKey.set("danceability");
        VALUE.set(Double.parseDouble(columns[3]));
        context.write(OutKey, VALUE);

        OutKey.set("energy");
        VALUE.set(Double.parseDouble(columns[4]));
        context.write(OutKey, VALUE);

        OutKey.set("music_key");
        VALUE.set(Double.parseDouble(columns[5]));
        context.write(OutKey, VALUE);

        OutKey.set("loudness");
        VALUE.set(Double.parseDouble(columns[6]));
        context.write(OutKey, VALUE);

        OutKey.set("mode");
        VALUE.set(Double.parseDouble(columns[7]));
        context.write(OutKey, VALUE);

        OutKey.set("speechiness");
        VALUE.set(Double.parseDouble(columns[8]));
        context.write(OutKey, VALUE);

        OutKey.set("acousticness");
        VALUE.set(Double.parseDouble(columns[9]));
        context.write(OutKey, VALUE);

        OutKey.set("instrumentalness");
        VALUE.set(Double.parseDouble(columns[10]));
        context.write(OutKey, VALUE);

        OutKey.set("liveness");
        VALUE.set(Double.parseDouble(columns[11]));
        context.write(OutKey, VALUE);

        OutKey.set("valence");
        VALUE.set(Double.parseDouble(columns[12]));
        context.write(OutKey, VALUE);

        OutKey.set("tempo");
        VALUE.set(Double.parseDouble(columns[13]));
        context.write(OutKey, VALUE);

        OutKey.set("duration");
        VALUE.set(Double.parseDouble(columns[14]));
        context.write(OutKey, VALUE);

        OutKey.set("time_signature");
        VALUE.set(Double.parseDouble(columns[15]));
        context.write(OutKey, VALUE);


    }
}






