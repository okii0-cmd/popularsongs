import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable zero = new IntWritable(0);
    private Text word = new Text();
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        context.write(new Text("Record"), one); //spit out the key,value(1) pairs.

//        CSVReader reader = new CSVReader(new StringReader(value.toString()));
//        string[] n = reader.readNext();
//        int colNum = 0 ;
//        for(string s: n){
//            context.write(new Text(colNum +" "+s),one);
        }

//


        }


