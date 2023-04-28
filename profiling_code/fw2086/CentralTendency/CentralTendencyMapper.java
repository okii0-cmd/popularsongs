import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CentralTendencyMapper
 extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        if (key.get() == 0) { // skip header!
            return;
        }
        
        String line = value.toString();
        String[] cols = line.split(",");
        // columns 5, 6, 7, 8 are numerical
        for (int i = 5; i < 9; i++) {
            context.write(new IntWritable(i), new Text(cols[i]));
        }
    }
}