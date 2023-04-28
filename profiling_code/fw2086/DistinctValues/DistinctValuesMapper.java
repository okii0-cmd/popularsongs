import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class DistinctValuesMapper
 extends Mapper<LongWritable, Text, IntWritable, Text> {
 private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        String line = value.toString();
        String[] cols = line.split(",");
        
        for (int i = 0; i < cols.length; i++) {
            context.write(new IntWritable(i), new Text(cols[i]));
        }
    }
}