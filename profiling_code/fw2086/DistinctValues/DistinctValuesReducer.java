import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class DistinctValuesReducer
 extends Reducer<IntWritable, Text, IntWritable, Text> {

 @Override
 public void reduce(IntWritable key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
    Set<String> distinctValues = new HashSet<>();
    for (Text value : values) {
        distinctValues.add(value.toString());
    }
    context.write(key, new Text(distinctValues.toString()));
 }
}
