import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CentralTendencyReducer
 extends Reducer<IntWritable, Text, IntWritable, Text> {

 @Override
 public void reduce(IntWritable key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException {
    
    List<Integer> valueList = new ArrayList<Integer>(); // for mean and median
    
    for (Text value: values) {
        valueList.add(Integer.parseInt(value.toString()));
    }

    Collections.sort(valueList);

    // For mean
    float sum = 0;
    int count = 0;
    // For mode
    Map<Integer, Integer> valueCounts = new HashMap<>();
    int maxCount = 0;
    int mode = 0;

    // Calculate mean and mode
    for (int value : valueList) {
        sum += value;
        count++;
        valueCounts.put(value, valueCounts.getOrDefault(value, 0) + 1);
        if (valueCounts.get(value) > maxCount) {
            maxCount = valueCounts.get(value);
            mode = value;
        }
    }
    float mean = sum / count;

    // Calculate median
    float median;
    if (count % 2 == 0) { // if even, get average of two middle values
        median = (valueList.get(count / 2 - 1) + valueList.get(count / 2)) / 2;
      } else { // if odd, get middle value
        median = valueList.get(count / 2);
      }

    String output = String.join(",", Float.toString(mean), Float.toString(median), Integer.toString(mode));
    context.write(key, new Text(output));
 }
}
