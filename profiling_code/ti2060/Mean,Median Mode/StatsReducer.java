import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class StatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        List<Double> list = new ArrayList<Double>();
        double sum = 0.0;
        double count = 0.0;
        //Caclculating mean
        for (DoubleWritable value : values) {
            double d = value.get();
            list.add(d);
            sum += d;
            count++;
        }
        double mean = sum / count;

        //Calculating Median
        double median = Double.NaN;
        Collections.sort(list);
        int mid = list.size()/2;
        if(list.size()%2 == 0){
             median = (list.get(mid-1)+list.get(mid))/2.0;
        }else{
             median = list.get((int)(mid/2.0));
        }

        //Calculating Mode
        Map<Double, Integer> countMap = new HashMap<Double, Integer>();
        int max = 0;
        double mode = Double.NaN;
        for (double value : list){
            int count1 = 1;
            if(countMap.containsKey(value)) {
                count1 += countMap.get(value);
            }
            countMap.put(value,count1);
            if (count1>max){
                max = count1;
                mode = value;
            }
        }

        //Creating output string

        String outputStr =  "mean:" + mean +" median:"+ median + " mode:"+ mode;



        context.write(key,new Text(outputStr));
    }
}
