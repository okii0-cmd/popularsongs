import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
public class CleanV2 {
    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "((())))");
    job.setNumReduceTasks(1);
	job.setJarByClass(CleanV2.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(CleanMapperV2.class);
       // job.setReducerClass(DBProfileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	///wait for job completion
	job.waitForCompletion(true);	
    
    }
}
