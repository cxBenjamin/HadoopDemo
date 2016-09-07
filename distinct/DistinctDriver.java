package distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DistinctDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,DistinctDriver.class.getSimpleName());
		job.setJarByClass(DistinctDriver.class);
		FileInputFormat.setInputPaths(job, args[0]);

		//job.setInputFormatClass(FileInputFormat.class);
		job.setMapperClass(DistinctMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setReducerClass(DistinctReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
		
	}

}
