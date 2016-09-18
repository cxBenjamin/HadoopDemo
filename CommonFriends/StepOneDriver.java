package CommonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepOneDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,StepOneDriver.class.getSimpleName());
		job.setJarByClass(StepOneDriver.class);
		FileInputFormat.setInputPaths(job, args[0]);

		//job.setInputFormatClass(FileInputFormat.class);
		job.setMapperClass(StepOneMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(StepOneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
