package CommonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepTwoDriver {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,StepTwoDriver.class.getSimpleName());
		job.setJarByClass(StepTwoDriver.class);
		FileInputFormat.setInputPaths(job, args[0]);

		//job.setInputFormatClass(FileInputFormat.class);
		job.setMapperClass(StepTwoMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setReducerClass(StepTwoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
