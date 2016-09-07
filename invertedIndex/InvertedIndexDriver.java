package invertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedIndexDriver {

	public static void main(String[] args) throws Exception{

		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,InvertedIndexDriver.class.getSimpleName());
		job.setJarByClass(InvertedIndexDriver.class);
		FileInputFormat.setInputPaths(job, args[0]);

		//job.setInputFormatClass(FileInputFormat.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	
		job.setCombinerClass(InvertedIndexCombiner.class);
		
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}

}
