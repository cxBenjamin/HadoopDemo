package CommonFriends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	
	//output <f,p>
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		String[] personAndFriends = str.split(":");
		String person = personAndFriends[0];
		String[] friends = personAndFriends[1].split(",");
		
		for (String friend : friends) {
			
			context.write(new Text(friend), new Text(person));
		}
		
	}
	

}
