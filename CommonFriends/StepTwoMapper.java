package CommonFriends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{

	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		String[] friendAndPersons = str.split("\t");
		String friend = friendAndPersons[0];
		String[] persons = friendAndPersons[1].split(",");
		Arrays.sort(persons);
		
		for(int i = 0;i<persons.length-1;i++){
			for(int j = i+1;j < persons.length;j++){
				context.write(new Text(persons[i]+"--"+persons[j]), new Text(friend));
			}
		}
	}
}
