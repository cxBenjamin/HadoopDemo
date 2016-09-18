package CommonFriends;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StepOneReducer extends Reducer<Text, Text, Text, Text>{

	
	@Override
	protected void reduce(Text friend, Iterable<Text> persons, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		
		StringBuffer sb = new StringBuffer();
		
		for (Text p : persons) {
			sb.append(p.toString()).append(",");
		}
		
		context.write(friend, new Text(sb.toString()));
	}
}
