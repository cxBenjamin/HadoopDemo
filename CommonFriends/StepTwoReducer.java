package CommonFriends;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StepTwoReducer extends Reducer<Text, Text, Text, Text>{

	
	@Override
	protected void reduce(Text pairs, Iterable<Text> friends, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuffer sb = new StringBuffer();
		for (Text f : friends) {
			sb.append(f.toString()).append(" ");
		}
		
		context.write(pairs, new Text(sb.toString()));
	}
}
