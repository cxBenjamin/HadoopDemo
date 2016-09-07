package invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{

	Text info = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for (Text value : values) {
			sum+=Integer.parseInt(value.toString());
		}
		//word+":"+filename
		String str = key.toString();
		String[] keys = str.split(":");
		info.set(keys[1]+":"+sum);
		
		context.write(new Text(keys[0]), info);
	}

}
