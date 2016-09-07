package invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


//MapReduce file1.txt:1;file2.txt:1;file3.txt:2;  
//is file1.txt:1;file2.txt:2;  
//simple file1.txt:1;file2.txt:1;  
//powerful file2.txt:1;  
//Hello file3.txt:1;  
//bye file3.txt:1;  
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] words = line.split(" ");
		
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		//拼写map的key
		for (String word : words) {
			
			String outPutKey = word+":"+filename;
			context.write(new Text(outPutKey) , new Text("1"));
		}
		
		
	}

	
}
