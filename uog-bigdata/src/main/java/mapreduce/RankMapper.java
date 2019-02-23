package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Text outlinksText = new Text();
		Text newKey = new Text();
		String original = "";

		String line = value.toString();
		String[] tokens = line.split(",,,|\t");
		String articleKey = tokens[0];
		String pageRank = tokens[1];
		//first value is the key
		//second value is the page rank
		
		int countOutlinks = tokens.length - 2;

		Text inlinksText = new Text(pageRank + ",,," + countOutlinks + ",,," + articleKey);

		for (int i = 2; i < tokens.length; i++) {

			newKey.set(tokens[i]);
			context.write(newKey, inlinksText);

		}
		for(int i = 1; i<tokens.length; i++) {
			original = original + tokens[i]+",,,";
		}
		
		original = original + "#outlinks";
		
		outlinksText.set(original);
		
		context.write(new Text(articleKey), outlinksText);
	}
}