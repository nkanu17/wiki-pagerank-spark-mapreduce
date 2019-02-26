/**
 *  Group: UG37 Members: Nitin Kanukolanu (2416724K), Shawn Yeng Wei Xen (2395121Y)
 *  File: RankMapper.java 
 *  This file functions as the second mapper, receives input that is Article, PageRank, and list of outlinks
 *  and then assigns a node for all articles.
 *  Send an output key (which is the outlink article), and value (page rank and article (inlink) that references the outlink) 
 *  in a loop but for the final loop sends the original input as the output. 
 */
package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RankMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/* 
	 * @param <key: line/index number as Text, page rank and list of outlinks as TextArrayWritable>
	 * @output <key: article title as Text, value: page rank and inlink/outlinks as TextArrayWritable>
	 */
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
		// setting nodes
		for (int i = 2; i < tokens.length; i++) {

			newKey.set(tokens[i]);
			context.write(newKey, inlinksText);

		}
		
		//after all inlinks, setting the original article with the original outlinks to orginal
		for(int i = 1; i<tokens.length; i++) {
			original = original + tokens[i]+",,,";
		}
		
		//adding #outlinks to identify it in the RankReducer
		original = original + "#outlinks";
		
		outlinksText.set(original);
		
		context.write(new Text(articleKey), outlinksText);
	}
}
