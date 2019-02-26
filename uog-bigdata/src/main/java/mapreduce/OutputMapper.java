/**
 *  Group: UG37 Members: Nitin Kanukolanu (2416724K), Shawn Yeng Wei Xen (2395121Y)
 *  File: OutputMapper.java 
 *  This file functions as the final mapper that produces the output with just the article key and page rank
 */
package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class OutputMapper extends Mapper<LongWritable, Text, Text, Text> {
	/* 
	 * @param <key: line/index number as Text, page rank and list of outlinks as TextArrayWritable>
	 * @output <key: article title as Text, value: page rank as TextArrayWritable> 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//reading in the input and removing all outlinks  
		String line = value.toString();
		String[] tokens = line.split(",,,|\t");
		String articleKey = tokens[0];
		String pageRank = tokens[1];
		
		//output is just the article and its page rank
		context.write(new Text(articleKey), new Text(pageRank));
	}
}
