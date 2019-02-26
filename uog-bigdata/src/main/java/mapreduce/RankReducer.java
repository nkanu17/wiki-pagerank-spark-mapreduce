/**
 *  Group: UG37 Members: Nitin Kanukolanu (2416724K), Shawn Yeng Wei Xen (2395121Y)
 *  File: RankReducer.java 
 *  This file functions as the reducer that calculates the page rank. 
 *  Receives the output from the RankMapper and computes the page rank according to the number of iterations. 
 *  Page Rank formula is shown in the read me file.
 *  The output is the article title, page rank, and outlinks (if present) for every article present.
 */
package mapreduce;
 
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RankReducer extends Reducer<Text, Text, Text, Text> {
	/*
	 * @param <key: line/index number as Text, page rank and list of outlinks as TextArrayWritable, context as Context>
	 * @output  <key: article title as Text, value: page rank and list of outlinks as TextArrayWritable>
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		double oldPageRank= 0.0;
		double pageRank = 0.0;
		double sumScore = 0.0;
		int countOutlinks = 0;
		boolean sumFlag = false;
		Text finalValue;
		Text finalKey;
		String outlinks = "";
		int count = 0;
		String articleKey = key.toString();


		// Checking every node and computing page rank
		for (Text value : values) {

			String line = value.toString();
			String[] tokens = line.split(",,,|\t");
			oldPageRank = Double.parseDouble(tokens[0]);

			// if this is one of the inlinks (not original article title
			// the page rank is being computed
			if(!line.contains("#outlinks")) {
				
				countOutlinks = Integer.parseInt(tokens[1]);
				sumScore = sumScore + (oldPageRank/countOutlinks);
				pageRank = 0.15 + (0.85 *sumScore);

			// if it is the orginal article key with all the outlinks	
			}else {

				articleKey = key.toString();
				for(int i = 1; i< tokens.length-1; i++) {

					if(i == tokens.length-2) {
						outlinks = outlinks + tokens[i];
					} else {
						outlinks = outlinks + tokens[i] + ",,,";
					}				
				}
				// if page rank is 0 meaning no references for a certain article
				if(pageRank==0) {
					pageRank = 0.15;
				}							
			}			
		}
		//final key is the article title and value is the page rank and outlinks
		finalValue = new Text(pageRank + ",,," + outlinks);
		context.write(key, finalValue);
	}
}
