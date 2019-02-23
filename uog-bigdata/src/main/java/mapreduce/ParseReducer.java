/**
 * Group: UG37
 * Members: Shawn Yeng Wei Xen (2395121Y), Nitin Kanukolanu (2416724K)
 * File: ParseMapper.java
 * This java file functions as the first mapper, inputting records from a text file containing 
 * parsed version of the complete Wikipedia edit history and outputting key-values of the article
 * title (as the key), date and time of the revision and outlinks of the article (as values).
 */

package mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {
	/*
	 * reduce class takes in the output produced by the ParseMapper map class
	 * this includes the key, which is article title, and value, which includes the date and the outlinks.
	 * This class computes the original page rank, 1.0, and outputs this along with the other outlinks 
	 * in the value. The key is the article title.
	 *@param <key, value, context>
	 *@output<key, value>
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text finalValues = new Text();
		long latestDate = 0;
		ArrayList<String> pageRankLinks = new ArrayList<String>();
		
		// values includes all of the articles and its tokens
		// value includes each line of the values
		for (Text value : values) {
			
			// turning the value into a string and splitting into
			// a string array with delimiter ,,,
			String line = value.toString();
			String[] tokens = line.split(",,,"); 
			
			// if latest date, then all the tokens are being added to the pageRankLinks
			long recordDate = Long.parseLong(tokens[0]);
			if (latestDate < recordDate) {
				for (int i = 1; i < tokens.length; i++) {
					pageRankLinks.add(tokens[i]);
				}
			}
		}
		
		// for each value, adding the original page rank 1.0
		pageRankLinks.add(0, "1.0");
		
		// join the array list with the delimiter ,,,
		String pageRankLinkString = String.join(",,,", pageRankLinks);
		// final output key and value
		finalValues.set(pageRankLinkString);
		context.write(key, finalValues);
	}
}
