/**
 * File: RankReducer.java
 * This java file functions as the second reducer, inputting the records from the second mapper. It
 * calculates the page rank based on the number of outlinks of the articles and outputs records
 * similar to the records being output to the second mapper from the first reducer
 * with different page rank values.
 */

package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RankReducer extends Reducer<Text, Text, Text, Text> {
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



		for (Text value : values) {

			String line = value.toString();
			String[] tokens = line.split(",,,|\t");
			oldPageRank = Double.parseDouble(tokens[0]);

			// if it is not the outlinks

			if(!line.contains("#outlinks")) {
				
				countOutlinks = Integer.parseInt(tokens[1]);
				sumScore = sumScore + (oldPageRank/countOutlinks);
				pageRank = 0.15 + (0.85 *sumScore);

			}else {

				articleKey = key.toString();
				for(int i = 1; i< tokens.length-1; i++) {

					if(i == tokens.length-2) {
						outlinks = outlinks + tokens[i];
					} else {
						outlinks = outlinks + tokens[i] + ",,,";
					}				
				}
				if(pageRank==0) {
					pageRank = 0.15;
				}							
			}			
		}
		finalValue = new Text(pageRank + ",,," + outlinks);
		context.write(key, finalValue);
	}
}
