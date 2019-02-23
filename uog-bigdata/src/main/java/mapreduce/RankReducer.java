/**
 * export HADOOP_CLASSPATH=/home/cloudera/Downloads/uog-bigdata/target/uog-bigdata-0.0.1-SNAPSHOT.jar

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
