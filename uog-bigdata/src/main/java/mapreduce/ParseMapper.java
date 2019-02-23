/**
 * File: ParseReducer.java
 * This java file functions as the first reducer, inputting records from the first mapper, and 
 * outputting the latest article record, its original page rank which is 1.0, and its outlinks.
 */
package mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text articleKey = new Text();
	private Object[] articleValues;
	private String articleDateString;
	private long articleDate;
	private boolean correctRecord = false;
	private Text dateAndOutlinks;
	/*
	 * map class takes in the parameters from the user input and derives the correct records that
	 * are required to perform this map reduce job. The output includes the key, which is the
	 * article title and value which consists of the date and outlinks
	 *@param <key, value, context>
	 *@output<key, value>
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//each incoming line is being split into a string
		//Future: to optimize, use a tokenizer
		String line = value.toString();
		String[] tokens = line.split(" ");
		
		//getting the configurations that were set in the driver
		Configuration conf = context.getConfiguration();
		
		// recordDate is the date as of the current record
		long recordDate = 0;
		
		//article date is being set as the UserDate that is being
		//pulled from a configuration that was set in the driver
		try {
			articleDate = utils.ISO8601.toTimeMS(conf.get("UserDate"));
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		
		
		//checks if token is the first line (with REVISION)
		//to obtain the revision date and time we need to check if the first token is "REVISION"
		if (tokens[0].equals("REVISION")) {

			try {
				//record date is converted into the ISO8601 format
				recordDate = utils.ISO8601.toTimeMS(tokens[4]);

				//check if record date fits within the user input date
				if (recordDate < articleDate) {
					
					//article name is set as article key
					//article date string is being set as the date
					//boolean flag correct record is being set indicating that its a correct record
					articleKey.set(tokens[3]);
					articleDateString = Long.toString(articleDate);
					correctRecord = true;
				} else
					correctRecord = false;
			} catch (ParseException e) {
				System.exit(0);
			}

		}
		
		// check if token equals MAIN which indicates that its the outlinks line
		// also write to the map output within this statement; therefore,
		// check if its a correct record, which is the record which has the revision date and time
		// before the user inputted date.
		if (tokens[0].equals("MAIN") && correctRecord) {

			//array list dateOutlinks will contain both the date and the outlinks
			ArrayList<String> dateOutlinks = new ArrayList<String>();
			dateOutlinks.add(articleDateString);
			String articleTitle = articleKey.toString();
			
			// ignore the first token as its the "MAIN"
			// add all other tokens, which are the outlinks into the array list
			// until the end of the tokens
			for (int i = 1; i < tokens.length; i++) {
				
				//check for self loops and duplicates
				if (!articleTitle.equals(tokens[i]) && !dateOutlinks.contains(tokens[i]))
					dateOutlinks.add(tokens[i]);
			}

			// adding the array list dateOutlinks to a string dateOutlinksString
			// delimiter that is being used is ",,,"
			String dateOutlinksString = String.join(",,,", dateOutlinks);
			
			//setting dateOutlinks into a text and writing it to the output
			dateAndOutlinks = new Text(dateOutlinksString);
			context.write(articleKey, dateAndOutlinks);

		}

	}
}
