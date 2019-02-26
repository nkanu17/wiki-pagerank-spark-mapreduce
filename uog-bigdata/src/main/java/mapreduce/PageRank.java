/**
 *  Group: UG37 Members: Nitin Kanukolanu (2416724K), Shawn Yeng Wei Xen (2395121Y)
 *  File: PageRank.java 
 *  This java file functions as the main driver for the program, 
 *  performing jobs for the page ranks.
 */
package mapreduce;
import mapreduce.OutputMapper;
import mapreduce.ParseMapper;
import mapreduce.ParseReducer;
import mapreduce.RankMapper;
import mapreduce.RankReducer;
import utils.ISO8601;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {
	
	public boolean Job1(String inputPath, String outputPath, String date)
			throws IOException, ClassNotFoundException, InterruptedException {

		// Instantiate a Job object and pass driver class' configuration
		Job job = Job.getInstance(getConf(), "MapReduceJob1");
		job.setJarByClass(PageRank.class);
		
		//set out put key and value types as Text
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//calling the mapper and reducer classes
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(ParseReducer.class);
		
		//the input file format, pass the inputPath from the arguments
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		//setting the output path from the output each mapreduce
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "_0"));
		
		//setting the date argument given to the UserDate, which can be accessed through out the configuration
		job.getConfiguration().set("UserDate", date);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true);

	}

	public boolean Job2(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		// Instantiate a Job object and pass driver class' configuration
		Job job2 = Job.getInstance(getConf(), "PageRank");
		job2.setJarByClass(PageRank.class);
		
		//set out put key and value types as Text
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		//calling mapper class
		job2.setMapperClass(RankMapper.class);
		//calling reducer class
		job2.setReducerClass(RankReducer.class);
		
		//the input file format, pass the inputPath from the arguments
		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		//setting the output path from the output each mapreduce 
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		 
		return job2.waitForCompletion(true); 
	}
	public boolean Job3(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
		
		// Instantiate a Job object and pass driver class' configuration
		Job job3 = Job.getInstance(getConf(), "OutputPageRank");

		job3.setJarByClass(PageRank.class);
		
		//set out put key and value types as Text
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		//call final output mapper class
		job3.setMapperClass(OutputMapper.class);
		
		//the input file format, pass the inputPath from the arguments
		FileInputFormat.setInputPaths(job3, new Path(inputPath));
		//setting the output path from the output each mapreduce
		FileOutputFormat.setOutputPath(job3, new Path(outputPath));
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		return job3.waitForCompletion(true);
	}

	// Main Driver method
	public int run(String[] args) throws Exception {

		int numIterations = 0;
		String rankInputPath = "";
		String rankOutputPath = "";
		String finalInputPath, finalOutputPath;
		//first argument of the user input is the input file path
		String inputPath = args[0];
		
		//second argument of the user input is output file path
		String outputPath = args[1];
		//third argument of the user input is number of iterations to compute the page rank
		numIterations = Integer.parseInt(args[2]);
		//fourth argument of the user input is the date in ISO 8601 format
		String inputDate = args[3];
		
		//calling job 1 to perform parsing
		boolean completedJob1 = Job1(inputPath, outputPath, inputDate);
		
		//if job 1 was completed successfully, the following will run a loop based on the number
		//of iterations
		if (!completedJob1) {
			return 1;
		}
		for (int i = 0; i < numIterations; i++) {
			
			//input is the output of ParseReducer + the iteration number
			rankInputPath = outputPath + "_"+ i;
			//output is the output path of the ParseReducer + iteration plus one
			//this way they are unique and iterative
			rankOutputPath = outputPath + "_" + (i + 1);
			
			//calling job 2, the page rank calculation job
			boolean rankJob = Job2(rankInputPath, rankOutputPath);

		}
		
		//input and output paths to final job
		finalInputPath = rankOutputPath;
		finalOutputPath = finalInputPath+"_output";
		
		//final job to output the results
		boolean outputJob = Job3(finalInputPath, finalOutputPath);


		return 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}
