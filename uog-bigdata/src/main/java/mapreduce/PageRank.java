/**
 *  Group: UG37 Members: Nitin Kanukolanu (2416724K), Shawn Yeng Wei Xen (2395121Y)
 *  File: PageRank.java 
 *  This java file functions as the driver for the program, 
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

		// 0. Instantiate a Job object; remember to pass the Driver's configuration on
		// to the job
		Job job = Job.getInstance(getConf(), "MapReduceJob1");

		job.setJarByClass(PageRank.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(ParseMapper.class);
		
		job.setReducerClass(ParseReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "_0"));

		job.getConfiguration().set("UserDate", date);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true);

	}

	public boolean Job2(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

		Job job2 = Job.getInstance(getConf(), "PageRank");

		job2.setJarByClass(PageRank.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(RankMapper.class);
		
		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));
		job2.setReducerClass(RankReducer.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		return job2.waitForCompletion(true);
	}
	public boolean Job3(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {

		Job job3 = Job.getInstance(getConf(), "OutputPageRank");

		job3.setJarByClass(PageRank.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapperClass(OutputMapper.class);
		
		FileInputFormat.setInputPaths(job3, new Path(inputPath));
		FileOutputFormat.setOutputPath(job3, new Path(outputPath));
		
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		return job3.waitForCompletion(true);
	}

	// Your main Driver method. Note: everything in this method runs locally at the
	// client.
	public int run(String[] args) throws Exception {

		int numIterations = 0;
		String rankInputPath = "";
		String rankOutputPath = "";
		String finalInputPath, finalOutputPath;
		String inputPath = args[0];
		String outputPath = args[1];
		numIterations = Integer.parseInt(args[2]);
		String inputDate = args[3];

		boolean completedJob1 = Job1(inputPath, outputPath, inputDate);

		if (!completedJob1) {
			return 1;
		}
		for (int i = 0; i < numIterations; i++) {
			rankInputPath = outputPath + "_"+ i;
			rankOutputPath = outputPath + "_" + (i + 1);
			
			boolean rankJob = Job2(rankInputPath, rankOutputPath);

		}
		finalInputPath = rankOutputPath;
		finalOutputPath = finalInputPath+"_output";
		boolean outputJob = Job3(finalInputPath, finalOutputPath);


		return 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
}