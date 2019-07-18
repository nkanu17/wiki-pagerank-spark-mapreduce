package spark;
import utils.ISO8601;
import java.util.List;
import java.util.stream.Collectors;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.*;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import com.google.common.collect.Iterables;


public class PageRankSpark {
	public static void main(String[] args) throws ParseException {
		//new spark context 
		JavaSparkContext context = new JavaSparkContext(new SparkConf().setAppName("PageRank"));
		// number of rounds/iterations from user input
		int numIterations = Integer.parseInt(args[2]);
		//date before by user input
		long articleDate = ISO8601.toTimeMS(args[3]);
		context.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n");
		
		// getting each revision
		JavaRDD<String> lines = context.textFile(args[0]); 
		// Inputs a record from a Wikipedia text article and outputs a Tuple2 of title article,
		// as well as another Tuple2 of date and list of outlinks, as long as the edit is before
		// the date specified and it's the most recent one.
		JavaPairRDD<String, String> links = lines.mapToPair(f -> {
			
			String outlinkString="";
			long recordDate = -1L;
			List<String> outlinks = null;//outlinks list
			String articleTitle = "";
			boolean correctRecord = false;
			for (String s : f.split("\n")) {
				if (s.startsWith("REVISION")) {
					String[] tokens = s.split(" ");
					try {
						recordDate = ISO8601.toTimeMS(tokens[4]);
						//checks here for the correct record (revison date)
						if (recordDate < articleDate)
						{
							articleTitle = tokens[3];
							correctRecord = true;
						}
						else
							correctRecord = false;
					} catch (ParseException e2) {
						e2.printStackTrace();
					}
				}
				//if the token is MAIN and it is a correct record, it removes self loops and joins outlinks
				if (s.startsWith("MAIN") && correctRecord) {
					outlinks = new ArrayList<>(Arrays.asList(s.split(" "))).stream().distinct()
							.collect(Collectors.toList());
					outlinks.remove(0);
					outlinkString= "".join(" ", outlinks);
				}

			}
			if (!correctRecord) {
				System.out.print("incorrect date");
			}
			//returns article title, date, and outlinks 
			return new Tuple2<String, String>(articleTitle, recordDate + outlinkString);

			
		})
		//check for latest revision, checks record date of a and b and returns the latest edition
		.reduceByKey((a, b) -> Long.parseLong(a.split(" ")[0]) >= Long.parseLong(b.split(" ")[0]) ? a : b).cache();

		//RDD to get updated outlinks and inlinks
		JavaPairRDD<String, Iterable<String>> updatedlinks = links.flatMapToPair(f -> {
			String articleTitle = f._1;
			String outlinksLine=f._2;
			List<String> outlinkList = new ArrayList<>(); 
			List<Tuple2<String, String>> mapResults = new ArrayList<>();
			//splits the outlinks string into an array list
			for (String s : outlinksLine.split(" ")) {
				outlinkList.add(s);
			}
			List<String> outlinks = outlinkList.subList(1, outlinkList.size());
			//retur			
			for (String outlink : outlinks) {
				if (!(articleTitle.equals(outlink))) { 
					mapResults.add(new Tuple2<String, String>(articleTitle, outlink));
				}
			}
			return mapResults;
			
		}).groupByKey().cache();
		
		//assigns initial rank of 1.0 to all updated link which are article title, outlinks with inlinks
		JavaPairRDD<String, Double> ranks = updatedlinks.mapValues(s -> 1.0); 

		//Page Rank formula
		//It runs depending on the number of iteration specified by the user
		for (int current = 0; current < numIterations; current++) {
			JavaPairRDD<String, Double> contribs = updatedlinks.join(ranks).values().flatMapToPair(v -> {
				List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
				int urlCount = Iterables.size(v._1);
				for (String s : v._1)
					res.add(new Tuple2<String, Double>(s, v._2() / urlCount));//counts the number of urls (inlins)
				return res;
			});
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85); //base page rank formula
			ranks = ranks.union(updatedlinks
			.mapValues(s -> 0.15)); //if no urls, then the score is just 0.15
		}
		//sorting functions
		ranks = ranks.mapToPair(s -> s.swap()).sortByKey(false).mapToPair(s -> s.swap()); 
        JavaRDD<Object> ranksFormat = ranks.map(s->s._1 + " " + s._2);  // Formate the output to "title rank"

		//output
		ranks.saveAsTextFile(args[1]);
		context.close();
	}

}
