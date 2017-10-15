package org.mr.documentsearch;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author anindita
 *This class ranks the search in decending order based on their accumulated score
 */
public class Rank extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Rank.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Rank(), args);
		System.exit(res);
	}

	/**
	   * This function is used to launch the map reduce job
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Rank");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(RankMap.class);
		job.setReducerClass(RankReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * This is the mapper class to read the file & generate key value pairs
	 *
	 */
	public static class RankMap extends Mapper<LongWritable, Text, Text, Text> {
		
		/**
		 * This method is used to send key value pairs to the reducer for sorting
		 * @param offset
		 * @param lineText
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			context.write(new Text("Rank"),lineText);
			}
		}
	/**
	 * The reducer class to read the key value pairs from mapper
	 *
	 */
	public static class RankReduce extends Reducer<Text, Text, Text, Text> {
		/**
		 * This method reads the key value pairs from mapper & displays the rank of each word in a document
		 * @param word
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Map<String,Double> unsortedMap=new HashMap<String, Double>();
			for(Text value:values){
				String[] strArray=value.toString().split("\t");
				unsortedMap.put(strArray[0], Double.valueOf(strArray[1]));
			}
			Map<String, Double> sortedMap=sortByValue(unsortedMap);
			for(Map.Entry<String, Double> entry:sortedMap.entrySet()){
				LOG.info(" Writing "+entry.getKey()+"      "+entry.getValue());
				context.write(new Text(entry.getKey()),new Text(String.valueOf(entry.getValue())));
			}
		}
	}
	/**
	  * This is used to sort the key value pairs
	 * @param unsortMap
	 * @return
	 */
	private static Map<String, Double> sortByValue(Map<String, Double> unsortMap) {

       List<Map.Entry<String, Double>> list =
               new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());

       Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
           public int compare(Map.Entry<String, Double> o1,
                              Map.Entry<String, Double> o2) {
               return (o2.getValue()).compareTo(o1.getValue());
           }
       });
       
       Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
       for (Map.Entry<String, Double> entry : list) {
           sortedMap.put(entry.getKey(), entry.getValue());
       }
       return sortedMap;
   }
}
