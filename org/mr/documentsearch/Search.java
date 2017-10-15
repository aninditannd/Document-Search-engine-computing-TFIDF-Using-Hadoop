package org.mr.documentsearch;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author anindita
 */
public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);
	private static String USER_INPUT="";
	public static void main(String[] args) throws Exception {
		//Getting the Search string from the user
		Scanner sc=new Scanner(System.in);
		System.out.println("Enter the search String");
		USER_INPUT=sc.nextLine();
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);

	}
	/**
	 * This function is used to launch the map reduce job
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {

		Configuration config = new Configuration();
		LOG.info("userInput"+USER_INPUT);
		config.set("userInput", USER_INPUT);
		Job job = Job.getInstance(config, "Search");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * This is the Map class for Search function
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		/**
		 * This is used to compute the key & value pairs for the search string
		 * @param offset
		 * @param lineText
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			String[] splitArr=line.split("[#]{5}");
			String key=splitArr[0];
			
			String[] splitVal=splitArr[1].split("\t");
			String fileName=splitVal[0];
			String value=splitVal[1];
			//fetching the user input from config object
			Configuration tfConf = context.getConfiguration();
			String userInput = tfConf.get("userInput");
			String[] inputWords=userInput.split(" +");
			
			//fetching just the search strings
			for(String input:inputWords){
				if(input.equalsIgnoreCase(key.toString())){
					context.write(new Text(fileName),new DoubleWritable(Double.valueOf(value)));
					break;
				}
			}
		}
	}

	/**
	 * This is used to compute the score for each document
	 *
	 */
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		/**
		 * This function is used to accumulate the score for each document based on the search string
		 * @param word
		 * @param values
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;	
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			context.write(word, new DoubleWritable(sum));
		}
	}
}

