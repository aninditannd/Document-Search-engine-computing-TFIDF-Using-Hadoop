package org.mr.documentsearch;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author Anindita Nandi
 *	aninditannd03@gmail.com
 *
 *This class contains the MR job for calculating the word count in each document
 */
public class DocWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocWordCount.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DocWordCount(), args);
		System.exit(res);
	}

	/**
	   * This function is used to launch the map reduce job
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "DocWordCount");
		job.setJarByClass(this.getClass());
		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * This is the mapper class to read the file & generate key value pairs
	 *
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private long numRecords = 0;    
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		/**
		 * This method is used the read the inputs & genetate key value pairs of the format
		 *  word#####filename count​
		 * @param offset
		 * @param lineText
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString().toLowerCase();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			Text currentWord = new Text();
			String key="";
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				// forming the key of the format word#####fileName
				key=word+"#####"+fileName;
				currentWord = new Text(key);
				context.write(currentWord,one);
			}
		}
	}

	/**
	 * The reducer class to read the key value pairs from mapper
	 *
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		/**
		 * This method reads the key value pairs from mapper & displays the count of each word in a document
		 * @param word
		 * @param counts
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			//calculating the count of the word in the document
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
}
