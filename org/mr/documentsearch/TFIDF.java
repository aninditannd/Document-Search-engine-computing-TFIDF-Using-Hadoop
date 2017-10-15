package org.mr.documentsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ContentSummary;
import org.myorg.TermFrequency;
//import org.myorg.*;

/**
 * @author anindita
 *This class contains the MR jobs to compute the TFIDF
 *
 */
public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);

	private static long DOCUMENT_TOTAL_COUNT=0;

	public static void main(String[] args) throws Exception {
		int tfres=ToolRunner.run(new TermFrequency(), args);
		int tfidfres = ToolRunner.run(new TFIDF(), args);
		System.exit(tfidfres);
	}

	/**
	 * This function is used to launch the map reduce jobs
	 * It contain two MR jobs. First job calculates the Term frequency
	 * The second job computes TFIDF taking the output of first MR job as input
	 * @param args
	 * @return
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();  

		//Finding the number of documents in the input path
		FileSystem fs = FileSystem.get(conf);
		Path pt = new Path(args[0]);
		ContentSummary cs = fs.getContentSummary(pt);
		DOCUMENT_TOTAL_COUNT = cs.getFileCount();  
		LOG.info("DOC count"+DOCUMENT_TOTAL_COUNT);

		int tfidfJobStatus=1;
		LOG.info("Completed termFreqjob job. starting tfidfJob job");
		//Adding the number of documents to the configuration of second job 
		Configuration config = new Configuration();
		config.set("DocumentCount", String.valueOf(DOCUMENT_TOTAL_COUNT));
		Job tfidfJob=Job.getInstance(config, "TFIDF");
		tfidfJob.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(tfidfJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(tfidfJob, new Path(args[2]));
		tfidfJob.setMapperClass(TFIDFMap.class);
		tfidfJob.setReducerClass(TFIDFReduce.class);
		tfidfJob.setMapOutputKeyClass(Text.class);
		tfidfJob.setMapOutputValueClass(Text.class);
		tfidfJob.setOutputKeyClass(Text.class);
		tfidfJob.setOutputValueClass(Text.class);
		tfidfJobStatus=tfidfJob.waitForCompletion(true)?0:1;
		LOG.info("Completed Second job");

		return tfidfJobStatus;
	}

	/**
	 * The mapper class of TFIDF job
	 *
	 */
	public static class TFIDFMap extends Mapper<LongWritable,Text,Text,Text>{

		/**
		 * This mapper function takes the output of term frequency job as input
		 * @param text
		 * @param lineText
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void map(LongWritable text,Text lineText,Context context) throws IOException, InterruptedException{

			String line=lineText.toString();

			//splitting the input to form key & value pairs

			//Spliting word & filename 
			String[] splitArr=line.split("[#]{5}");
			//fileName as key
			String key=splitArr[0];
			//spliting file Name & term frequency
			String[] splitValue=splitArr[1].split("\t");

			//Forming key value pairs for mapper & writing it
			context.write(new Text(key),new Text(splitValue[0]+"="+splitValue[1]));
		}
	}
	/**
	 * This is the reducer class of TFIDF job
	 *
	 */
	public static class TFIDFReduce extends Reducer<Text,Text,Text,Text>{
		/**
		 * This reduce function calculates the TFIDF score & writes it to the output file
		 * @param word
		 * @param documents
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		@Override
		public void reduce(Text word,Iterable<Text> documents, Context context) throws IOException,InterruptedException{	

			Configuration tfConf = context.getConfiguration();
			//Fetching the document count from 
			long totalDocumentCount = Long.valueOf(tfConf.get("DocumentCount"));
			long noOfDocuments=0;
			double idf=0;
			java.util.Map<String,Double> tfHashMap=new HashMap<String,Double>();
			String[] value;
			double tf=0;
			String key="";
			//calculating the number of documents that contain the word
			for(Text document:documents){
				noOfDocuments++;
				value=document.toString().split("=");
				key=word.toString()+"#####"+value[0];
				tf=Double.valueOf(value[1]);
				tfHashMap.put(key, tf);
			}
			//calculating TFIDF score
			double tfidf=0;
			idf=Math.log10(1+(totalDocumentCount/noOfDocuments));
			for(java.util.Map.Entry<String,Double> entry:tfHashMap.entrySet()){
				tfidf=entry.getValue()*idf;
				key=entry.getKey();
				context.write(new Text(key),new Text(String.valueOf(tfidf)));
			}

		}
	}
}
