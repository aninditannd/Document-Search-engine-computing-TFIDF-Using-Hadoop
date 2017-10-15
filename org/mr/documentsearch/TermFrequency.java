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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * @author anindita
 *This class contains the MR job to compute the logarithmic TermFrequency 
 */
public class TermFrequency extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(TermFrequency.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TermFrequency(), args);
    System.exit(res);
  }

  /**
   * This function is used to launch the map reduce job
 * @param args
 * @return
 * @throws Exception
 */
public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "TermFrequency");
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
 * This class contains the mapper function & used to generate key value pairs 
 */
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    /**
     * This map function process the input file & generate key value pairs
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
        	//Forming the key in the format word#####fileName
        	key=(word+"#####"+fileName).toLowerCase();
            currentWord = new Text(key);
            //writing the key value pairs of the mapper
            context.write(currentWord,one);
        }
    }
  }

  /**
 * This reduce class is used to process the key value pairs from the mapper &
 * write it to the output file
 *
 */
public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    /**
     * This reduce function is used write the termfrequency to the output file
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
      for (IntWritable count : counts) {
        sum += count.get();
      }
      double tf=0;
      if(sum!=0){
    	  //calculating the term frequency
    	  tf=(1+Math.log10(sum));
      }
      context.write(word, new DoubleWritable(tf));
    }
  }
  
}
