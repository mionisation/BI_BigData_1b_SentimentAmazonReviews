package SentimentAnalysisAmazon;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class SentimentDriver extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(SentimentDriver.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new SentimentDriver(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "sentimentcalculator");
    job.setJarByClass(this.getClass());
    //add positive and negative words
    job.addCacheFile(new Path("/pos-words.txt").toUri());
    job.addCacheFile(new Path("/neg-words.txt").toUri());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(SentimentMapper.class);
    job.setReducerClass(SentimentReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
