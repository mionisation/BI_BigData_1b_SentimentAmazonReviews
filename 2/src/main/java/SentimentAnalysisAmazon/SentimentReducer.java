package SentimentAnalysisAmazon;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SentimentReducer  extends Reducer<Text, DoubleArrayWritable, Text, DoubleWritable> {
	
	/**
	 * Simply iterates over the array and sums up both the number of positive and negative
	 * words. Subsequently the sentiment is calculated with a simple formula.
	 */
    @Override
    public void reduce(Text word, Iterable<DoubleArrayWritable> countsList, Context context) throws IOException, InterruptedException {
      double pos = 0.0;
      double neg = 0.0; 
      for (DoubleArrayWritable counts : countsList) {
    	  pos += ((DoubleWritable)(counts.get())[0]).get();
    	  neg += ((DoubleWritable)(counts.get())[1]).get();
          
      }
      double result = (pos - neg) / (pos + neg);
      context.write(word, new DoubleWritable(result));
    }
  }