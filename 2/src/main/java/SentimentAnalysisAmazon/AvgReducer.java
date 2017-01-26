package SentimentAnalysisAmazon;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      double amount = 0; 
      for (DoubleWritable count : counts) {
        sum += count.get();
        amount++;
      }
     sum /= amount;
      context.write(word, new DoubleWritable(sum));
    }
  }