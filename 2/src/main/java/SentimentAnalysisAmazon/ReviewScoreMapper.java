package SentimentAnalysisAmazon;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {   

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      String[] prArr = line.split(",");
      String product = prArr[1].trim();
      String rating = prArr[2].trim();
      context.write(new Text(product), new DoubleWritable(Double.parseDouble(rating)));
    }
  }