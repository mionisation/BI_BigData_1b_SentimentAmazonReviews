package SentimentAnalysisAmazon;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;


public class SentimentMapper extends Mapper<LongWritable, Text, Text, ArrayWritable> {   

	private Set<String> goodWords;
	private Set<String> badWords;
	
	protected void setup(Mapper<LongWritable, Text, Text, ArrayWritable>.Context context) throws IOException, InterruptedException
	{		
		URI[] localPaths = context.getCacheFiles();
		goodWords = parseWords(localPaths[0]);
		badWords = parseWords(localPaths[1]);
	}
	
	// Parse the positive words to match and capture during Map phase.
	private Set<String> parseWords(URI wordsURI) {
		Set<String> words = new HashSet<String>();
		try {
			BufferedReader fis = new BufferedReader(new FileReader(
					new File(wordsURI.getPath()).getName()));
			String word;
			while ((word = fis.readLine()) != null) {
				words.add(word);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing cached file '"
					+ goodWords + "' : " + StringUtils.stringifyException(ioe));
		}
		return words;
	}
	
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      line = line.toLowerCase();
      //get json product name, add composite writeable
      
      String[] prArr = line.split(",");
      String product = prArr[1].trim();
      String rating = prArr[2].trim();
      DoubleArrayWritable aw = new DoubleArrayWritable();
      //TODO fill values in array
      context.write(new Text(product), aw);
    }
  }