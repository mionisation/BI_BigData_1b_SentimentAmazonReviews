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
import org.json.JSONObject;


public class SentimentMapper extends Mapper<LongWritable, Text, Text, ArrayWritable> {   

	private Set<String> goodWords;
	private Set<String> badWords;
	
	protected void setup(Mapper<LongWritable, Text, Text, ArrayWritable>.Context context) throws IOException, InterruptedException
	{		
		URI[] localPaths = context.getCacheFiles();
		goodWords = parseWords(localPaths[0]);
		badWords = parseWords(localPaths[1]);
	}
	
	/** 
	 * Parse the positive words to match and capture during Map phase.
	 */
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
					+ words + "' : " + StringUtils.stringifyException(ioe));
		}
		return words;
	}
	
	/**
	 * Use JSON properties of the line.
	 * For each line both the review text and the summary is
	 * analyzed, meaning that the occurrences of bad and good words are counted.
	 */
    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      JSONObject js = new JSONObject(lineText.toString());
      //from json: get product name and reviews
      String product = (String) js.get("asin");
      double goodWordCount = getOccurrences(goodWords, (String)js.get("reviewText")) ;
      double badWordCount = getOccurrences(badWords, (String)js.get("reviewText")) ;
      goodWordCount += getOccurrences(goodWords, (String)js.get("summary")) ;
      badWordCount += getOccurrences(badWords, (String)js.get("summary")) ;
      //put in array to pass to reduce function
      DoubleWritable[] dw = {new DoubleWritable(goodWordCount), new DoubleWritable(badWordCount)};
      DoubleArrayWritable daw = new DoubleArrayWritable();
      daw.set(dw);
      context.write(new Text(product), daw);
    }
    
    /**
     * Returns the number of occurrences on any word of the
     * wordlist that is in the text. 
     * @param wordList a set of strings representing either positive or negative words
     * @param text a particular line of text
     * @return the number how many words of the text occur in the provided list of
     * (positive or negative) words
     */
    private double getOccurrences(Set<String> wordList, String text) {
    	double count = 0.0;
    	for (String word : text.replace(",", "").split(" ")) {
    		if(wordList.contains(word))
    			count++;
    	}
    	return count;
    }
  }