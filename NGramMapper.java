import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NGramMapper extends Mapper<LongWritable, Text, NGramCustomKey,Text> {

	public void map(LongWritable key, Text value, Context context) {

		int sigma = 4;
		String line = value.toString();
		try {
			String inputWords[] = processLine(line);

			String ngram = "";
			String prefix = "";
			for (int i = 0; i < inputWords.length; i++) {
				for (int j = i; j < Math.min(sigma + i, inputWords.length); j++) {
					ngram = ngram + inputWords[j] + " ";
				}
				ngram = ngram.trim();
				if (ngram.split(" ").length == 4) {
					prefix = ngram.split(" ")[0];
					NGramCustomKey customKey = new NGramCustomKey(new Text(prefix),new Text(ngram),new IntWritable(1));
					context.write(customKey, new Text(ngram + "|1"));
				}
				prefix = "";
				ngram = "";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String[] processLine(String line) {

		String words[];
		String input = line.replaceAll("[^a-zA-Z]+", " ").toLowerCase();
		StringTokenizer token = new StringTokenizer(input.trim());

		words = new String[token.countTokens()];
		int index = 0;
		while (token.hasMoreTokens()) {
			words[index] = token.nextToken();
			index++;
		}

		return words;
	}
}
