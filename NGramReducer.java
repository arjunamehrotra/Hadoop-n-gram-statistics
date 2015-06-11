import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NGramReducer extends
		Reducer<NGramCustomKey, Text, Text, Text> {
	//variable to store the min frequency required.
	private int minFreq = 5;
	public void reduce(NGramCustomKey key, Iterable<Text> values,
			Context context) {
		
		//Stack to store terms
		Stack<String> terms = new Stack<String>();
		//Stack to store counts
		Stack<Integer> count = new Stack<Integer>();

		ArrayList<NGramCustomKey> ngramSeq = new ArrayList<NGramCustomKey>();

		//Find the frequency of each ngram with same prefix
		ngramSeq = getFrequencyForNgram(key, values);

		String currNgram = "";
		String prevNgram = "";
		try {
			//loop on all n-grams
			for (int i = 0; i < ngramSeq.size(); i++) {
				//get count
				int ngramFreq = Integer.parseInt(ngramSeq.get(i).getCount()
						.toString());
				//get ngram
				currNgram = ngramSeq.get(i).getNgram().toString().trim();

				String words[] = currNgram.split(" ");
				int currNgramLen = words.length;
				//loop and pop elements from terms and counts until of same size
				while (lcp(currNgram, prevNgram) < terms.size()) {
					if (count.peek() >= minFreq) {
						if(terms.size()>=4){
//							context.write(new Text(terms.toString().replaceAll("[,\\[\\]]", " ").trim()),new Text(count.peek()+""));
						}
					}
					terms.pop();
					count.push(count.pop() + count.pop());
				}

				if (terms.size() == currNgramLen) {
					count.push(count.pop() + ngramFreq);
				} else {
					for (int j = lcp(currNgram, prevNgram); j < currNgramLen; j++) {
						terms.push(words[j]);
						count.push(j == currNgramLen-1 ? ngramFreq : 0);
					}
				}
				prevNgram = currNgram;
				//if the frequency matches than emit word to output
				if (count.peek() >= minFreq) {
					if(terms.size()>=4){
						context.write(new Text(terms.toString().replaceAll("[,\\[\\]]", " ").trim()),new Text(count.peek()+""));
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private int lcp(String currNgram, String prevNgram) {
		int lcp = 0;
		//convert string to words array
		String currWords[] = currNgram.split(" ");
		String prevWords[] = prevNgram.split(" ");
		//loop until words are same.
		int length = Math.min(currWords.length, prevWords.length);
		for (int i = 0; i < length; i++) {
			if (currWords[i].equals(prevWords[i])) {
				lcp++;
			} else {
				break;
			}
		}
		return lcp;
	}

	private ArrayList<NGramCustomKey> getFrequencyForNgram(NGramCustomKey key,
			Iterable<Text> values) {

		ArrayList<NGramCustomKey> ngramSequence = new ArrayList<NGramCustomKey>();
		Iterator i = values.iterator();

		String prevNgram = "";
		String currNgram = "";
		int totalCount = 0;
		while (i.hasNext()) {
			String val = i.next().toString();
			currNgram = val.split("[|]")[0];
			int currCount = Integer.parseInt(val.split("[|]")[1]);

			if (currNgram.equals(prevNgram)) {
				totalCount = totalCount + currCount;
			} else {
				if (prevNgram.equals("")) {
					totalCount = currCount;
				} else {
					NGramCustomKey newObj = new NGramCustomKey(key.getPrefix(),
							new Text(prevNgram), new IntWritable(totalCount));
					ngramSequence.add(newObj);
					totalCount = currCount;
				}
				prevNgram = currNgram;
			}
			if (!i.hasNext()) {
				NGramCustomKey newObj = new NGramCustomKey(key.getPrefix(),
						new Text(currNgram), new IntWritable(totalCount));
				ngramSequence.add(newObj);
			}
		}
		return ngramSequence;
	}

}
