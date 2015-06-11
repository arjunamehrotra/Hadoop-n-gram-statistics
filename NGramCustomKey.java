import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;



public class NGramCustomKey implements WritableComparable<NGramCustomKey>{

	private Text prefix;
	private Text ngram;
	private IntWritable count;
	
	
	public NGramCustomKey() {
		
		this.prefix = new Text();
		this.ngram 	= new Text();
		this.count 	= new IntWritable();
	}
	
	public NGramCustomKey(Text prefix, Text ngram, IntWritable count) {
		this.prefix = prefix;
		this.ngram = ngram;
		this.count = count;
	}

	public Text getPrefix() {
		return prefix;
	}

	public void setPrefix(Text prefix) {
		this.prefix = prefix;
	}

	public Text getNgram() {
		return ngram;
	}

	public void setNgram(Text ngram) {
		this.ngram = ngram;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		
		prefix.readFields(arg0);
		ngram.readFields(arg0);
		count.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		
		prefix.write(arg0);
		ngram.write(arg0);
		count.write(arg0);
		
	}

	@Override
	public int compareTo(NGramCustomKey o) {
		// TODO Auto-generated method stub
		
		int result = this.getPrefix().toString().compareTo(o.getPrefix().toString());
		if (result == 0){
			result = -1*this.getNgram().toString().compareTo(o.getNgram().toString());
		}
		return result;
	}

	@Override
	public String toString() {
		return "NGramCustomKey [prefix=" + prefix + ", ngram=" + ngram
				+ ", count=" + count + "]";
	}

}
