import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CompositeKeyComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected CompositeKeyComparator() {
		super(NGramCustomKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		NGramCustomKey k1 = (NGramCustomKey)w1;
		NGramCustomKey k2 = (NGramCustomKey)w2;
		
		int result = k1.getPrefix().toString().compareTo(k2.getPrefix().toString());
		if (result == 0){
			result = -1*k1.getNgram().toString().compareTo(k2.getNgram().toString());
		}
		return result;
	}
}