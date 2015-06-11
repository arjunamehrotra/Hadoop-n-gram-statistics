import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class NaturalKeyGroupingComparator extends WritableComparator {

	/**
	 * Constructor.
	 */
	protected NaturalKeyGroupingComparator() {
		super(NGramCustomKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		NGramCustomKey k1 = (NGramCustomKey)w1;
		NGramCustomKey k2 = (NGramCustomKey)w2;
		
		return k1.getPrefix().compareTo(k2.getPrefix());
	}
}