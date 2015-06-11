import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class NaturalKeyPartitioner extends Partitioner<NGramCustomKey, Text> {

	@Override
	public int getPartition(NGramCustomKey key, Text val, int numPartitions) {
		int hash = key.getPrefix().hashCode();
		int partition = Math.abs(hash % numPartitions);
		return partition;
	}

}
