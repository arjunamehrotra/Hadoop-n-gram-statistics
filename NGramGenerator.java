import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NGramGenerator {
	public static void main(String args[]){
		if(args.length!=2){
			System.err.print("Usage Patter: NGramGenerator <input file> <output file>");
		}
		
		Configuration conf = new Configuration();
		
		try {
			
			Job job = new Job(conf,"NGramGenerator");
			
			job.setJarByClass(NGramGenerator.class);

			job.setMapperClass(NGramMapper.class);
			job.setMapOutputKeyClass(NGramCustomKey.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setReducerClass(NGramReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setPartitionerClass(NaturalKeyPartitioner.class);
			job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
			job.setSortComparatorClass(CompositeKeyComparator.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			TextInputFormat.addInputPath(job, new Path(args[0] +"/"));
			TextOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setNumReduceTasks(1);
			
			job.waitForCompletion(true);
			
			
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
