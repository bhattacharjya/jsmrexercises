package exerciseJS;

import org.apache.hadoop.mapreduce.Partitioner;

public class FilterPartitioner extends Partitioner<TextP, TextP> {

	@Override
	public int getPartition(TextP key, TextP value, int numReduceTasks) {
		
		String keyStr = key.getUser().toString();
		
		return ((keyStr.hashCode() & Integer.MAX_VALUE) % numReduceTasks);
		
	}

}
