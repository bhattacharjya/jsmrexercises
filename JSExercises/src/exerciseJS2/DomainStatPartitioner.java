package exerciseJS2;

import org.apache.hadoop.mapreduce.Partitioner;

public class DomainStatPartitioner extends Partitioner<TextTriplet, TextTriplet> {

	@Override
	public int getPartition(TextTriplet key, TextTriplet value, int numReduceTasks) {
		
		String keyStr = key.getDomain().toString();
		
		return ((keyStr.hashCode() & Integer.MAX_VALUE) % numReduceTasks);
		
	}

}
