package exerciseJS2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DomainStatGroupComparator extends WritableComparator{

	public DomainStatGroupComparator() {
		super(TextTriplet.class, true);
	}
	
	@Override
	public int compare(WritableComparable first, WritableComparable second) {
		
		// We want to group only by domain. So extract the domain from the TextTriplet
		
		TextTriplet firstPair = (TextTriplet) first;
		TextTriplet secondPair = (TextTriplet) second;
		return firstPair.getDomain().compareTo(secondPair.getDomain());
	}
}