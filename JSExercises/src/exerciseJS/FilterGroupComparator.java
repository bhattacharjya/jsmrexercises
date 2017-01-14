package exerciseJS;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FilterGroupComparator extends WritableComparator{

	public FilterGroupComparator() {
		super(TextP.class, true);
	}
	
	@Override
	public int compare(WritableComparable first, WritableComparable second) {
		
		// We want to group only by user. So extract the user from the TextPairs.
		
		TextP firstPair = (TextP) first;
		TextP secondPair = (TextP) second;
		return firstPair.getUser().compareTo(secondPair.getUser());
	}
}