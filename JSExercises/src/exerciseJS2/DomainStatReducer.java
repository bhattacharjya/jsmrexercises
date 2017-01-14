package exerciseJS2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DomainStatReducer extends Reducer<TextTriplet, TextTriplet, Text, Text> {
	
	static final int DELTA = 1800; // 30 minutes == 1800 seconds,
	
	@Override
	public void reduce(TextTriplet key, Iterable<TextTriplet> values, Context context)
			throws IOException, InterruptedException {
		
		// Invariant for key : It will be always be the domain, first user and the first timestamp
		// seen for her.
		Text domain = key.getDomain();
		long totalViews = 0;
		long totalVisits = 0;
		long numberofVisitors = 0;
		
		String prevUser=""; 
		long prevUserTS; 
		
		for (TextTriplet value : values) { // Loop over all the values for the domain.
			totalViews++;
			
			// Now because we did secondary sorting, we have values sorted by "", user and timestamp, 
			// effectively - user and timestamp since "" is same for all.
			
			String user = value.getUser().toString();
			prevUserTS = Long.parseLong(value.getTimeStamp().toString());
			
			if (user != prevUser) { // We have new unique visitor. Increment number of Unique Visitors
				prevUser = user;
				numberofVisitors++;
				totalVisits++;
				continue;
			}
			long visitTS = Long.parseLong(value.getTimeStamp().toString());
			if ((visitTS - prevUserTS) < DELTA) { // Since time gap is less than 30m, 
				continue;                         // It is considered part of one visit.
			}
			totalVisits++;
			
		}
		
		context.write(new Text(domain), new Text(Long.toString(totalViews) + ","
				                                + Long.toString(numberofVisitors)
				                                +"," + Long.toString(totalVisits)) );
	}
	
	
}
