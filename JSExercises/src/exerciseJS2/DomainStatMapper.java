package exerciseJS2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DomainStatMapper extends Mapper<LongWritable, Text, TextTriplet, TextTriplet> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String delimiter = "\t";
        String clickstream = value.toString();
        String[] clickstreamFields = clickstream.split(delimiter);
        
        /* I am skipping out on data cleansing here, assuming input is perfect.
         * I won't validate the fields for example.
         */
        
        // Generate a key = user and time. MR will sort it for us.
        // value will be the time and URL
        if (clickstreamFields.length == 3) {
               context.write(new TextTriplet(clickstreamFields[2], clickstreamFields[1], clickstreamFields[0]), 
				             new TextTriplet("", clickstreamFields[1], clickstreamFields[0]));
        }
	}

}