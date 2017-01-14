package exerciseJS;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, TextP, TextP> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String delimiter = "\t";
        String clickstream = value.toString();
        String[] clickstreamFields = clickstream.split(delimiter);
        
        // Generate a key = user and time. MR will sort it for us.
        // value will be the time and URL
        if (clickstreamFields.length == 3) {
               context.write(new TextP(clickstreamFields[1], clickstreamFields[0]), 
				             new TextP(clickstreamFields[2], clickstreamFields[0]));
        }
	}

}