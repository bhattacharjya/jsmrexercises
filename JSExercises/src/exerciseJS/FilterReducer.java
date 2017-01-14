package exerciseJS;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<TextP, TextP, Text, Text> {

	private static List<String> tuples = new ArrayList<>();
	
	@Override
    protected void setup(Context context)
    {
        Configuration conf = context.getConfiguration();
        String tupleFile = conf.get("tupleFile");
        System.out.println("SETUP has been called to get interesting clicks from files  '" + tupleFile + "'");
        loadTuples(tupleFile);
    }
	
	@Override
	public void reduce(TextP key, Iterable<TextP> values, Context context)
			throws IOException, InterruptedException {
		
		// Loop for every pattern.
		// Invariant for key : It will be always be the user and timestamp first seen for her.
		
		for (String regex : tuples) { // Looping for each tuple
			
			TextP prevValue = new TextP();
		    for (TextP value : values) { // Loop over all the values for the user.
			    String url = value.getUrl().toString();
			    String prevUrl = prevValue.getUrl().toString();
			    if (Pattern.matches(regex, url)) {
			    	String finalKey = value.getTimeStamp().toString()
			    			          + "," + key.getUser().toString()
			    			          + "," + url
			    			          + "," + prevUrl;
			    	
			    	context.write(new Text(finalKey), new Text());
			    }
			    prevValue = value;
		    }
		}
	}
	
	private void loadTuples(String tupleFile)
    {

        String line;
        try {
            org.apache.hadoop.fs.Path tupleFilePath = new Path(tupleFile);
	        FileSystem fs = FileSystem.get(new Configuration());

            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(tupleFilePath)));
            while ((line=reader.readLine() ) != null)
            {
            	String[] words = line.trim().split(",");
                tuples.add(words[0]+words[1]);
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch( IOException e ) {
            e.printStackTrace();
        }
    }
}
