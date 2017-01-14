package exerciseJS;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextP implements WritableComparable<TextP> {

	private Text userOrUrl;
	private Text ts;
	
	public TextP() { 
		userOrUrl = new Text();
		ts = new Text();
	}
	
	public TextP(Text user, Text ts) {
		this.userOrUrl = user;
		this.ts = ts;
	}
	
	public TextP(String user, String ts) {
		this.userOrUrl = new Text(user);
		this.ts = new Text(ts);
	}
	
	public Text getUser() {
		return userOrUrl;
	}
	
	public Text getUrl() {
		return userOrUrl;
	}
	
	public Text getTimeStamp(){
		return ts;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		userOrUrl.readFields(in);
		ts.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		userOrUrl.write(out);
		ts.write(out);
	}

	@Override
	public int compareTo(TextP other) {
		int compare = userOrUrl.compareTo(other.userOrUrl);
		if (compare != 0) {
			return compare;
		} else {
		    return ts.compareTo(other.ts);
		}
	}
	
	@Override
	public String toString(){
		String retval = null;
		if (ts != null) {
			retval = ts.toString() + ",";
		}
		if (userOrUrl != null) {
			retval = retval + userOrUrl.toString();
		}
		return retval;
	}
	// we won't override hashCode and equals as we don't plan 
	// on using the default HashPartitioner.

}
