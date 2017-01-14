package exerciseJS2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextTriplet implements WritableComparable<TextTriplet> {

	private Text domain;
	private Text user;
	private Text ts;
	
	public TextTriplet() {
		domain = new Text();
		user = new Text();
		ts = new Text();
	}
	
	public TextTriplet(Text domain, Text user, Text ts) {
		this.domain = domain;
		this.user = user;
		this.ts = ts;
	}
	
	public TextTriplet(String domain, String user, String ts) {
		this.domain = new Text(domain);
		this.user = new Text(user);
		this.ts = new Text(ts);
	}
	
	public Text getDomain() {
		return domain;
	}
	
	public Text getUser() {
		return user;
	}
	
	public Text getTimeStamp(){
		return ts;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		domain.readFields(in);
		user.readFields(in);
		ts.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		domain.write(out);
		user.write(out);
		ts.write(out);
	}

	@Override
	public int compareTo(TextTriplet other) {
		int compare = domain.compareTo(other.domain);
		if (compare != 0) {
			return compare;
		}
		compare = user.compareTo(other.user);
		if (compare != 0) {
			return compare;
		} 
		
		return ts.compareTo(other.ts);
		
	}
	
	@Override
	public String toString(){
		
		return domain.toString() + "," + user.toString() + "," + ts.toString();
	}
	// we won't override hashCode and equals as we don't plan 
	// on using the default HashPartitioner.

}
