package exerciseJS;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

/*
 * For this task we have a set of tuples (domain,path_regexp) describing interesting clicks. The goal is 
 * to create a filtered output containing just the interesting clicks in the following form: 
 * clicktime,userid,URL,reffererURL 
 
 * First three output fields match the fields in input records; the record is included in the output if the URL 
 * can be matched to one of tuples, i.e. its domain is equal and path matches to regular expression. 
 * reffererURL contains last URL of the same user that has a different domain. 
 *  
 * For example if the clickstream contains: 
 * 147489900,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://google.com/q=acme+deals 
 * 147490000,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://cooldeals.com/acme‐offer 
 * 147490014,03d79704‐84bd‐11e6‐88ff‐6c4008b1dad6,http://yahoo.com/ 
 * 147490080,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/specials 
 * 147490095,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/checkout 
 * 147490095,2985b1e8‐84bd‐11e6‐814c‐6c4008b1dad6,http://facebook.com/abc
 * 147490150,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/thank‐you 
 *  
 * And one of our interesting click tuples is (‘acme.com’,’/thank‐you’) 
 * Then the output will contain: 
 * 147490150,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/thank‐you,http://cooldeals.com/acme‐offer 
 *  
 * Write an implementation for this (Map/Reduce, Spark, or anything else). 
 * How can we scale this with number of clicks / with number of matching tuples?
 */

public class FilterMR {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 3) {
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		if (args[2] != null) {
	        System.err.println("tupleFile = " + args[2]);
	        conf.set("tupleFile", args[2]);
	    }
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FilterMR.class);
		job.setJobName("JumpShot MapReduce Filter job");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);
		// job.setCombinerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TextP.class);
		job.setMapOutputValueClass(TextP.class);
		job.setPartitionerClass(FilterPartitioner.class);
		job.setGroupingComparatorClass(FilterGroupComparator.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


