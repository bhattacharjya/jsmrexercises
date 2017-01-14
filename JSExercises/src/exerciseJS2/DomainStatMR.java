package exerciseJS2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

/* 
 *  
 * If the click stream contains: 
 * 147489900,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://google.com/q=acme+deals 
 * 147490000,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://cooldeals.com/acme‐offer 
 * 147490014,03d79704‐84bd‐11e6‐88ff‐6c4008b1dad6,http://yahoo.com/ 
 * 147490080,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/specials 
 * 147490095,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/checkout 
 * 147490095,2985b1e8‐84bd‐11e6‐814c‐6c4008b1dad6,http://facebook.com/abc
 * 147490150,3e7d3e64‐84bc‐11e6‐b518‐6c4008b1dad6,http://acme.com/thank‐you 
 *  
 * Task 2. Domain Statistics 
 * We want to generate domain statistics for the biggest domains: for each day we calculate the number of 
 * pages views, number of unique visitors, and numbers of visits. (Visit is single browsing session, 
 * meaning the visitor used the site with no breaks longer than 30 minutes. A single visitor may have 
 * made multiple visits.) All these metrics can be estimated if it improves the performance (explain why if it 
 * is so). 
 
 * Write an implementation for this (again Map/Reduce, Spark, or anything else). 
 * How does it behave for largest domains and smallest domains? 
 */

public class DomainStatMR {

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
		job.setJarByClass(DomainStatMR.class);
		job.setJobName("JumpShot MapReduce DomainStat job");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(DomainStatMapper.class);
		job.setReducerClass(DomainStatReducer.class);
		// job.setCombinerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(TextTriplet.class);
		job.setMapOutputValueClass(TextTriplet.class);
		job.setPartitionerClass(DomainStatPartitioner.class);
		job.setGroupingComparatorClass(DomainStatGroupComparator.class);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


