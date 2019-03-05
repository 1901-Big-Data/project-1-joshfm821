package com.revature.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.revature.app.Driver;
import com.revature.map.gsMapper;
import com.revature.reduce.gsReducer;

public class Driver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: gsStudy <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job(getConf());
		//job.setNumReduceTasks(0); //set reducer to 0
		job.setJarByClass(Driver.class);
		job.setJobName("Gender Study");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(gsMapper.class);
		job.setReducerClass(gsReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		if (job.getCombinerClass() == null) {
			throw new Exception("Combiner not set");
		}
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
		System.exit(exitCode);
	}
}
