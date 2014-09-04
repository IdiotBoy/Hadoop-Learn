package com.hulu.xuxin.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MTJoin {
	
	public static class MTMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String line = value.toString();
			
			if (line.charAt(line.length() - 1) <= '9' && line.charAt(line.length() - 1) >= '0') {
				String number = line.substring(line.length() - 1);
				String factory = line.substring(0, line.length() - 2);
				context.write(new Text(number), new Text("1+" + factory));
			} else if (line.charAt(0) <= '9' && line.charAt(0) >= '0') {
				String number = line.substring(0, 1);
				String city = line.substring(2);
				context.write(new Text(number), new Text("2+" + city));
			} 	
		}
	}
	
	public static class MTReducer extends Reducer<Text, Text, Text, Text> {
		private int time = 0;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (time == 0) {
				context.write(new Text("factoryname"), new Text("addressname"));
				time++;
			}
			
			ArrayList<String> factory = new ArrayList<String>();
			ArrayList<String> address = new ArrayList<String>();
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				String record = iter.next().toString();
				char type = record.charAt(0);
				if (type == '1')
					factory.add(record.substring(2));
				else 
					address.add(record.substring(2));
			}
			for (int i = 0; i < factory.size(); i++) {
				for (int j = 0; j < address.size(); j++) {
					context.write(new Text(factory.get(i)), new Text(address.get(j)));
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MTJoin <in> <out>");
			System.exit(2);
		}

		Job job = new Job(configuration, "MTJoin");
		job.setJarByClass(MTJoin.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(MTMapper.class);
		job.setReducerClass(MTReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
	
}
