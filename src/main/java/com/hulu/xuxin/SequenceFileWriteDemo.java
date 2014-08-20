package com.hulu.xuxin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class SequenceFileWriteDemo {

	private static String[] myValue = {
		"hello world",
		"bye world",
		"hello hadoop",
		"bye hadoop"
	};
	
	public static void main(String[] args) throws IOException {
		String uri = "sequece";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, configuration, path, key.getClass(),value.getClass());
			final int count = 50000;
			for (int i = 0; i < count; i++) {
				key.set(count - i);
				value.set(myValue[i % myValue.length]);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

}
