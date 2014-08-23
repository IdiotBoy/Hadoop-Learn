package com.hulu.xuxin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileWriteFile {

	private static String[] myValue = {
		"hello world",
		"bye world",
		"hello hadoop",
		"bye hadoop"
	};
	
	public static void main(String[] args) throws IOException {
		String uri = "output/mapFile";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		try {
			writer = new MapFile.Writer(configuration, fs, uri, key.getClass(), value.getClass());
			final int count = 500;
			for (int i = 0; i < count; i++) {
				key.set(i);
				value.set(myValue[i % myValue.length]);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

}
