package com.hulu.xuxin.hadoop;

import java.io.IOException;
import java.net.URI;

import javax.print.attribute.standard.Compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;

public class SequenceFileWriteDemo {

	private static String[] myValue = {
		"hello world",
		"bye world",
		"hello hadoop",
		"bye hadoop"
	};
	
	private static void demo1() throws IOException {
		String uri = "output/sequence1";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, configuration, path, key.getClass(), value.getClass());
			final int count = 500;
			for (int i = 0; i < count; i++) {
				key.set(count - i);
				value.set(myValue[i % myValue.length]);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
	private static void demo2() throws IOException {
		String uri = "output/sequence2";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, configuration, path, key.getClass(), value.getClass(), CompressionType.BLOCK);
			final int count = 500;
			for (int i = 0; i < count; i++) {
				key.set(count - i);
				value.set(myValue[i % myValue.length]);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
	
	public static void main(String[] args) throws IOException {
		demo1();
		demo2();
	}

}
