package com.hulu.xuxin.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileReadFile {

	private static void read(String uri) throws IOException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		MapFile.Reader reader = null;
		try {
			reader = new MapFile.Reader(fs, uri, configuration);
			WritableComparable key = (WritableComparable)ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), configuration);
			while (reader.next(key, value)) {
				System.out.printf("%s\t%s\n", key, value);
			}
			reader.get(new IntWritable(7), value);
			System.out.printf("%s\n", value);
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	public static void main(String[] args) throws IOException {
		read("output/mapFile");
	}

}