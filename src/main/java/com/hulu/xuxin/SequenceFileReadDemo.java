package com.hulu.xuxin;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReadDemo {

	private static void read(String uri) throws IOException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), configuration);
		Path path = new Path(uri);
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fs, path, configuration);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), configuration);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		} finally {
			IOUtils.closeStream(reader);
		}
	}
	
	public static void main(String[] args) throws IOException {
		read("output/sequence1");
		System.out.print("\n\n\n");
		read("output/sequence2");
	}

}
