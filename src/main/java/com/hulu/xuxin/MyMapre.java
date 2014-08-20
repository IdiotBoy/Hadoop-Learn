package com.hulu.xuxin;

import org.apache.hadoop.io.*;
 
public class MyMapre {

	public static void main(String[] args) {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";
		Text t = new Text(s);
		
		System.out.println(s.length() + "\t" + t.getLength());
		System.out.println(s.indexOf("\u0041") + "\t" + t.find("\u0041"));
		System.out.println(s.indexOf("\u00DF") + "\t" + t.find("\u00DF"));
		System.out.println(s.indexOf("\u6771") + "\t" + t.find("\u6771"));
		System.out.println(s.indexOf("\uD801\uDC00") + "\t" + t.find("\uD801\uDC00"));
	}

}
