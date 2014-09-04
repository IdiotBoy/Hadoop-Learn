package com.hulu.xuxin.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.*;

public class ZooKeeperInstance {

	public static final int SESSION_TIMEOUT = 30000;
	ZooKeeper zk;

	Watcher wh = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println(event.toString());
			}
		};
	
	public void createZKInstance() throws IOException {
		zk = new ZooKeeper("localhost:2181", SESSION_TIMEOUT, wh);
	}

	public void close() throws InterruptedException {
		zk.close();
	}
}
