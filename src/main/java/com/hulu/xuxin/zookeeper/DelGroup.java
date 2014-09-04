package com.hulu.xuxin.zookeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

public class DelGroup extends ZooKeeperInstance {

	public void delete(String groupPath) throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(groupPath, false);
		if (!children.isEmpty()) {
			for (String child : children)
				zk.delete(groupPath + "/" + child, -1);
		}
		zk.delete(groupPath, -1);
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		DelGroup dg = new DelGroup();
		dg.createZKInstance();
		dg.delete("/ZKGroup");
		dg.close();
	}

}
