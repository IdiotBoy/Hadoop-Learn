package com.hulu.xuxin.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class JoinGroup extends ZooKeeperInstance {

	public int join(String groupPath, int k) throws KeeperException, InterruptedException {
		String child = k + "";
		child = "child_" + child;
		
		String path = groupPath + "/" + child;
		if (zk.exists(groupPath, true) != null) {
			zk.create(path, child.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return 1;
		} else {
			System.out.println("组不存在！");
			return 0;
		}
	}
	
	public void multiJoin() throws KeeperException, InterruptedException {
		for (int i = 0; i < 10; i++) {
			int k = join("/ZKGroup", i);
			if (0 == k)
				System.exit(1);
		}
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		System.out.println("1");
		JoinGroup jg = new JoinGroup();
		jg.createZKInstance();
		jg.multiJoin();
		jg.close();
	}

}
