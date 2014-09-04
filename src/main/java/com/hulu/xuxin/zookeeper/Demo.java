package com.hulu.xuxin.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

public class Demo {

	private static final int SESSION_TIMEOUT = 30000;
	ZooKeeper zk;
	
	Watcher wh = new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println(event.toString());
			}
		};
	
	private void createZKInstance() throws IOException {
		zk = new ZooKeeper("localhost:2181", SESSION_TIMEOUT, wh);
	}
	
	private void ZKOperations() throws IOException, InterruptedException, KeeperException {
		System.out.println("\n1. 创建 ZooKeeper 节点 (znode: z002, 数据: myData2, 权限OPEN_ACL_UNSAFE, 节点类型: Persistent)");
		System.out.println(zk.exists("/zoo2", false) == null);
		zk.create("/zoo2", "myData2".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		System.in.read();
		System.out.println("\n2. 查看是否创建成功：");
		System.out.println(new String(zk.getData("/zoo2", false, null)));

		System.in.read();
		System.out.println("\n3. 修改节点数据");
		zk.setData("/zoo2", "shenlan211314".getBytes(), -1);

		System.in.read();
		System.out.println("\n4. 查看是否修改成功：");
		System.out.println(new String(zk.getData("/zoo2", false, null)));

		System.in.read();
		System.out.println("\n5. 删除节点");
		zk.delete("/zoo2", -1);
		
		System.out.println("\n6. 查看是否被删除：");
		System.out.println("节点状态: [" + zk.exists("/zoo2", false) + "]");
	}
	
	private void close() throws InterruptedException {
		zk.close();
	}
		
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		Demo d = new Demo();
		d.createZKInstance();
		d.ZKOperations();
		d.close();
	}
}
