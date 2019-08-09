package com.diyo.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor.Builder;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateNameSpace {
	public static void main(String[] args) throws Exception {
		createNameSpace("Diyo_searchEngine");
	}
	public static void createNameSpace(String nameSpace) throws Exception {
		//获取HBase配置配置对象
		Configuration conf = HBaseConfiguration.create();
		
		//增加配置项
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");
		
		//获取连接对象
		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
		
		Admin admin = conn.getAdmin();
		
		Builder builder = NamespaceDescriptor.create(nameSpace);
		NamespaceDescriptor desc = builder.build();
		admin.createNamespace(desc);
		
		System.out.println("命名空间创建成功:"+nameSpace);
	}
}
