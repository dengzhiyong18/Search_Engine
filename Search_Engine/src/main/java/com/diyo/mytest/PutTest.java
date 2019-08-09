package com.diyo.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PutTest {
	public static void main(String[] args) throws Exception {
		insert();
	}

	public static void insert() throws Exception{

		Configuration conf = HBaseConfiguration.create();

		// 增加配置项
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf("table_info"));
		Put put = new Put(Bytes.toBytes("Diyo_searchEngine:mytest_invertindex_result"));
		put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("owner"),Bytes.toBytes("Diyo"));
		table.put(put);
		System.out.println("插入成功");
	}
}
