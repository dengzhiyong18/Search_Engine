package com.diyo.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanTable {
	public static void main(String[] args) throws Exception{
		scanTable("Diyo_searchEngine:mytest_invertindex_result");
	}

	@SuppressWarnings("deprecation")
	public static void scanTable(String tableName) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		// 增加配置项
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");

		Table table = conn.getTable(TableName.valueOf(tableName));

		Scan scan = new Scan();

		ResultScanner rs = table.getScanner(scan);

		for (Result r : rs) {
			for (KeyValue kv : r.raw()) {
				System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
						Bytes.toString(kv.getRow()), Bytes.toString(kv.getFamily()), Bytes.toString(kv.getQualifier()),
						Bytes.toString(kv.getValue()), kv.getTimestamp()));
			}
		}
	}

}
