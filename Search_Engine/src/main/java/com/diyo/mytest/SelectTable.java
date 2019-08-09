package com.diyo.mytest;

import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SelectTable {
	public static void main(String[] args) throws Exception{
		select();
	}

	public static void select() throws Exception {

		// 获取HBase配置配置对象
		Configuration conf = HBaseConfiguration.create();

		// 增加配置项
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");

//		Admin admin = conn.getAdmin();
		Table table = conn.getTable(TableName.valueOf("aliyun_webpage"));

		System.out.println("开始查询");
		Get get = new Get(Bytes.toBytes("com.aliyun.bbs:https/read/577771.html"));
		// 可以理解成数组 cell
		Result result = table.get(get);
		showResult(result);
		System.out.println("结束查询");
	}
	
	public static void showResult(Result result) {
		System.out.println("结果中cell的个数：" + result.size());
		System.out.println("结果中是否包含某个列：" + result.containsColumn(Bytes.toBytes("f"), Bytes.toBytes("name")));
		System.out.println("结果中是否包含空列：" + result.containsEmptyColumn(Bytes.toBytes("f"), Bytes.toBytes("name")));
		System.out.println("获得某列下的数据" + result.getValue(Bytes.toBytes("f"), Bytes.toBytes("name")));
		System.out.println("結果中的第一行数据"+Bytes.toString(result.value()));
		System.out.println("---------------------------");
		NavigableMap<byte[], byte[]> map1 = result.getFamilyMap(Bytes.toBytes("f"));
		map1.forEach((k,v) ->{
			System.out.println(Bytes.toString(k) + " " + Bytes.toString(v));
		});
		System.out.println("---------------------------");
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cqtvs = result.getMap();
		for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> cqtvEntry : cqtvs.entrySet()) {
			byte[] c = cqtvEntry.getKey();
			NavigableMap<byte[], NavigableMap<Long, byte[]>> qtvs = cqtvEntry.getValue();
			
			for (Entry<byte[], NavigableMap<Long, byte[]>>  qtv: qtvs.entrySet()) {
				byte[] q = qtv.getKey();
				NavigableMap<Long, byte[]> vs = qtv.getValue();
				
				for ( Entry<Long, byte[]> tv: vs.entrySet()) {
					Long t = tv.getKey();
					byte[] v = tv.getValue();
					System.out.println("列族："+Bytes.toString(c)+" 列名："+Bytes.toString(q)+" 版本号："+t+" 值："+Bytes.toString(v));
				}
			}
		}
		
		
		System.out.println("===========================");
	}
	
}
