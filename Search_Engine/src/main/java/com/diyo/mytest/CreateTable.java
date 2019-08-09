package com.diyo.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class CreateTable {
	public static void main(String[] args) throws Exception{
		createTable("Diyo_searchEngine:mytest_count_table");
	}

	public static void createTable(String tableName) throws Exception{
		// 获取HBase配置配置对象
		Configuration conf = HBaseConfiguration.create();

		// 增加配置项
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");
		
		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
		
		Admin admin = conn.getAdmin();
		
		HTableDescriptor desc  = new HTableDescriptor(TableName.valueOf(tableName));
		/*
		<table name="webpage">
        <family name="p" maxVersions="1"/>
        <family name="f" maxVersions="1"/>
        <family name="s" maxVersions="1"/>
        <family name="il" maxVersions="1"/>
        <family name="ol" maxVersions="1"/>
        <family name="h" maxVersions="1"/>
        <family name="mtdt" maxVersions="1"/>
        <family name="mk" maxVersions="1"/>
    	</table>
		 * */
//		String f = "f";
//		String p = "p";
//		String s = "s";
//		String il = "il";
//		String ol = "ol";
//		String h = "h";
//		String mtdt = "mtdt";
//		String mk = "mk";
//		String page = "page";
		String num = "num";
		
//		HColumnDescriptor familyName1 = new HColumnDescriptor(f);
//		HColumnDescriptor familyName2 = new HColumnDescriptor(p);
//		HColumnDescriptor familyName3 = new HColumnDescriptor(s);
//		HColumnDescriptor familyName4 = new HColumnDescriptor(il);
//		HColumnDescriptor familyName5 = new HColumnDescriptor(ol);
//		HColumnDescriptor familyName6 = new HColumnDescriptor(h);
//		HColumnDescriptor familyName7 = new HColumnDescriptor(mtdt);
//		HColumnDescriptor familyName8 = new HColumnDescriptor(mk);
//		HColumnDescriptor familyName9 = new HColumnDescriptor(page);
		HColumnDescriptor familyName10 = new HColumnDescriptor(num);
//		desc.addFamily(familyName1);
//		desc.addFamily(familyName2);
//		desc.addFamily(familyName3);
//		desc.addFamily(familyName4);
//		desc.addFamily(familyName5);
//		desc.addFamily(familyName6);
//		desc.addFamily(familyName7);
//		desc.addFamily(familyName8);
//		desc.addFamily(familyName9);
		desc.addFamily(familyName10);

		admin.createTable(desc);
	
		System.out.println("创建表成功：" + tableName);
	
		
	}
}
