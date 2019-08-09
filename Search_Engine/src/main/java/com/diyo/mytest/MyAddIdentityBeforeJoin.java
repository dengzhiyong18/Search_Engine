package com.diyo.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 为了链接查询方便识别  为clean_webpage 和 rank_result添加标识
 *
 * */
public class MyAddIdentityBeforeJoin {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        //连接zookeeper集群
        conf.set("hbase.zookeeper.quorum",
                "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");
        System.out.println(conf);
        //获得连接对象
        Connection conn = ConnectionFactory.createConnection(conf);

        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_rank_result"));
        ResultScanner scanner = table.getScanner(scan);
        List<Put> puts = new ArrayList<>();
        for (Result result : scanner) {
            Put put = new Put(result.getRow());
            put.addColumn(Bytes.toBytes("page"),Bytes.toBytes("i"),
                    Bytes.toBytes("b"));
            puts.add(put);
        }
        table.put(puts);
    }
}
