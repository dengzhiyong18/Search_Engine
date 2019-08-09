package com.diyo.mytest;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//此mapreduce程序，承接JoinCleanAndRank
//因为上一个mapreduce没法完成连接，故采用两个mapreduce
//左表clean_webpage 右表rank_result
// JoinCleanAndRank 做右外连接
// HoldJoinResult 将连接结果 列数不为2的剔除
// 得到的结果即为 左外连接结果
public class MyHoldJoinResult extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MyHoldJoinResult(), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
//		Admin admin = conn.getAdmin();
		Table intable = conn.getTable(TableName.valueOf("last_result"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_join_result"));
        
        Job job = Job.getInstance(conf, "Diyo_HoldJoinResult");
        job.setJarByClass(MyHoldJoinResult.class);
        TableMapReduceUtil.initTableMapperJob(intable.getName(),new Scan(),HJRMapper.class,
                ImmutableBytesWritable.class,MapWritable.class,job);
        TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(), HJRReducer.class, job);
        job.waitForCompletion(true);
        return 0;
    }

    public static class HJRMapper extends TableMapper<ImmutableBytesWritable, MapWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            NavigableMap<byte[], byte[]> qvs = value.getFamilyMap(Bytes.toBytes("page"));
            MapWritable map = new MapWritable();
            if (qvs.size() == 2) {
                byte[] rank = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("rank"));
                byte[] keyword = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("key"));
                if(rank != null && keyword != null){
                    map.put(new Text("rank"), new BytesWritable(rank));
                    map.put(new Text("key"), new BytesWritable(keyword));
                    context.write(key, map);
                }
            }
        }
    }


    public static class HJRReducer extends TableReducer<ImmutableBytesWritable, MapWritable, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(key.get());
            for (MapWritable value : values) {
                Set<Map.Entry<Writable, Writable>> entries = value.entrySet();
                for (Map.Entry<Writable, Writable> entry : entries) {
                    Text k = (Text) entry.getKey();
                    BytesWritable v = (BytesWritable)entry.getValue();
                    put.addColumn(Bytes.toBytes("page"),k.getBytes(),v.getBytes());
                }
            }
            context.write(NullWritable.get(), put);
        }
    }
}
