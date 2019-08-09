package com.diyo.mytest;

import java.io.IOException;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyCountWebpageNum extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MyCountWebpageNum(),args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
//		Admin admin = conn.getAdmin();
		Table intable = conn.getTable(TableName.valueOf("invertindex_result"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_count_table"));
        
        Job job = Job.getInstance(conf,"Fiyo_count_webpage_num");
        job.setJarByClass(MyCountWebpageNum.class);
        
        TableMapReduceUtil.initTableMapperJob(intable.getName(),new Scan(),CountMapper.class,
                Text.class,LongWritable.class,job);
        
        TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(),CountReducer.class,job);
        
        job.waitForCompletion(true);
        return 0;
    }
    public static class CountMapper extends TableMapper<Text,LongWritable>{
        @Override
        protected void map(ImmutableBytesWritable key,
                           Result value, Context context) throws IOException, InterruptedException {
            context.write(new Text("num"),new LongWritable(1));
        }
    }
    public static class CountReducer extends TableReducer<Text,LongWritable,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v:values) {
                sum += v.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("num"),Bytes.toBytes("num"),Bytes.toBytes(sum+""));
            context.write(NullWritable.get(),put);
        }
    }

}
