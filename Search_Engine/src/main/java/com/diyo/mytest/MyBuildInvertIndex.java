package com.diyo.mytest;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 此mapreduce进行倒置索引，前置表 join_result 结果 join_result
 * 
 * page:key page:rank page:cnt
 *
 * rowkey page keyword uri:rank
 */
public class MyBuildInvertIndex extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
//		Admin admin = conn.getAdmin();
		Table intable = conn.getTable(TableName.valueOf("join_result"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_invertindex_result"));

		Job job = Job.getInstance(conf, "Diyo_BuildInvertIndex");
		job.setJarByClass(MyBuildInvertIndex.class);
		TableMapReduceUtil.initTableMapperJob(intable.getName(), new Scan(), BIIMapper.class, ImmutableBytesWritable.class,
				Text.class, job);
		TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(), BIIReducer.class, job);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new MyBuildInvertIndex(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class BIIMapper extends TableMapper<ImmutableBytesWritable, Text> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String uri = new String(key.get()).trim();
			byte[] keyword = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("key"));
			// String kw = new String(keyword).trim();
			byte[] rank = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("rank"));
			double r = Bytes.toDouble(rank);
			context.write(new ImmutableBytesWritable(keyword), new Text(r + "," + uri));
		}
	}

	public static class BIIReducer extends TableReducer<ImmutableBytesWritable, Text, NullWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String kw = new String(key.get()).trim();
			if (kw.length() != 0) {
				TreeSet<String> set = new TreeSet<>(new Comparator<String>() {
					@Override
					public int compare(String o1, String o2) {
						String[] s1 = o1.split(",");
						String[] s2 = o2.split(",");
						double r1 = Double.parseDouble(s1[0]);
						double r2 = Double.parseDouble(s2[0]);
						return (int) (r2 * 10 - r1 * 10);
					}
				});
				for (Text value : values) {
					set.add(value.toString());
				}
				Put put = new Put(key.get());
				for (String s : set) {
					String[] split = s.split(",");
					String uri = split[1];
					double rank = Double.parseDouble(split[0]);
					put.addColumn(Bytes.toBytes("page"), Bytes.toBytes(uri), Bytes.toBytes(rank));
				}
				context.write(NullWritable.get(), put);
			}
		}
	}
}
