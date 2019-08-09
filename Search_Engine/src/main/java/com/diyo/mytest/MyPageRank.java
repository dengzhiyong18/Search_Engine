package com.diyo.mytest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyPageRank extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
//		Admin admin = conn.getAdmin();
		Table intable = conn.getTable(TableName.valueOf("clean_webpage"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_rank_result"));
		
		Job job = Job.getInstance(conf, "Diyo_PageRank_ite0");
		job.setJarByClass(MyPageRank.class);
		TableMapReduceUtil.initTableMapperJob(intable.getName(), new Scan(), RankMapper.class, Text.class, Text.class,
				job);
		TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(), RankReducer.class, job);
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);

		for (int i = 1; i < 10; i++) {
			Configuration conf_ite = getConf();
			Job job_ite = Job.getInstance(conf_ite, "Diyo_PageRank_ite" + i);
			job_ite.setJarByClass(MyPageRank.class);
			TableMapReduceUtil.initTableMapperJob(outtable.getName(), new Scan(), RankMapper.class, Text.class, Text.class,
					job_ite);
			TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(), RankReducer.class, job_ite);
			job_ite.setNumReduceTasks(1);
			job_ite.waitForCompletion(true);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MyPageRank(), args);
	}

	/*
	 *
	 * 在此mapper中，将每个网址转化成平铺状态，并给出权重值 例如此结构，A 1 B,C,D A代表某一网址，1是自身所携带权重值，B,C,D是A网页的出链
	 * 在map方法中，把A 对于 B,C,D的 跳转的概率值计算出来，即 都是1/3 map的输出 为两部分 1，A 以及
	 * 出链继续原样输出，作为下一次迭代的原始数据 2，每个出链作为key，A网页对于此出链贡献的权重值 作为value输出
	 */
	public static class RankMapper extends TableMapper<Text, Text> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {

			String s = new String(key.get());
			// 得到常规uri格式
			String uri = showURI(s);
			if (uri != null) {
				uri = uri.trim();
				// 拿page:oln的值如果 为0 则代表没有外链 如果大于0 则代表有外链，进行不同处理
				byte[] num = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("oln"));
				// 当前页面外连接个数
				int i = 0;
				if (num != null) {
					i = Bytes.toInt(num);
				}
				// 设置每个网页初始权重值,或初始值为10
				byte[] rank_byte = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("rank"));
				;
				// 当前页面权重值
				double rank = 10;
				if (rank_byte != null) {
					rank = Bytes.toDouble(rank_byte);
				}
				// 声明外连接平铺字符串
				String outlinks = "";
				// i > 0 代表当前页面有外链接
				if (i > 0) {
					// 拿到所有外链接
					NavigableMap<byte[], byte[]> ols = value.getFamilyMap(Bytes.toBytes("ol"));
					// 获得ols中所有的外链，即key值
					Set<byte[]> olset = ols.keySet();
					// 计算每个外链得到的权重
					double score = rank / (double) olset.size();
					// 每个外链携带自己的分数进行输出
					for (byte[] ol : olset) {
						// 拼接外链字符串以做后用
						outlinks += Bytes.toString(ol) + ",";
						context.write(new Text(Bytes.toString(ol).trim()), new Text(score + ""));
					}
					// 去除拼接外链接 最后的逗号
					if (outlinks.endsWith(",")) {
						outlinks = outlinks.substring(0, outlinks.length() - 1).trim();
					}
					context.write(new Text(uri), new Text(outlinks));
				} else {
					// 对于没有外链的页面，即对于所有网页贡献皆为0
					context.write(new Text(uri), new Text(0 + ""));
				}
			}
		}

		// 把nutch所扒取的网址转化为常见格式
		public static String showURI(String s) {
			// com.redhat.access:https/security/vulnerabilities/3442151
			String[] ss = s.split(":");
			if (ss.length == 2) {
				String[] domain = ss[0].split("\\.");
				List<String> strings = Arrays.asList(domain);
				Collections.reverse(strings);
				String rs = "";
				for (String info : strings) {
					rs += info + ".";
				}
				rs = rs.substring(0, rs.length() - 1);
				String proto = ss[1].substring(0, ss[1].indexOf("/"));
				String res = ss[1].substring(ss[1].indexOf("/"), ss[1].length());
				return proto.trim() + "://" + rs.trim() + res.trim();
			} else {
				return null;
			}
		}
	}

	public static class RankReducer extends TableReducer<Text, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 障碍因子
			double factor = 0.85d;
			double sum = 0.0;
			// 拿到values，如果是空字符串 则不操作
			// 如果包含http 代表是map整合出来的 网页 和它出链的关系 正常输出
			// 如果不包含http 代表是 map计算出来的rank值，按照公式进行累加计算

			// 把key恢复成nutch采集出来的key的结构
			String k = key.toString();
			String uri = returnNutchKey(k);
			// 声明存到hbase所用的put
			Put put = new Put(Bytes.toBytes(uri));
			// 遍历所有的value，此集合中 有rank值，有 外链接字符串
			for (Text value : values) {
				String ctn = value.toString().trim();
				if (!ctn.equals("")) {
					// 如果value中有http 子串，代表是外链接
					if (ctn.indexOf("http") != -1) {
						// 通过逗号拆分外链接
						String[] ols = ctn.split(",");
						// 输出外链个数
						put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("oln"), Bytes.toBytes(ols.length));
						// 输出每个外链
						for (String ol : ols) {
							put.addColumn(Bytes.toBytes("ol"), Bytes.toBytes(ol), Bytes.toBytes("1"));
						}
					} else {
						// 累加当前页面获取的rank值
						double v = Double.parseDouble(ctn);
						sum += v;
					}
				}
			}
			double rank = sum * factor + (1 - factor);
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("rank"), Bytes.toBytes(rank));
			context.write(NullWritable.get(), put);
		}

		public static String returnNutchKey(String key) {
			// com.aliyun.bbs:http/
			// https://bbs.aliyun.com/thread/356.html
			String[] splits = key.split("://");
			String protocol = splits[0];
			String d = splits[1].substring(0, splits[1].indexOf("/"));
			String res = splits[1].substring(splits[1].indexOf("/"), splits[1].length());
			String[] domain = d.split("\\.");
			List<String> strings = Arrays.asList(domain);
			Collections.reverse(strings);
			String rs = "";
			for (String info : strings) {
				rs += info + ".";
			}
			rs = rs.substring(0, rs.length() - 1);
			return rs.trim() + ":" + protocol.trim() + res.trim();
		}
	}
}
