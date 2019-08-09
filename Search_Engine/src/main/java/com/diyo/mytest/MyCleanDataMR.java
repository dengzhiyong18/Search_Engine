package com.diyo.mytest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 输入: 172.16.0.4:16010 aliyun_webpage/huawei_webpage
 * zk:172.16.0.4,172.16.0.5,172.16.0.6,172.16.0.7
 *
 * 输出: 名字_类名 Diyo_CleanDataMR 数据清洗 从抓取过的页面中（f:st == 2） 从提取有效字段 （url,title,il,ol）
 *
 * 建表 用hbase api做 172.16.0.4 computer1.cloud.briup.com 172.16.0.5
 * computer2.cloud.briup.com 172.16.0.6 computer3.cloud.briup.com 172.16.0.7
 * computer4.cloud.briup.com
 */
public class MyCleanDataMR {

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
		Admin admin = conn.getAdmin();
		Table intable = conn.getTable(TableName.valueOf("aliyun_webpage"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_CleanDataMR"));
		
	
		Job job = Job.getInstance(conf,"Diyo_CleanData");
		job.setJarByClass(MyCleanDataMR.class);

//		String intable = conf.get("huawei_webpage");
//		String outtable = conf.get("Diyo_searchEngine:mytest_CleanDataMR");

/*		TableMapReduceUtil.initTableMapperJob(intable, new Scan(), CDMapper.class, ImmutableBytesWritable.class,
				MapWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(outtable, CDReducer.class, job);*/
		TableMapReduceUtil.initTableMapperJob(intable.getName(),new Scan(), CDMapper.class, ImmutableBytesWritable.class,
				MapWritable.class, job);
		TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(), CDReducer.class, job);

		
		job.waitForCompletion(true);
	}

	public static class CDMapper extends TableMapper<ImmutableBytesWritable, MapWritable> {

		// 接收清洗后数据的map
		MapWritable map_clean = new MapWritable();

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, MapWritable>.Context context)
				throws IOException, InterruptedException {
//			判断 value  f列族   st列 的值不是2（fetched状态） 则弃用
//			如果是2 进一步

			// 获取爬取的数据状态
			Cell fetched_status = value.getColumnLatestCell(Bytes.toBytes("f"), Bytes.toBytes("st"));
			byte[] f_s_value = Arrays.copyOfRange(fetched_status.getValueArray(), fetched_status.getValueOffset(),
					fetched_status.getValueArray().length);

			// 如果爬取状态为2，则有效
			if (Bytes.toInt(f_s_value) == 2 || Bytes.toInt(f_s_value) == 5) {

				// 从提取有效字段 （url,title,il,ol）

				// 统计ol的行数
				int ol_num = countQualifierNum(value, Bytes.toBytes("ol"));
				// 统计il的行数
				int il_num = countQualifierNum(value, Bytes.toBytes("il"));

				// 获得解析到网页内容
				byte[] page_content = getValueByFamilyAndQualifier(value, Bytes.toBytes("p"), Bytes.toBytes("c"));
				// 获得解析到的标题
				byte[] page_title = getValueByFamilyAndQualifier(value, Bytes.toBytes("p"), Bytes.toBytes("t"));
				// 获得nutch为当前url给出的分值
				byte[] score = getValueByFamilyAndQualifier(value, Bytes.toBytes("s"), Bytes.toBytes("s"));

				// 拼接数据进入reduce
				map_clean.put(new BytesWritable(Bytes.toBytes("oln")), new BytesWritable(Bytes.toBytes(ol_num)));
				map_clean.put(new BytesWritable(Bytes.toBytes("iln")), new BytesWritable(Bytes.toBytes(il_num)));

				// 入链列表
				MapWritable il_list = getWritebleQVMap(value, Bytes.toBytes("il"));
				// 出链列表
				MapWritable ol_list = getWritebleQVMap(value, Bytes.toBytes("ol"));

				map_clean.put(new BytesWritable(Bytes.toBytes("oln_list")), ol_list);
				map_clean.put(new BytesWritable(Bytes.toBytes("iln_list")), il_list);

				map_clean.put(new BytesWritable(Bytes.toBytes("t")), new BytesWritable(page_title));
				map_clean.put(new BytesWritable(Bytes.toBytes("cnt")), new BytesWritable(page_content));
				map_clean.put(new BytesWritable(Bytes.toBytes("s")), new BytesWritable(score));
				context.write(key, map_clean);

			}

		}

		private int countQualifierNum(Result value, byte[] family) {
			NavigableMap<byte[], byte[]> qvs = value.getFamilyMap(family);
			return qvs.size();
		}

		private byte[] getValueByFamilyAndQualifier(Result value, byte[] family, byte[] qualifier) {
			Cell cell = value.getColumnLatestCell(family, qualifier);
			if (cell == null) {
				return Bytes.toBytes(0);
			}
			return Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueArray().length);
		}

		private MapWritable getWritebleQVMap(Result value, byte[] family) {
			MapWritable map = new MapWritable();
			NavigableMap<byte[], byte[]> qvs = value.getFamilyMap(family);
			for (Entry<byte[], byte[]> qv : qvs.entrySet()) {
				byte[] q = qv.getKey();
				byte[] v = qv.getValue();
				map.put(new BytesWritable(q), new BytesWritable(v));
			}
			return map;
		}
	}

	public static class CDReducer extends TableReducer<ImmutableBytesWritable, MapWritable, NullWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values,
				Reducer<ImmutableBytesWritable, MapWritable, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			MapWritable map_clean = values.iterator().next();
			Put put = new Put(key.get());
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("oln"),
					((BytesWritable) map_clean.get(new BytesWritable(Bytes.toBytes("oln")))).getBytes());
			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("iln"),
					((BytesWritable) map_clean.get(new BytesWritable(Bytes.toBytes("iln")))).getBytes());

			MapWritable oln_list = (MapWritable) map_clean.get(new BytesWritable(Bytes.toBytes("oln_list")));
			for (Entry<Writable, Writable> qvs : oln_list.entrySet()) {
				put.addColumn(Bytes.toBytes("ol"), ((BytesWritable) qvs.getKey()).getBytes(),
						((BytesWritable) qvs.getValue()).getBytes());
			}

			MapWritable iln_list = (MapWritable) map_clean.get(new BytesWritable(Bytes.toBytes("iln_list")));
			for (Entry<Writable, Writable> qvs : iln_list.entrySet()) {
				put.addColumn(Bytes.toBytes("il"), ((BytesWritable) qvs.getKey()).getBytes(),
						((BytesWritable) qvs.getValue()).getBytes());
			}

			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("t"),
					((BytesWritable) map_clean.get(new BytesWritable(Bytes.toBytes("t")))).getBytes());

			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("s"),
					((BytesWritable) map_clean.get(new BytesWritable(Bytes.toBytes("s")))).getBytes());

			put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("cnt"),
					((BytesWritable) map_clean.get(new BytesWritable(Bytes.toBytes("cnt")))).getBytes());

			context.write(NullWritable.get(), put);
		}
	}
}
