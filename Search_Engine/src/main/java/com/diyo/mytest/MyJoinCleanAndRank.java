package com.diyo.mytest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJoinCleanAndRank extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MyJoinCleanAndRank(), args);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
//		Admin admin = conn.getAdmin();
		Table intable1 = conn.getTable(TableName.valueOf("clean_webpage"));
		Table intable2 = conn.getTable(TableName.valueOf("rank_result"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_last_result"));
        
        Job job = Job.getInstance(conf, "JoinCleanAndRank");
        job.setJarByClass(MyJoinCleanAndRank.class);
        Scan scan1 = new Scan();
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, intable1.getName().toBytes());
        Scan scan2 = new Scan();
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, intable2.getName().toBytes());
        List<Scan> list = new ArrayList<>();
        list.add(scan1);
        list.add(scan2);
        job.setInputFormatClass(MultiTableInputFormat.class);
        TableMapReduceUtil.initTableMapperJob(list, JCARMapper.class, ImmutableBytesWritable.class, MapWritable.class, job);
        TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(),JCARReducer.class,job);
        job.waitForCompletion(true);
        return 0;
    }
    public static class JCARMapper extends TableMapper<ImmutableBytesWritable,MapWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
                String k = new String(key.get());
                //得到常规uri格式
                String uri = showURI(k).trim();

                //根据 page:i 这个标志位 判断数据从哪张表中来 a 来自clean_webpage b 来自rank_result
                byte[] t =
                        getValueByFamilyAndQualifier(value, Bytes.toBytes("page"), Bytes.toBytes("i"));
                String tag = Bytes.toString(t).trim();
                MapWritable res_map = new MapWritable();
                if (tag.equals("a")) {
                    byte[] page_key_word =
                            getValueByFamilyAndQualifier(value, Bytes.toBytes("page"), Bytes.toBytes("key"));
                    res_map.put(
                            new BytesWritable(Bytes.toBytes("key")),
                            new BytesWritable(page_key_word));
                    context.write(new ImmutableBytesWritable(uri.getBytes()), res_map);
                } else if (tag.equals("b")) {
                    byte[] rank_value =
                            getValueByFamilyAndQualifier(value, Bytes.toBytes("page"), Bytes.toBytes("rank"));
                    res_map.put(
                            new BytesWritable(Bytes.toBytes("rank")),
                            new BytesWritable(rank_value));
                    context.write(new ImmutableBytesWritable(uri.getBytes()), res_map);
                }
        }
        public byte[] getValueByFamilyAndQualifier(Result value, byte[] family, byte[] qualifier) {
            Cell cell = value.getColumnLatestCell
                    (family, qualifier);
            if (cell == null) {
                return Bytes.toBytes(0);
            }
            return Arrays.copyOfRange
                    (cell.getValueArray(),
                            cell.getValueOffset(),
                            cell.getValueArray().length);
        }

        // 把nutch所扒取的网址转化为常见格式
        public static String showURI(String s) {
            // com.redhat.access:https/security/vulnerabilities/3442151
            String[] ss = s.split(":");
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
        }

    }


    public static class JCARReducer extends TableReducer<ImmutableBytesWritable, MapWritable, NullWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(key.get());
            // 凑语法
            // 坑！ 大坑！ values不能迭代两次，不论用迭代器，还是增强for，还是forEach
            // 所以此处连接查询 进行了变化，先将连接结果 直接平铺 输出
            // 再用下一个mapreduce 将符合条件的 行留下
            for (MapWritable value : values) {
                Set<Map.Entry<Writable, Writable>> entries = value.entrySet();
                for (Map.Entry<Writable, Writable> entry : entries) {
                    BytesWritable k = (BytesWritable) entry.getKey();
                    BytesWritable v = (BytesWritable) entry.getValue();
                    String tk = new String(k.getBytes()).trim();
                    if (tk.equals("rank")) {
                        put.addColumn(Bytes.toBytes("page"), getTirmBytes(k), v.getBytes());
                    } else {
                        put.addColumn(Bytes.toBytes("page"), getTirmBytes(k), getTirmBytes(v));
                    }
                }
            }
            context.write(NullWritable.get(), put);
        }
        public static byte[] getTirmBytes(BytesWritable b) {
            String str = Bytes.toString(b.getBytes()).trim();
            return str.getBytes();
        }
    }

}
