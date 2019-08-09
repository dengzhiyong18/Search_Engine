package com.diyo.mytest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
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

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;


/**
 * 此mapreduce程序用来找到每个网页的关键字
 * 可以利用算法分析内容，但为了更快满足本次实验要求
 * 网页的关键字通过入链的超链接标签中的字符内容 以及当前网页中 title 来确定
 * */
public class MyFindAllPageKeyWord extends Configured implements Tool {
    @SuppressWarnings("unused")
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        conf.set("hbase.zookeeper.quorum", "172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
		conf.set("hbase.master.dns.interface", "computer1.cloud.briup.com");

		Connection conn = ConnectionFactory.createConnection(conf);
		System.out.println("连接成功!");
		Admin admin = conn.getAdmin();
		Table intable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_CleanDataMR:"));
		Table outtable = conn.getTable(TableName.valueOf("Diyo_searchEngine:mytest_CleanDataMR"));
        
        Job job = Job.getInstance(conf,"Diyo_findkeyword");
        job.setJarByClass(MyFindAllPageKeyWord.class);
        TableMapReduceUtil.initTableMapperJob(intable.getName(),new Scan(),
                KeyWordMapper.class,Text.class,Text.class,job);
        TableMapReduceUtil.initTableReducerJob(outtable.getName().toString(),KeyWordReducer.class,job);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MyFindAllPageKeyWord(),args);
    }

    public static class KeyWordMapper extends TableMapper<Text,Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context)
                throws IOException, InterruptedException {
            String k = Bytes.toString(key.get());
            //拿到 il列族下的所有 列 和 值组成的map
            NavigableMap<byte[], byte[]> qvs = value.getFamilyMap(Bytes.toBytes("il"));
            //拿到当前页面的title值
            byte[] title = value.getValue(Bytes.toBytes("page"), Bytes.toBytes("t"));
            //如果 map长度大于0代表该页面有入链
            if(qvs.size() > 0){
                Set<Map.Entry<byte[], byte[]>> qvset = qvs.entrySet();
                //拿到第一个入链的 内容 作为基础值
                byte[] v = qvs.firstEntry().getValue();
                //遍历所有的入链内容，把第一个非空值作为 keyword
                for (Map.Entry<byte[], byte[]> e : qvset) {
                    v = e.getValue();
                    if(v.length > 0){
                        break;
                    }
                }
                // 把入链内容和title共同作为关键字
                if(title != null){
                    context.write(new Text(k),new Text(Bytes.toString(v)
                            +"\t"+Bytes.toString(title)));
                }else {
                    context.write(new Text(k), new Text(Bytes.toString(v)));
                }
            }
        }
    }

    public static class KeyWordReducer extends TableReducer<Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));
            for (Text value : values) {
                put.addColumn(Bytes.toBytes("page"),Bytes.toBytes("key"),Bytes.toBytes(value.toString()));
            }
            context.write(NullWritable.get(),put);
        }
    }

}
