nutch结合hbase提供搜索引擎基础数据

1 nutch 扒取数据到 hbase中
  此时数据为字节类型，读取不同字段时注意类型
  时间为 long，签名 为 16进制，
  parseStatus 是short或者byte

2 在表上做mr程序进行清洗 状态为 status_fetched  解析状态为parseStatus:	success/ok  为合法数据
 每个url相关数据间以空行分割    
   f列族   st列 的值 是2(status_fetched)

3 在清洗后数据上做mr程序读取
  url  外连接个数 外连接url *外连接内容  *入链接个数 *入链接url 入链接内容 *score title *content

4 使用pagerank方法对于连接进行计算权重值
    迭代次数20次

5 在hbase中针对以上数据做连接查询将计算过rank值的网页 整理出（keyworld） *title，*然后排序
  
  如何确定当前网页的主要内容或者关键字
  i，可以根据当前页面的title标签确定
  ii，分析页面内容确定
  *iii，根据当前网页入链 在超链接中写的字符串确定

6 构建二级索引或者倒置索引，即 将查询字段放入rowkey
  关键字   url:rank,url1:rank1.....


7 可视化搜索引擎页面，进行hbase数据测试

------------------------------------
nutch的安装
1 官网下载源码包apache-nutch-2.3-src.tar.gz  

2 解压源码包tar -zxvf apache-nutch-2.3-src.tar.gz  

3 进入源码包ivy ，修改ivy.xml
  <dependency org="org.apache.hadoop" name="hadoop-hdfs" rev="2.5.2" conf="*->default"/>
  <dependency org="org.apache.hadoop" name="hadoop-mapreduce-client-core" rev="2.5.2" conf="*->default"/>
  <dependency org="org.apache.hadoop" name="hadoop-mapreduce-client-jobclient" rev="2.5.2" conf="*->default"/>

 <dependency org="org.apache.gora" name="gora-hbase" rev="0.6.1" conf="*->default" />
 <dependency org="org.apache.gora" name="gora-compiler-cli" rev="0.6.1" conf="*->default"/>
 <dependency org="org.apache.gora" name="gora-compiler" rev="0.6.1" conf="*->default"/>
 <dependency org="org.apache.hbase" name="hbase-common" rev="0.98.8-hadoop2" conf="*->default" />

4 进入 conf/gora.properties增加如下一行
  <--将HbaseStore设为默认的存储 -->  
  gora.datastore.default=org.apache.gora.hbase.store.HBaseStore  

5 安装ant 官网下载压缩包，解压后配置环境变量即可
   官方主页http://ant.apache.org下载新版的ant
   export ANT_HOME=xxxxx/ant/apache-ant-1.9.8
   export Path=$PATH:$ANT_HOME/bin

6 在nutch的安装目录下，使用 ant runtime 进行编译

7 使nutch采集的数据放到hbase中
  编辑 conf/nutch-site.xml
  <property>
      <name>http.agent.name</name>
      <value>Your Nutch Spider</value>
  </property
  <property>
      <name>parser.character.encoding.default</name>
      <value>utf-8</value>
      <description>The character encoding to fall back towhen no oth    er information
      is available</description>
  </property>
  <property>
      <name>storage.data.store.class</name>
      <value>org.apache.gora.hbase.store.HBaseStore</value>
      <description>Default class for storingdata</description>
 </property>

8 在 安装 目录下，创建一个urls目录。
mkdir urls
进入urls目录
vi seed.txt
在seed.txt中加入需要抓取的网址。

9 测试扒取
bin/crawl <seedDir> <crawlID> [<solrURL>] <numberOfRounds> 

-----------------------------------------------------------------------
中间表结构

ali_webpage    huawei_webpage

clean_webpage 存放第二步结果  以及  第五步的结果
page:iln入链个数 oln出链个数 t标题  s评分 cnt内容 key关键字
il:入链uri  cnt
ol:出链uri  cnt

rank_result 第四步结果
page:oln出链个数   rank权重值
ol:出链uri  1

join_result 第四步和第五步结果进行连接

page:key
page:rank
page:cnt

最终结果   连接结果整理成需要的格式
invertindex_result
  rowkey                     page
keyword                   uri : rank                content
-----------------------------------------
Mapreduce 作用以及顺序
CleanDataMapRed   整理原始数据得到有效数据
url  外连接个数 外连接url 入链接个数 入链接url score title 内容关键字

FindAllPageKeyWord  提取每个页面的关键字

PageRank 计算每个页面的权重值

AddIdentityBeforeJoin 用来给两张表添加标识，在连接操作的时候方便区分数据的来源


JoinCleanAndRank 把有效页面 和 页面的权重值做链接查询

HoldJoinResult 承接JoinCleanAndRank 一个mapred做不完链接查询，
此处用两个mapred做完的

BuildSecondaryIndex 构建二级索引，把keyword rank值拼到rowkey列去


CountWebpageNum 辅助mapred程序，计算网页个数




运行顺序
              Nutch采集完后
                    |
             CleanDataMapRed
              |            |
FindAllPageKeyWord        PageRank
              |            |
           AddIdentityBeforeJoin    (表名在代码中修改)
                    |
              JoinCleanAndRank 
                    |
              HoldJoinResult
                    |
              BuildInvertIndex      
                    |
         搜索引擎webapp运行，进行关键字查找
     

1 zookeeper 172.16.0.4   172.16.0.6    172.16.0.7

2 用自己的名字做命名空间 设计中间表  java API

3 最终结果的表名  放入  table_info
  rowkey            info:owner
  最终结果的表名         名字


yarn jar pr.jar com.briup.clean.CleanDataMapRed -Dbefore_table=aliyun_webpage -Dclean_table=clean_webpage

yarn jar pr.jar com.briup.clean.FindAllPageKeyWord

yarn jar pr.jar com.briup.clean.PageRank

添加标志位

yarn jar pr.jar com.briup.clean.JoinCleanAndRank

yarn jar pr.jar com.briup.clean.HoldJoinResult

yarn jar pr.jar com.briup.clean.BuildInvertIndex



get 'rank_result','com.aliyun.bbs:https/'

get 'clean_webpage','com.aliyun.bbs:https/'

get 'last_result','https://bbs.aliyun.com/'

get 'join_result','https://bbs.aliyun.com/'




 https://yq.aliyun.com/publication/117nicatione









