#大数据管理平台执行引擎
1. 使用了maven进行项目的管理
2. HDFS作为数据源
3. mapreduce作为计算引擎
4. hbase作为数据写入目的地
5. 使用mrunit和minicluster进行单元测试

##模型
模型位于package:com.zx.bigdata.bean，其中datadef表示数据模型定义部分；processdef表示流程定义部分。

##mr执行引擎
引擎代码位于package:com.zx.bigdata.mapreduce。<br>
目前只用了mapper，使用counter进行输入数据和输出数据的统计。<br>

##测试
hbase<br>
refer to: <br>
* http://blog.csdn.net/ebay/article/details/43528941<br>
* http://www.tuicool.com/articles/FBjEzq

## 发布到计算节点的jar
-libjars需要的jar如下：<br>
* htrace-core-3.1.0-incubating.jar
* hbase-client-1.0.0-cdh5.4.0.jar
* zookeeper-3.4.5-cdh5.4.0.jar
* hbase-common-1.0.0-cdh5.4.0.jar
* hbase-server-1.0.0-cdh5.4.0.jar
* hbase-protocol-1.0.0-cdh5.4.0.jar
* javacsv.jar
* commons-lang-2.6.jar ???
* 当前项目package的jar