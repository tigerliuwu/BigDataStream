#大数据管理平台执行引擎
1)使用了maven进行项目的管理
2）HDFS作为数据源
3）mapreduce作为计算引擎
4）hbase作为数据写入目的地
5）使用mrunit和minicluster进行单元测试

##模型
模型位于package:com.zx.bigdata.bean，其中datadef表示数据模型定义部分；processdef表示流程定义部分。

##mr执行引擎
引擎代码位于package:com.zx.bigdata.mapreduce。<br>
目前只用了mapper，使用counter进行输入数据和输出数据的统计。<br>

##测试
1. hbase<br>
refer to: http://blog.csdn.net/ebay/article/details/43528941
