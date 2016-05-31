package org.wliu.spark.demo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.collection.JavaConversions;

public class SparkStreamDemo {
	public static void main(String[] args) {

		System.setProperty("HADOOP_USER_NAME", "hdfs");

		SparkConf sparkConf = getSparkConf("file://");

		JavaStreamingContext ctx = new JavaStreamingContext(sparkConf, new Duration(1000));

		run(ctx);
	}

	public static SparkConf getSparkConf(String libjars) {
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparkStreamDemo"); // spark app's name
		sparkConf.setMaster("yarn-client");

		List<String> libjar = new ArrayList<String>();
		if (libjars != null && !"".equals(libjars.trim())) {
			for (String jar : libjars.split(",")) {
				libjar.add(jar);
			}
		}
		// put all the mandatory external jars as the cached files.
		sparkConf.setJars(libjar.toArray(new String[libjar.size()]));

		// configurations
		sparkConf.set("spark.hadoop.yarn.application.classpath",
				"$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$YARN_HOME/*,$YARN_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*");
		sparkConf.set("spark.hadoop.yarn.resourcemanager.address", "SAS01:8032");
		sparkConf.set("spark.hadoop.yarn.resourcemanager.scheduler.address", "SAS01:8030");
		sparkConf.set("spark.hadoop.mapreduce.jobhistory.address", "SAS01:10020");

		Map<String, String> tuningConf = new HashMap<String, String>();
		tuningConf.put("spark.hadoop.fs.defaultFS", "hdfs://SAS01:8020");
		sparkConf.setAll(JavaConversions.asScalaMap(tuningConf));

		return sparkConf;

	}

	public static int run(JavaStreamingContext ctx) {

		ctx.start();
		ctx.awaitTermination();

		return 0;
	}
}
