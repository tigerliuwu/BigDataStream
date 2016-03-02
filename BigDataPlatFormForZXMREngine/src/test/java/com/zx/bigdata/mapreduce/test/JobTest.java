package com.zx.bigdata.mapreduce.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Before;
import org.junit.Test;

import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;
import com.zx.bigdata.mapreduce.mapper.MRMapper;
import com.zx.bigdata.mapreduce.test.model.HBaseClusterCache;

public class JobTest {
	private static HBaseTestingUtility utility;

	@Before
	public void setup() throws Exception {
		utility = HBaseClusterCache.getMiniCluster();
	}

	@Test
	public void test() throws IOException, ClassNotFoundException, InterruptedException {
		final String input = "/home/liuwu/work/gitRepos/org.wliu.mr.demo/src/test/resources/mrunit";
		final String output = "/home/liuwu/tmp/out3";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count"); // new Job(conf,
														// "wordcount");

		job.setOutputKeyClass(ZXDBObjectKey.class);
		job.setOutputValueClass(Writable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(MRMapper.class);
		job.waitForCompletion(true);
	}

}
