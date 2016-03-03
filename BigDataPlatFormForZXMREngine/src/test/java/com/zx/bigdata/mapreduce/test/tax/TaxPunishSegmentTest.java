package com.zx.bigdata.mapreduce.test.tax;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;
import com.zx.bigdata.mapreduce.format.file.ZXDBObjectRecordOutputFormat;
import com.zx.bigdata.mapreduce.mapper.MRMapper;
import com.zx.bigdata.mapreduce.test.tax.bean.TaxDataProcess;
import com.zx.bigdata.mapreduce.test.tax.bean.TaxDataSchema;
import com.zx.bigdata.mapreduce.test.tax.bean.TaxReportSegments;
import com.zx.bigdata.utils.MRCounterUtil;

public class TaxPunishSegmentTest {
	ObjectMapper mapper;
	Configuration conf;
	TaxDataSchema dataSchema;
	TaxDataProcess dataProcess;
	final String input = "testData/tax/taxpunish";

	@Before
	public void setup() {
		mapper = new ObjectMapper();
		conf = new Configuration();
		dataSchema = new TaxDataSchema(ReportTypeEnum.NORMAL);
		dataSchema.setSegments(TaxReportSegments.getSegments());
		dataProcess = new TaxDataProcess(dataSchema.getDataSchema());
		dataProcess.initDetailedTaxDBSchema();// 初始化分税种明细信息段
		// init the path
		dataProcess.addHDFSPath(input);

	}

	/**
	 * 源文件中只有基本信息段 + 分税种明细信息段
	 * <p>
	 * 分税种明细信息段没有对应的ColumnObject
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPunishSegments1() throws Exception {

		final String output = "tmp/taxpunish/tax1";
		FileUtil.fullyDelete(new File(output));

		conf.clear();
		String json = mapper.writeValueAsString(dataSchema.getDataSchema());
		conf.set("org.zx.bigdata.dataschema", json);
		json = mapper.writeValueAsString(dataProcess.getDataProcess());
		conf.set("org.zx.bigdata.dataprocess", json);

		Job job = Job.getInstance(conf, "taxPunish1"); // new Job(conf,
														// "wordcount");
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(ZXDBObjectKey.class);
		job.setOutputValueClass(Writable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ZXDBObjectRecordOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		MultipleOutputs.addNamedOutput(job, "feedback", TextOutputFormat.class, NullWritable.class, Text.class);

		job.setMapperClass(MRMapper.class);
		job.waitForCompletion(true);
		assertTrue(MRCounterUtil.validCounterNum(job.getCounters(), dataProcess.getDataProcess()));
		System.out.println("there you see");
		assertTrue(job.isSuccessful());

	}

	/**
	 * 源文件中只有基本信息段 + 分税种明细信息段
	 * <p>
	 * 分税种明细信息段有对应的ColumnObject
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPunishSegments2() throws Exception {

		// init the path
		// final String input = "testData/tax/onlytaxtype";
		final String output = "tmp/taxpunish/tax2";
		FileUtil.fullyDelete(new File(output));

		conf.clear();
		String json = mapper.writeValueAsString(dataSchema.getDataSchema());
		conf.set("org.zx.bigdata.dataschema", json);
		// dataProcess.addHDFSPath(input);
		dataProcess.initPunishDBSchema();
		json = mapper.writeValueAsString(dataProcess.getDataProcess());
		conf.set("org.zx.bigdata.dataprocess", json);

		Job job = Job.getInstance(conf, "taxPunish2"); // new Job(conf,
														// "wordcount");
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(ZXDBObjectKey.class);
		job.setOutputValueClass(Writable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ZXDBObjectRecordOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		MultipleOutputs.addNamedOutput(job, "feedback", TextOutputFormat.class, NullWritable.class, Text.class);

		job.setMapperClass(MRMapper.class);
		job.waitForCompletion(true);
		assertTrue(MRCounterUtil.validCounterNum(job.getCounters(), dataProcess.getDataProcess()));
		System.out.println("there you see");
		assertTrue(job.isSuccessful());

	}

}
