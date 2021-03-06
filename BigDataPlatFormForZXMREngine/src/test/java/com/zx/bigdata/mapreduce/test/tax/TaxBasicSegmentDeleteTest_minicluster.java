package com.zx.bigdata.mapreduce.test.tax;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;
import com.zx.bigdata.mapreduce.format.hbase.ZXMultiHBaseTblOutputFormat;
import com.zx.bigdata.mapreduce.mapper.MRMapper;
import com.zx.bigdata.mapreduce.test.model.HBaseClusterCache;
import com.zx.bigdata.mapreduce.test.tax.bean.TaxDataProcess;
import com.zx.bigdata.mapreduce.test.tax.bean.TaxDataSchema;
import com.zx.bigdata.mapreduce.test.tax.bean.TaxReportDeleteSegment;
import com.zx.bigdata.mapreduce.test.util.HBaseTableUtil;
import com.zx.bigdata.mapreduce.test.util.MRCounterUtil;

/**
 * 在本地启动了DFS，zookeeper以及mapreduce clusters
 * 
 * @author liuwu
 *
 */
public class TaxBasicSegmentDeleteTest_minicluster {

	ObjectMapper mapper;
	Configuration conf;
	TaxDataSchema dataSchema;
	TaxDataProcess dataProcess;

	@Before
	public void setup() throws Exception {
		mapper = new ObjectMapper();
		// HBaseClusterCache.startupMinicluster();
		conf = HBaseClusterCache.getHBaseUtility().getConfiguration();
	}

	/**
	 * 源文件中只有基本信息段
	 * 
	 * @throws Exception
	 */
	@Test
	public void testTaxDeleteReport() throws Exception {

		// testTaxBasicReport();

		HBaseTableUtil.storeTotal4Table(HBaseClusterCache.getReportTable());
		HBaseTableUtil.storeTotal4Table(HBaseClusterCache.getSndKeyTable());

		// init the path
		final String input = "testData/tax/onlybasicdelete/tax_with_only_basic";
		final String output = "/tmp/tax/basicSeg";
		FileUtil.fullyDelete(new File(output));

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path("/user/tax/basicDel/");
		fs.mkdirs(p);
		fs.copyFromLocalFile(new Path(input), p);

		if (!fs.exists(new Path("/user/tax/basic/tax_with_only_basic"))) {
			System.err.println("=========failed to upload the local file===========");
			return;
		}

		// conf.clear();
		dataSchema = new TaxDataSchema(ReportTypeEnum.DELETE);
		dataSchema.setSegments(TaxReportDeleteSegment.getSegments());
		dataProcess = new TaxDataProcess(dataSchema.getDataSchema());
		String json = mapper.writeValueAsString(dataSchema.getDataSchema());
		conf.set("org.zx.bigdata.dataschema", json);
		json = mapper.writeValueAsString(dataProcess.getDataProcess());
		conf.set("org.zx.bigdata.dataprocess", json);

		conf.set("org.zx.bigdata.msgtype.delete", "true");

		Job job = Job.getInstance(conf, "WriteToHBase"); // new Job(conf,
															// "wordcount");
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(ZXDBObjectKey.class);
		job.setOutputValueClass(Writable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(ZXMultiHBaseTblOutputFormat.class);
		// job.setOutputFormatClass(ZXDBObjectRecordOutputFormat.class);

		FileInputFormat.addInputPath(job, p);
		FileOutputFormat.setOutputPath(job, new Path(output));

		MultipleOutputs.addNamedOutput(job, "feedback", TextOutputFormat.class, NullWritable.class, Text.class);

		job.setMapperClass(MRMapper.class);
		job.waitForCompletion(true);

		assertTrue(MRCounterUtil.validCounterNum(job.getCounters(), dataProcess.getDataProcess(),
				HBaseClusterCache.getReportTable(), HBaseClusterCache.getSndKeyTable()));
		assertTrue(job.isSuccessful());

		System.out.println("there you see");

		// fs.copyToLocalFile(new Path(output + "/part-m-00000"), new
		// Path("/home/liuwu/tmp/out1"));

	}

	@After
	public void shutdown() {
		// HBaseClusterCache.shutdownMinicluster();
	}

}
