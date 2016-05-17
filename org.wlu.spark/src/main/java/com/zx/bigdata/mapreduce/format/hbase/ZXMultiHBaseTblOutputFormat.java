package com.zx.bigdata.mapreduce.format.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;

public class ZXMultiHBaseTblOutputFormat extends OutputFormat<ZXDBObjectKey, Writable> {

	public static final String WAL_PROPERTY = MultiTableOutputFormat.WAL_PROPERTY;
	public static final boolean WAL_ON = MultiTableOutputFormat.WAL_ON;
	public static final boolean WAL_OFF = MultiTableOutputFormat.WAL_OFF;

	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {

		return new TableOutputCommitter();
	}

	@Override
	public RecordWriter<ZXDBObjectKey, Writable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		return new ZXHBaseTblRecordWriter(conf, conf.getBoolean(WAL_PROPERTY, WAL_OFF));
	}

}
