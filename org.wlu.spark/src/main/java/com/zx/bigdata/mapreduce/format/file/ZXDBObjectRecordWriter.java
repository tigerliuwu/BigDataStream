package com.zx.bigdata.mapreduce.format.file;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;

public class ZXDBObjectRecordWriter extends RecordWriter<ZXDBObjectKey, Writable> {

	private DataOutputStream out;
	private String fieldDelimiter;

	public ZXDBObjectRecordWriter(DataOutputStream out, String fieldSep) {
		this.out = out;
		this.fieldDelimiter = fieldSep;
	}

	@Override
	public void write(ZXDBObjectKey key, Writable value) throws IOException, InterruptedException {
		StringBuilder builder = new StringBuilder();
		if (key.secondaryKey) {
			builder.append("SK:");
		}
		builder.append(key.key);

		if (value instanceof NullWritable) {

		} else if (value instanceof MapWritable) {
			builder.append(this.fieldDelimiter);
			MapWritable map = (MapWritable) value;
			builder.append("{");
			for (Map.Entry<Writable, Writable> tmp : map.entrySet()) {
				builder.append(((Text) tmp.getKey()).toString()).append(":");
				builder.append(((Text) tmp.getValue()).toString());
			}
			builder.append("}");
		}
		out.write(builder.toString().getBytes());
		out.write("\n".getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (this.out != null) {
			this.out.close();
		}
	}

}
