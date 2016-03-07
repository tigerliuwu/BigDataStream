package com.zx.bigdata.mapreduce.format.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.zx.bigdata.mapreduce.bean.ZXDBObjectKey;
import com.zx.bigdata.utils.MRUtils;

public class ZXHBaseTblRecordWriter extends RecordWriter<ZXDBObjectKey, Writable> {
	private static final Logger LOG = Logger.getLogger(ZXHBaseTblRecordWriter.class);

	public static byte[] TABLE_FAMILY = null;
	// private static byte[] TABLE_COLUMN = null;
	public static byte[] TABLE_STATUS = null;
	public static byte[] DELETE_STATUS = null;
	public static byte[] TABLE_REPORT = null;// 报文数据保存表
	public static byte[] TABLE_SND_KEY = null;// 二级索引数据保存表
	public static byte[] SND_VAL_COL = null; // 二级索引数据保存列
	public static final byte[] EMPTY_BYTE_ARR = new byte[0];

	static {
		TABLE_REPORT = Bytes.toBytes(MRUtils.HBASE_PROCESS_DATA);
		TABLE_SND_KEY = Bytes.toBytes(MRUtils.HBASE_PROCESS_SNDINDEX_DATA);
		TABLE_FAMILY = Bytes.toBytes(MRUtils.HBASE_TABLE_FAMILY);
		TABLE_STATUS = Bytes.toBytes(MRUtils.HBASE_RECORD_STATUS_COL);
		DELETE_STATUS = Bytes.toBytes(MRUtils.CONSTANT_DELETE_STATUS);
		SND_VAL_COL = Bytes.toBytes(MRUtils.HBASE_PROCESS_SNDINDEX_VAL_COL);

	}

	private Connection conn;
	private Configuration conf;
	private boolean useWriteAheadLog;
	private Map<byte[], BufferedMutator> mutatorMap = new HashMap<byte[], BufferedMutator>();
	private BufferedMutator reportMutator; // 报文
	private BufferedMutator sndKeyMutator; // 二级索引

	// private Table reportTable;
	private Table sndKeyTable;

	private boolean isDelStatus = false;

	public ZXHBaseTblRecordWriter(Configuration conf, boolean wal_on) throws IOException {
		LOG.info("created new ZXHBaseTblRecordWriter with WAL " + (this.useWriteAheadLog ? "on" : "off"));
		this.conf = conf;
		this.useWriteAheadLog = wal_on;

		String strDel = conf.get("org.zx.bigdata.msgtype.delete");
		if (strDel != null && !strDel.isEmpty()) {
			this.isDelStatus = Boolean.parseBoolean(strDel);
			LOG.info("this is a delete report.");
		}
	}

	BufferedMutator getBufferedMutator(byte[] tableName) throws Exception {
		BufferedMutator result = null;
		if (this.conn == null) {
			conn = ConnectionFactory.createConnection(conf);
			LOG.info("create a hbase connection.");
		}
		if (!this.mutatorMap.containsKey(tableName)) {
			mutatorMap.put(tableName, conn.getBufferedMutator(TableName.valueOf(tableName)));
			LOG.info("create a Buffered Mutator for table:" + new String(tableName) + ".");
		}
		result = this.mutatorMap.get(tableName);
		if (tableName == TABLE_REPORT) {
			this.reportMutator = result;
		} else if (tableName == TABLE_SND_KEY) {
			this.sndKeyMutator = result;
			this.sndKeyTable = conn.getTable(result.getName());
		}
		return result;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		for (BufferedMutator mutator : this.mutatorMap.values()) {
			mutator.flush();
		}
		LOG.info("flush all the data in the buffered Mutator into corresponding tables.");
		if (this.conn != null && !this.conn.isClosed()) {
			this.conn.close();
		}
		LOG.info("the hbase connection is closed.");
	}

	@Override
	public void write(ZXDBObjectKey key, Writable value) throws IOException, InterruptedException {
		Put put = null;

		// 二级索引
		if (key.secondaryKey) {
			if (this.sndKeyMutator == null) {
				try {
					this.sndKeyMutator = this.getBufferedMutator(TABLE_SND_KEY);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			put = new Put(Bytes.toBytes(key.key));
			put.addColumn(TABLE_FAMILY, SND_VAL_COL, EMPTY_BYTE_ARR);
			put.setDurability(this.useWriteAheadLog ? Durability.SYNC_WAL : Durability.SKIP_WAL);
			this.sndKeyMutator.mutate(put);
			LOG.debug("二级索引写入：(key:" + Bytes.toString(put.getRow()) + ")");

			return;
		}

		if (this.reportMutator == null) {
			try {
				this.reportMutator = this.getBufferedMutator(TABLE_REPORT);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (this.sndKeyMutator == null) {
			try {
				this.sndKeyMutator = this.getBufferedMutator(TABLE_SND_KEY);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// 删除报文
		if (this.isDelStatus) {
			// 查找相关的二级索引并进行物理删除
			byte[] row = null;
			Scan scan = new Scan();

			// 查找和删除正反二级索引以及二级删除索引

			// 查找二级删除索引
			StringBuilder sndKeyBuilder = new StringBuilder(key.key);
			sndKeyBuilder.append(MRUtils.KEY_SPLIT_DEL).append(MRUtils.CONSTANT_DELETE).append(MRUtils.KEY_SPLIT_DEL);
			row = Bytes.toBytes(sndKeyBuilder.toString());
			Filter filter1 = new PrefixFilter(row);
			scan.setFilter(filter1);
			ResultScanner rs = this.sndKeyTable.getScanner(scan);
			for (Result r : rs) {
				byte[] sndRow = r.getRow();
				Delete del = new Delete(sndRow);
				this.sndKeyMutator.mutate(del); // 删除二级删除索引
				LOG.debug("物理删除二级删除索引：(key:" + Bytes.toString(del.getRow()) + ")");

				// 获取报文数据表中对应的rowkey
				byte[] reportRow = Bytes.tail(sndRow, sndRow.length - row.length);
				// 逻辑删除报文表中的数据
				put = new Put(reportRow);
				put.setDurability(this.useWriteAheadLog ? Durability.SYNC_WAL : Durability.SKIP_WAL);
				put.addColumn(TABLE_FAMILY, TABLE_STATUS, DELETE_STATUS);
				this.reportMutator.mutate(put);
				LOG.debug("逻辑删除正常报文：(key:" + Bytes.toString(put.getRow()) + ")");

				// 删除正反向二级索引
				String strReportRow = Bytes.toString(reportRow);
				sndKeyBuilder = new StringBuilder(strReportRow);
				sndKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
				sndRow = Bytes.toBytes(sndKeyBuilder.toString());
				scan = new Scan();
				scan.setFilter(new PrefixFilter(sndRow));
				ResultScanner sndRs = this.sndKeyTable.getScanner(scan);
				for (Result tr : sndRs) {
					LOG.debug("开始删除正反二级索引.");
					byte[] reverseRow = tr.getRow();
					del = new Delete(reverseRow);
					this.sndKeyMutator.mutate(del);
					LOG.debug("物理删除二级索引：(key:" + Bytes.toString(del.getRow()) + ")");

					reverseRow = Bytes.tail(reverseRow, reverseRow.length - sndRow.length);
					sndKeyBuilder = new StringBuilder(Bytes.toString(reverseRow));
					sndKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
					sndKeyBuilder.append(strReportRow);
					reverseRow = Bytes.toBytes(sndKeyBuilder.toString());
					del = new Delete(reverseRow);
					this.sndKeyMutator.mutate(del);
					LOG.debug("物理删除二级删除索引：(key:" + Bytes.toString(del.getRow()) + ")");

				}
				sndRs.close();
			}
			rs.close();

			return;
		}

		// 正常报文入库
		byte[] row = Bytes.toBytes(key.key);
		put = new Put(row);
		put.setDurability(this.useWriteAheadLog ? Durability.SYNC_WAL : Durability.SKIP_WAL);
		if (value instanceof MapWritable) {
			MapWritable map = (MapWritable) value;
			for (Entry<Writable, Writable> entry : map.entrySet()) {
				Text tKey = (Text) entry.getKey();
				Text tVal = (Text) entry.getValue();
				put.addColumn(TABLE_FAMILY, tKey.getBytes(), tVal.getBytes());
			}
		}
		this.reportMutator.mutate(put);
		LOG.debug("正常报文入库：" + reportMutator.getName().getNameAsString() + "\tvalue=" + Bytes.toString(put.getRow())
				+ "\t" + put.toJSON());
	}

}
