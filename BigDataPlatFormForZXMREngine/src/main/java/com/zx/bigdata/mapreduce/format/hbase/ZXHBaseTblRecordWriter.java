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
import com.zx.bigdata.mapreduce.mapper.MRMapper;
import com.zx.bigdata.utils.MRUtils;

public class ZXHBaseTblRecordWriter extends RecordWriter<ZXDBObjectKey, Writable> {
	private static final Logger LOG = Logger.getLogger(MRMapper.class);
	
	private static byte[] TABLE_FAMILY = null;
//	private static byte[] TABLE_COLUMN = null;
	private static byte[] TABLE_STATUS = null;
	private static byte[] DELETE_STATUS = null;
	private static byte[] TABLE_REPORT = null;//报文数据保存表
	private static byte[] TABLE_SND_KEY = null;//二级索引数据保存表
	
	static {
		TABLE_REPORT = Bytes.toBytes(MRUtils.HBASE_PROCESS_DATA);
		TABLE_SND_KEY = Bytes.toBytes(MRUtils.HBASE_PROCESS_SNDINDEX_DATA);
		TABLE_FAMILY = Bytes.toBytes(MRUtils.HBASE_TABLE_FAMILY);
		TABLE_STATUS = Bytes.toBytes(MRUtils.HBASE_RECORD_STATUS_COL);
		DELETE_STATUS = Bytes.toBytes(MRUtils.CONSTANT_DELETE_STATUS);
	}
	
	private Connection conn;
	private Configuration conf;
	private boolean useWriteAheadLog;
	private Map<byte[], BufferedMutator> mutatorMap = new HashMap<byte[], BufferedMutator>();
	private BufferedMutator reportMutator;	//报文
	private BufferedMutator sndKeyMutator;	//二级索引
	
//	private Table reportTable;
	private Table sndKeyTable;
	
	private boolean isDelStatus = false;
	
	public ZXHBaseTblRecordWriter(Configuration conf, boolean wal_on) throws IOException {
		LOG.debug("created new ZXHBaseTblRecordWriter with WAL " + (this.useWriteAheadLog?"on":"off"));
		this.conf = conf;
		this.useWriteAheadLog = wal_on;
		
		
		String strDel = conf.get("org.zx.bigdata.msgtype.delete");
		if (strDel != null && !strDel.isEmpty()) {
			this.isDelStatus = Boolean.parseBoolean(strDel);
		}
	}
	
	BufferedMutator getBufferedMutator(byte[] tableName) throws IOException {
		BufferedMutator result = null;
		if (this.conn == null) {
			conn = ConnectionFactory.createConnection(conf);
		}
		if (!this.mutatorMap.containsKey(tableName)) {
			mutatorMap.put(tableName,conn.getBufferedMutator(TableName.valueOf(tableName)));
		}
		result = this.mutatorMap.get(tableName);
		if (tableName==TABLE_REPORT) {
			this.reportMutator = result;
		} else if (tableName == TABLE_SND_KEY) {
			this.sndKeyMutator = result;
			this.sndKeyTable = conn.getTable(result.getName());
		}
		return result;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		for (BufferedMutator mutator : this.mutatorMap.values()) {
			mutator.flush();
		}
	}

	@Override
	public void write(ZXDBObjectKey key, Writable value) throws IOException, InterruptedException {
		Put put = null;
		
		// 二级索引
		if (key.secondaryKey) {
			if (this.sndKeyMutator == null) {
				this.sndKeyMutator = this.getBufferedMutator(TABLE_SND_KEY);
			}
			put = new Put(Bytes.toBytes(key.key));
			put.setDurability(this.useWriteAheadLog?Durability.SYNC_WAL:Durability.SKIP_WAL);
			this.sndKeyMutator.mutate(put);
			
			return;
		}
		
		
		if (this.reportMutator == null) {
			this.reportMutator = this.getBufferedMutator(TABLE_REPORT);
		}
		
		// 删除报文
		if (this.isDelStatus) {
			// 查找相关的二级索引并进行物理删除
			byte[] row = null;
			Scan scan = new Scan();
			
			//查找和删除正反二级索引以及二级删除索引
			
			//查找二级删除索引
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
				
				// 获取报文数据表中对应的rowkey
				byte[] reportRow = Bytes.tail(sndRow, sndRow.length - row.length);
				// 逻辑删除报文表中的数据
				put = new Put(reportRow);
				put.setDurability(this.useWriteAheadLog?Durability.SYNC_WAL:Durability.SKIP_WAL);
				put.addColumn(TABLE_FAMILY, TABLE_STATUS, DELETE_STATUS);
				this.reportMutator.mutate(put);

				// 删除正反向二级索引
				String strReportRow = Bytes.toString(reportRow);
				sndKeyBuilder = new StringBuilder(strReportRow);
				sndKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
				sndRow = Bytes.toBytes(sndKeyBuilder.toString());
				scan = new Scan();
				scan.setFilter(new PrefixFilter(sndRow));
				ResultScanner sndRs = this.sndKeyTable.getScanner(scan);
				for (Result tr : sndRs) {
					
					byte[] reverseRow = tr.getRow(); 
					del = new Delete(reverseRow);
					this.sndKeyMutator.mutate(del);
					
					reverseRow = Bytes.tail(reverseRow, reverseRow.length - reportRow.length);
					sndKeyBuilder = new StringBuilder(Bytes.toString(reverseRow));
					sndKeyBuilder.append(MRUtils.KEY_SPLIT_DEL);
					sndKeyBuilder.append(strReportRow);
					reverseRow = Bytes.toBytes(sndKeyBuilder.toString());
					del = new Delete(reverseRow);
					this.sndKeyMutator.mutate(del);
					
				}
				sndRs.close();
			}
			rs.close();
			
			return;
		}
		
		// 正常报文入库
		byte[] row = Bytes.toBytes(key.key);
		put = new Put(row);
		put.setDurability(this.useWriteAheadLog?Durability.SYNC_WAL:Durability.SKIP_WAL);
		if (value instanceof MapWritable) {
			MapWritable map = (MapWritable)value;
			for (Entry<Writable, Writable> entry :map.entrySet()) {
				Text tKey = (Text)entry.getKey();
				Text tVal = (Text)entry.getValue();
				put.addColumn(TABLE_FAMILY, tKey.getBytes(), tVal.getBytes());
			}
		}
		this.reportMutator.mutate(put);
		
	}

}
