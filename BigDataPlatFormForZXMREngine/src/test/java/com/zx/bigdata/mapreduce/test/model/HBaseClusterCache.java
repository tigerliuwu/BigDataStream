package com.zx.bigdata.mapreduce.test.model;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;

import com.zx.bigdata.mapreduce.format.hbase.ZXHBaseTblRecordWriter;

public class HBaseClusterCache {
	private static final Logger LOG = Logger.getLogger(HBaseClusterCache.class);
	private static HBaseTestingUtility utility;
	private static HTable reportTable;
	private static HTable sndKeyTable;

	static {
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e);
		}
	}

	private static void init() throws IOException {
		if (utility == null) {
			utility = new HBaseTestingUtility();
		}

	}

	private static void initCreateTable() throws IOException {
		LOG.info("good to come here:initCreateTable start");
		reportTable = utility.createTable(ZXHBaseTblRecordWriter.TABLE_REPORT, ZXHBaseTblRecordWriter.TABLE_FAMILY);// 报文表
		sndKeyTable = utility.createTable(ZXHBaseTblRecordWriter.TABLE_SND_KEY, ZXHBaseTblRecordWriter.TABLE_FAMILY); // 索引表
		LOG.info("good to come here:initCreateTable=============== end");
	}

	public static HTable getReportTable() {
		return reportTable;
	}

	public static HTable getSndKeyTable() {
		return sndKeyTable;
	}

	public static HBaseTestingUtility getHBaseUtility() throws Exception {
		return utility;
	}

	public static void startupMinicluster() throws Exception {
		utility.startMiniCluster();
		initCreateTable();
	}

	public static void shutdownMinicluster() {
		try {
			if (utility != null) {
				utility.shutdownMiniCluster();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOG.error(e);
		}
	}

}
