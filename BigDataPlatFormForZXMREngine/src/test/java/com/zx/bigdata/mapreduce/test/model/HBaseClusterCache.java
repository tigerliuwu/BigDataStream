package com.zx.bigdata.mapreduce.test.model;

import org.apache.hadoop.hbase.HBaseTestingUtility;

public class HBaseClusterCache {
	private static HBaseTestingUtility utility;

	public static HBaseTestingUtility getMiniCluster() throws Exception {
		if (utility == null) {
			utility = new HBaseTestingUtility();
			utility.startMiniCluster();
		}
		return utility;

	}

}
