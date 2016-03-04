package com.zx.bigdata.mapreduce.test.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.zx.bigdata.utils.MRUtils;

public class HBaseTableUtil {

	private static final Map<HTable, Long> preTblTotal = new HashMap<HTable, Long>();

	public static long calculateTotalRecord(HTable table) {
		long total = 0;
		Scan scan = new Scan();
		scan.setFilter(new SingleColumnValueFilter(Bytes.toBytes(MRUtils.HBASE_TABLE_FAMILY),
				Bytes.toBytes(MRUtils.HBASE_RECORD_STATUS_COL), CompareOp.NOT_EQUAL, Bytes.toBytes("1")));
		ResultScanner rs;
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				total += 1;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return total;
	}

	public static void storeTotal4Table(HTable table) {
		put(table, calculateTotalRecord(table));
	}

	public static void put(HTable table, long total) {
		preTblTotal.put(table, total);
	}

	public static long get(HTable table) {
		Long lval = preTblTotal.get(table);
		if (lval == null) {
			return 0;
		} else {
			return lval.longValue();
		}
	}

}
