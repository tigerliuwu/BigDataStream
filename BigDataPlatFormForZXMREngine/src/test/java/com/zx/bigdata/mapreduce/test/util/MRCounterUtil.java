package com.zx.bigdata.mapreduce.test.util;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import com.zx.bigdata.bean.datadef.ReportTypeEnum;
import com.zx.bigdata.bean.processdef.DataProcess;
import com.zx.bigdata.bean.processdef.SecondaryIndex;
import com.zx.bigdata.mapreduce.bean.StatusCounterEnum;

public class MRCounterUtil {

	public static boolean validCounterNum(Counters counters, DataProcess dataProcess) {
		boolean valid = true;

		long num_input_record = 0; // 输入数据条数
		Counter counter = counters.findCounter(StatusCounterEnum.NUM_INPUT_RECORD);
		if (counter != null) {
			num_input_record = counter.getValue();
		}

		long num_valid_record = 0; // 有效数据条数
		counter = counters.findCounter(StatusCounterEnum.NUM_VALID_RECORD);
		if (counter != null) {
			num_valid_record = counter.getValue();
		}

		long num_invalid_record = 0; // 无效数据条数
		counter = counters.findCounter(StatusCounterEnum.NUM_INVALID_RECORD);
		if (counter != null) {
			num_invalid_record = counter.getValue();
		}

		long num_delete_rowkey = 0; // 删除数据条数
		counter = counters.findCounter(StatusCounterEnum.NUM_DELETE_ROWKEY);
		if (counter != null) {
			num_delete_rowkey = counter.getValue();
		}

		long num_normal_rowkey = 0; // rowkey条数
		counter = counters.findCounter(StatusCounterEnum.NUM_NORMAL_ROWKEY);
		if (counter != null) {
			num_normal_rowkey = counter.getValue();
		}

		long num_normal_skey = 0; // 正常二级索引条数
		counter = counters.findCounter(StatusCounterEnum.NUM_NORMAL_SECONDARY_KEY);
		if (counter != null) {
			num_normal_skey = counter.getValue();
		}

		// 输入数据条数 = 有效数据条数 + 无效数据条数
		if (num_input_record != num_valid_record + num_invalid_record) {
			valid = false;
			return valid;
		}

		int num_def_rowkey = dataProcess.getDbSchemas().size();// 定义的rowkey数量
		int num_skey_other = 0; // 定义的二级索引数量
		int num_skey_normal = 0; // 定义的正常的二级索引数量
		for (SecondaryIndex sk : dataProcess.getsKeys()) {
			if (!sk.isDeleteSKey() && sk.getRowKeyObject() != null) {
				num_skey_normal += 1;
			}
		}
		if (dataProcess.getsKeys().size() > 0) {
			num_skey_other = dataProcess.getsKeys().size() - num_skey_normal;
		}

		// rowkey条数 = 有效数据条数 ＊ 定义的rowkey数量
		if (num_normal_rowkey != num_valid_record * num_def_rowkey) {
			valid = false;
			return valid;
		}

		// 删除条数 = 有效数据条数 ＊ 定义的rowkey数量
		if (dataProcess.getReportType() == ReportTypeEnum.DELETE
				&& num_delete_rowkey != num_valid_record * num_def_rowkey) {
			valid = false;
			return valid;
		}

		// 正常二级索引条数 = rowkey条数 ＊ (定义的正常的二级索引数量 ＊ 2 + 定义的二级索引数量)
		if (num_normal_skey != num_normal_rowkey * (num_skey_normal * 2 + num_skey_other)) {
			valid = false;
			return valid;
		}

		return valid;
	}

	public static boolean validCounterNum(Counters counters, DataProcess dataProcess, HTable reportTable,
			HTable sndKeyTable) {
		boolean valid = true;

		long num_input_record = 0; // 输入数据条数
		Counter counter = counters.findCounter(StatusCounterEnum.NUM_INPUT_RECORD);
		if (counter != null) {
			num_input_record = counter.getValue();
		}

		long num_valid_record = 0; // 有效数据条数
		counter = counters.findCounter(StatusCounterEnum.NUM_VALID_RECORD);
		if (counter != null) {
			num_valid_record = counter.getValue();
		}

		long num_invalid_record = 0; // 无效数据条数
		counter = counters.findCounter(StatusCounterEnum.NUM_INVALID_RECORD);
		if (counter != null) {
			num_invalid_record = counter.getValue();
		}

		long num_delete_rowkey = 0; // 删除数据条数
		counter = counters.findCounter(StatusCounterEnum.NUM_DELETE_ROWKEY);
		if (counter != null) {
			num_delete_rowkey = counter.getValue();
		}

		long num_normal_rowkey = 0; // rowkey条数
		counter = counters.findCounter(StatusCounterEnum.NUM_NORMAL_ROWKEY);
		if (counter != null) {
			num_normal_rowkey = counter.getValue();
		}

		long num_normal_skey = 0; // 正常二级索引条数
		counter = counters.findCounter(StatusCounterEnum.NUM_NORMAL_SECONDARY_KEY);
		if (counter != null) {
			num_normal_skey = counter.getValue();
		}

		// 输入数据条数 = 有效数据条数 + 无效数据条数
		if (num_input_record != num_valid_record + num_invalid_record) {
			valid = false;
			return valid;
		}

		int num_def_rowkey = dataProcess.getDbSchemas().size();// 定义的rowkey数量
		int num_skey_other = 0; // 定义的二级索引数量
		int num_skey_normal = 0; // 定义的正常的二级索引数量
		for (SecondaryIndex sk : dataProcess.getsKeys()) {
			if (!sk.isDeleteSKey() && sk.getRowKeyObject() != null) {
				num_skey_normal += 1;
			}
		}
		if (dataProcess.getsKeys().size() > 0) {
			num_skey_other = dataProcess.getsKeys().size() - num_skey_normal;
		}

		// rowkey条数 = 有效数据条数 ＊ 定义的rowkey数量
		if (dataProcess.getReportType() == ReportTypeEnum.NORMAL
				&& num_normal_rowkey != num_valid_record * num_def_rowkey) {
			valid = false;
			return valid;
		}

		// 删除条数 = 有效数据条数 ＊ 定义的rowkey数量
		if (dataProcess.getReportType() == ReportTypeEnum.DELETE
				&& num_delete_rowkey != num_valid_record * num_def_rowkey) {
			valid = false;
			return valid;
		}

		// 正常二级索引条数 = rowkey条数 ＊ (定义的正常的二级索引数量 ＊ 2 + 定义的二级索引数量)
		if (dataProcess.getReportType() == ReportTypeEnum.NORMAL
				&& num_normal_skey != num_normal_rowkey * (num_skey_normal * 2 + num_skey_other)) {
			valid = false;
			return valid;
		}

		long preTotal = 0;
		long curTotal = 0;
		// 正常数据
		if (dataProcess.getReportType() == ReportTypeEnum.NORMAL) {
			// 报文表
			preTotal = HBaseTableUtil.get(reportTable);
			curTotal = HBaseTableUtil.calculateTotalRecord(reportTable);
			if (curTotal - preTotal != num_normal_rowkey) {
				valid = false;
				return valid;
			}
			// 二级索引表
			preTotal = HBaseTableUtil.get(sndKeyTable);
			curTotal = HBaseTableUtil.calculateTotalRecord(sndKeyTable);
			if (curTotal - preTotal != num_normal_skey) {
				valid = false;
				return valid;
			}
		} else if (dataProcess.getReportType() == ReportTypeEnum.DELETE) {
			// 报文表
			preTotal = HBaseTableUtil.get(reportTable);
			curTotal = HBaseTableUtil.calculateTotalRecord(reportTable);
			long val = preTotal - curTotal;
			if (val < 0 || val != num_delete_rowkey) {
				valid = false;
				return valid;
			}
			// 二级索引表
			preTotal = HBaseTableUtil.get(sndKeyTable);
			curTotal = HBaseTableUtil.calculateTotalRecord(sndKeyTable);
			val = preTotal - curTotal;
			if (val < 0 || val != num_def_rowkey * num_delete_rowkey * (num_skey_normal * 2 + 1)) {
				valid = false;
				return valid;
			}
		}

		return valid;
	}

}
