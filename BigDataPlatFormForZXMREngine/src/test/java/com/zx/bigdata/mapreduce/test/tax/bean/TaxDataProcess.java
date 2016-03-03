package com.zx.bigdata.mapreduce.test.tax.bean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zx.bigdata.bean.datadef.DataItem;
import com.zx.bigdata.bean.datadef.DataSchema;
import com.zx.bigdata.bean.datadef.Segment;
import com.zx.bigdata.bean.processdef.ColumnObject;
import com.zx.bigdata.bean.processdef.DBSchema;
import com.zx.bigdata.bean.processdef.DataProcess;
import com.zx.bigdata.bean.processdef.RowKeyObject;
import com.zx.bigdata.bean.processdef.SecondaryIndex;
import com.zx.bigdata.bean.processdef.SegmentDataItemPair;

public class TaxDataProcess {

	private DataProcess dataProcess;
	private Map<String, Boolean> segMap = new HashMap<String, Boolean>();

	private void init() {
		this.dataProcess = new DataProcess();
		this.initDBSchemas();
		this.initSecondaryIndexes();
	}

	public TaxDataProcess(DataSchema dataSchema) {
		init();
		this.dataProcess.setReportInformationType(dataSchema.getReportInformationType());
		this.dataProcess.setReportType(dataSchema.getReportType());
		this.dataProcess.setName("个人税务数据流程-" + this.dataProcess.getReportType().name());
	}

	public DataProcess getDataProcess() {
		return this.dataProcess;
	}

	public void addHDFSPath(String path) {
		if (this.dataProcess.getHdfsPaths().contains(path)) {
			return;
		}
		this.dataProcess.getHdfsPaths().add(path);
	}

	/**
	 * 分税种明细信息段
	 */
	public void initDetailedTaxDBSchema() {
		String segName = "B";
		if (this.segMap.get(segName) != null) {
			return;
		}
		this.segMap.put(segName, true);

		ColumnObject colObj = new ColumnObject();
		List<ColumnObject> colObjs = this.dataProcess.getDbSchemas().get(0).getColumnObjects();

		colObjs.add(colObj);
		colObj.setName("taxDetailedType");
		colObj.setSegName(segName);

		// 来自于报文头的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "LITIGATIONTYPE"));

		// 来自于基本信息段的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxOrgCode"));

		// 来自于当前信息段的数据段
		for (DataItem item : TaxReportSegments.getSegment(segName).getDataItems()) {
			colObj.getDataItems().add(new SegmentDataItemPair(segName, item.getName()));
		}

	}

	/**
	 * 惩罚信息段
	 */
	public void initPunishDBSchema() {
		String segName = "C";
		if (this.segMap.get(segName) != null) {
			return;
		}
		this.segMap.put(segName, true);

		ColumnObject colObj = new ColumnObject();
		List<ColumnObject> colObjs = this.dataProcess.getDbSchemas().get(0).getColumnObjects();

		colObjs.add(colObj);
		colObj.setName("taxPunish");
		colObj.setSegName(segName);

		// 来自于报文头的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "LITIGATIONTYPE"));

		// 来自于基本信息段的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxOrgCode"));

		// 来自于当前信息段的数据段
		for (DataItem item : TaxReportSegments.getSegment(segName).getDataItems()) {
			colObj.getDataItems().add(new SegmentDataItemPair(segName, item.getName()));
		}
	}

	/**
	 * 奖励信息段
	 */
	public void initCommendDBSchema() {
		String segName = "D";
		if (this.segMap.get(segName) != null) {
			return;
		}
		this.segMap.put(segName, true);

		ColumnObject colObj = new ColumnObject();
		List<ColumnObject> colObjs = this.dataProcess.getDbSchemas().get(0).getColumnObjects();

		colObjs.add(colObj);
		colObj.setName("taxCommend");
		colObj.setSegName(segName);

		// 来自于报文头的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "LITIGATIONTYPE"));

		// 来自于基本信息段的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxOrgCode"));

		// 来自于当前信息段的数据段
		for (DataItem item : TaxReportSegments.getSegment(segName).getDataItems()) {
			colObj.getDataItems().add(new SegmentDataItemPair(segName, item.getName()));
		}
	}

	private void initDBSchemas() {

		DBSchema dbSchema = new DBSchema();
		this.dataProcess.getDbSchemas().add(dbSchema);

		// **** rowkey definition **start***********************
		RowKeyObject rowKey = new RowKeyObject();
		dbSchema.setRowKeyObject(rowKey);
		// 唯一标识
		List<SegmentDataItemPair> idCodes = rowKey.getIdCode();
		idCodes.add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "NAME"));
		idCodes.add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "CERTTYPE"));
		idCodes.add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "CERTNO"));

		// 数据源区分段
		rowKey.setReportInformationType(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "LITIGATIONTYPE"));

		// 业务类型区分段
		rowKey.getBusiKeys().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "COURTCODE"));// 报送机构代码
		rowKey.getBusiKeys().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxOrgCode"));// 主管税务机关代码
		rowKey.getBusiKeys().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "TAXSYMBOL"));// 国税／地税标识
		rowKey.getBusiKeys().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxPayerId"));// 纳税人识别号

		// 税务报送统计时间
		rowKey.setTime(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxReportTime"));
		// *****rowkey definition **end******************************

		// ****rowkey columnObjects definition ** start************

		// 基本信息段作为一个ColumnObject
		ColumnObject colObj = new ColumnObject();
		dbSchema.getColumnObjects().add(colObj);
		colObj.setName("basicSeg");
		colObj.setSegName(Segment.CONST_BASIC_KEY);

		// 来自于报文头的数据段
		colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "LITIGATIONTYPE"));

		// 来自于基本信息段的数据段
		for (DataItem item : TaxReportSegments.getSegment(Segment.CONST_BASIC_KEY).getDataItems()) {
			colObj.getDataItems().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, item.getName()));
		}

		// ****rowkey columnObjects definition ** end************

	}

	private void initSecondaryIndexes() {
		// 登记状态 + 行业分类
		SecondaryIndex skey = new SecondaryIndex();
		this.dataProcess.getsKeys().add(skey);
		skey.getKey().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "regStatus"));// 登记状态
		skey.getKey().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "tradeClass"));// 行业分类
		skey.setRowKeyObject(this.dataProcess.getDbSchemas().get(0).getRowKeyObject());

		// 报送机构代码 + 主管税务机关代码 + 纳税人识别号 (删除索引）
		skey = new SecondaryIndex();
		this.dataProcess.getsKeys().add(skey);
		skey.setDeleteSKey(true);
		skey.getKey().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY, "COURTCODE"));// 报送机构代码
		// skey.getKey().add(new SegmentDataItemPair(Segment.CONST_HEADER_KEY,
		// "TAXSYMBOL"));// 国税／地税标识
		skey.getKey().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxOrgCode"));// 主管税务机关代码
		skey.getKey().add(new SegmentDataItemPair(Segment.CONST_BASIC_KEY, "taxPayerId"));// 纳税人识别号
		skey.setRowKeyObject(this.dataProcess.getDbSchemas().get(0).getRowKeyObject());
	}

}
