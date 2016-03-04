package com.zx.bigdata.mapreduce.test.tax.bean;

import java.util.HashMap;
import java.util.Map;

import com.zx.bigdata.bean.datadef.DataItem;
import com.zx.bigdata.bean.datadef.DataItemTypeEnum;
import com.zx.bigdata.bean.datadef.OccurrencyRateEnum;
import com.zx.bigdata.bean.datadef.Segment;
import com.zx.bigdata.bean.datadef.rules.Rule;
import com.zx.bigdata.bean.datadef.rules.RuleMethodEnum;

public class TaxReportDeleteSegment {

	private static Map<String, Segment> segments;

	static {
		init();
	}

	private static void init() {
		segments = new HashMap<String, Segment>();
		initHeaderSegment();
		initDeleteSegment();

	}

	public static Segment getSegment(String segName) {

		return segments.get(segName);
	}

	public static Map<String, Segment> getSegments() {
		return segments;
	}

	private static void initHeaderSegment() {
		Segment segment = new Segment();
		segment.setName(Segment.CONST_HEADER_KEY);
		segment.setOccurrencyRate(OccurrencyRateEnum.M_ONCE);
		segments.put(Segment.CONST_HEADER_KEY, segment);

		// *** head items** start **
		// 数据格式版本
		DataItem item = new DataItem();
		item.setName("version");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(1);
		item.setEndPose(3);
		item.getRules().add(new Rule(RuleMethodEnum.IS_VERSION_FORMAT.getMethodName()));
		segment.getDataItems().add(item);
		// 报送机构代码
		item = new DataItem();
		item.setName("COURTCODE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(4);
		item.setEndPose(17);
		segment.getDataItems().add(item);
		// 报文生成时间YYYYMMDDHHMMSSS
		item = new DataItem();
		item.setName("REGISTERDATE");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(18);
		item.setEndPose(31);
		Rule rul = new Rule();
		rul.setRuleName(RuleMethodEnum.CHECK_DATE.getMethodName());
		rul.getParams().add("YYYYMMDDHHMMSS");
		item.getRules().add(rul);
		segment.getDataItems().add(item);
		// 报文类别
		item = new DataItem();
		item.setName("LITIGATIONTYPE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(32);
		item.setEndPose(32);
		segment.getDataItems().add(item);
		// 应用系统代码
		item = new DataItem();
		item.setName("ORGINALCASENO");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(33);
		item.setEndPose(33);
		segment.getDataItems().add(item);
		// 报文类型
		item = new DataItem();
		item.setName("ORGINALCASETYPE");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(34);
		item.setEndPose(34);
		segment.getDataItems().add(item);
		// 反馈标志
		item = new DataItem();
		item.setName("FEEDBACKSIGN");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(35);
		item.setEndPose(35);
		segment.getDataItems().add(item);
		// 国税／地税标识
		item = new DataItem();
		item.setName("TAXSYMBOL");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(36);
		item.setEndPose(37);
		segment.getDataItems().add(item);
		// 联系人
		item = new DataItem();
		item.setName("CONTACKOR");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(38);
		item.setEndPose(67);
		segment.getDataItems().add(item);
		// 联系电话
		item = new DataItem();
		item.setName("CONTACKNO");
		item.setDataItemType(DataItemTypeEnum.ANC);
		item.setStartPose(68);
		item.setEndPose(92);
		segment.getDataItems().add(item);
		// 预留字段
		item = new DataItem();
		item.setName("OBLIGATE");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(93);
		item.setEndPose(122);
		segment.getDataItems().add(item);
		// *** head items ** end ***

	}

	private static void initDeleteSegment() {
		Segment segment = new Segment();
		segment.setName(Segment.CONST_BASIC_KEY);
		segment.setOccurrencyRate(OccurrencyRateEnum.M_ONCE);
		segments.put(Segment.CONST_BASIC_KEY, segment);

		// 主管税务机关代码
		DataItem item = new DataItem();
		item.setName("taxOrgCode");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(1);
		item.setEndPose(14);
		segment.getDataItems().add(item);

		// 纳税人识别号
		item = new DataItem();
		item.setName("taxPayerId");
		item.setDataItemType(DataItemTypeEnum.AN);
		item.setStartPose(15);
		item.setEndPose(34);
		segment.getDataItems().add(item);

		// 起始删除税务报送统计时间
		item = new DataItem();
		item.setName("startDelTaxTime");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(35);
		item.setEndPose(42);
		segment.getDataItems().add(item);

		// 终止删除税务报送统计时间
		item = new DataItem();
		item.setName("startDelTaxTime");
		item.setDataItemType(DataItemTypeEnum.N);
		item.setStartPose(43);
		item.setEndPose(50);
		segment.getDataItems().add(item);
	}

}
